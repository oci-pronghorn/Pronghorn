package com.ociweb.pronghorn.network.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.EmptyBlockHolder;
import com.ociweb.pronghorn.network.OrderSupervisorStage;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.AbstractRestStage;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableByteWriter;

public abstract class AbstractAppendablePayloadResponseStage <   
                                T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractRestStage<T,R,V,H> {

	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final GraphManager graphManager;
		
	private static final Logger logger = LoggerFactory.getLogger(AbstractAppendablePayloadResponseStage.class);
	
	private long activeChannelId = -1;
	private int activeSequenceNo = -1;
	private int activeFieldRequestContext = -1;	

	private Pipe<ServerResponseSchema> activeOutput = null;
	private final int messageFragmentsRequired;
	
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
            Pipe<HTTPRequestSchema>[] inputs, 
            Pipe<ServerResponseSchema>[] outputs,
			 HTTPSpecification<T, R, V, H> httpSpec, int payloadSizeBytes) {
			super(graphManager, inputs, outputs, httpSpec);
			
			this.inputs = inputs;
			this.outputs = outputs;		
			this.graphManager = graphManager;
			
			this.messageFragmentsRequired = (payloadSizeBytes/minVarLength(outputs))+2;//+2 for header and round up.
			int i = outputs.length;
			while (--i >= outputs.length) {
				if (messageFragmentsRequired* Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100)
						   >= outputs[i].sizeOfSlabRing) {
					throw new UnsupportedOperationException("\nMake pipe larger or lower max payload size: max message size "+payloadSizeBytes+" will not fit into pipe "+outputs[i]);
				}
			}

			assert(inputs.length == inputs.length);
			
			this.supportsBatchedPublish = false;
			this.supportsBatchedRelease = false;
			
			GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
			
	}
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
			                 Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
							 HTTPSpecification<T, R, V, H> httpSpec,
							 Pipe<?>[] otherInputs, int payloadSizeBytes) {
		super(graphManager, join(inputs,otherInputs), outputs, httpSpec);
		
		this.inputs = inputs;
		this.outputs = outputs;		
		this.graphManager = graphManager;
		
		this.messageFragmentsRequired = (payloadSizeBytes/minVarLength(outputs))+1;//+1 for header
		int i = outputs.length;
		while (--i >= outputs.length) {
			if (messageFragmentsRequired > outputs[i].sizeOfSlabRing) {
				throw new UnsupportedOperationException("\nMake pipe larger or lower max payload size: max message size "+payloadSizeBytes+" will not fit into pipe "+outputs[i]);
			}
		}
		
		assert(inputs.length == inputs.length);
		
		 GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);

	}

	
	@Override
	public void run() {
		
//		while ((activeChannelId != -1) 
//				&& (null!=activeOutput) 
//				&& Pipe.hasRoomForWrite(activeOutput) ) {
//			
//         TODO: this is for chunk-ing support where we could call for another run, Future feature...
//			
//		}		

		int i = this.inputs.length;
		while (--i >= 0) {			
			process(inputs[i], outputs[i]);			
		}

	}


	
	private boolean process(Pipe<HTTPRequestSchema> input, 
			                Pipe<ServerResponseSchema> output) {
		
		boolean didWork = false;
		//NOTE: the output writer is the high level while input is the low level.
		while (
				Pipe.hasRoomForWrite(output,
						messageFragmentsRequired * Pipe.sizeOf(output, ServerResponseSchema.MSG_TOCHANNEL_100) ) 
				&& Pipe.hasContentToRead(input)) {

			didWork = true;
			//logger.trace("has room and has data to write out from "+input);
		    
	//      ServerCoordinator.inServerCount.incrementAndGet();
    //  	  ServerCoordinator.start = System.nanoTime();
			
			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

		        	activeChannelId = Pipe.takeLong(input);
		        	activeSequenceNo = Pipe.takeInt(input);
		        	
		        	int temp = Pipe.takeInt(input);//verb
    	    	    //int routeId = temp>>>HTTPVerb.BITS;
	    	        int fieldVerb = HTTPVerb.MASK & temp;
		        			        	
		        	DataInputBlobReader<HTTPRequestSchema> paramStream = Pipe.openInputStream(input);//param
		        	
		        	int parallelRevision = Pipe.takeInt(input);
		        	//int parallelId = parallelRevision >>> HTTPRevision.BITS;
		        	//int fieldRevision = parallelRevision & HTTPRevision.MASK;

		        	activeFieldRequestContext = Pipe.takeInt(input);//context
		        	 
		        	//needed to keep telemetry going, not sure why...
		        	if (closeEveryRequest()) {
		        		activeFieldRequestContext |= OrderSupervisorStage.CLOSE_CONNECTION_MASK;
		        	}
		        	
		        			        	
		        	//must read context before calling this
		        	if (!sendResponse(output, 
		        			          paramStream, 
		        			          (HTTPVerbDefaults)httpSpec.verbs[fieldVerb])) {

		        		HTTPUtil.publishStatus(activeChannelId, activeSequenceNo, 404, output); 
		        	}
		        	
		        	activeChannelId = -1;
				break;
		        case -1:
		           //requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
		    Pipe.releaseReadLock(input);
		    
		}
		return didWork;
	}

	private EmptyBlockHolder ebh = new EmptyBlockHolder();
	
	private final boolean sendResponse(Pipe<ServerResponseSchema> output, 
			                       DataInputBlobReader<HTTPRequestSchema> params, HTTPVerbDefaults verb) {
		
		final boolean isChunked = false;
		final boolean isServer = true;
		//logger.info("sending:\n{}",payloadWorkspace);
        			
		///////////////////////////////////////
		//message 1 which contains the headers
		//////////////////////////////////////		
		ebh.holdEmptyBlock(activeChannelId, activeSequenceNo, output);
		
		//////////////////////////////////////////
		//begin message 2 which contains the body
		//////////////////////////////////////////
		
		DataOutputBlobWriter<ServerResponseSchema> outputStream = Pipe.openOutputStream(output);

		int startPos = Pipe.getWorkingBlobHeadPosition(output);
		
		boolean allWritten = payload(
						outputStream, 
						graphManager, 
						params, verb); //should return error and take args?

		if (!allWritten) {
			throw new UnsupportedOperationException("Not yet implemented support for chunks");
		}
		
		
		byte[] etagBytes = eTag();
		
		int totalLengthWritten = outputStream.length();
				
		//Writes all the data as a series of fragments
		int lengthCount = totalLengthWritten;
		boolean first = true;
		do {
			
			////////////
			int size = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100);
			Pipe.addLongValue(activeChannelId, output);
			Pipe.addIntValue(activeSequenceNo, output);
			
			int blockLen = Math.min(lengthCount, output.maxVarLen);
			if (first) {
				DataOutputBlobWriter.closeLowLeveLField(outputStream, blockLen);
			} else {
				first = false;
				Pipe.addBytePosAndLenSpecial(output, startPos, blockLen);
			}
			
			//TODO: can return false 404 if this is too large and we should.
			startPos += blockLen;
			lengthCount -= blockLen;
						
			Pipe.addIntValue(
					activeFieldRequestContext 
					| (lengthCount==0 ? OrderSupervisorStage.END_RESPONSE_MASK : 0), 
					output);//request context
			
			Pipe.confirmLowLevelWrite(output, size);

			//rotate around until length is done...
		} while (lengthCount>0);
		
        activeOutput = output;
		
		//NOTE: we can force redirects to this content location if desired.
		byte[] contentLocationBacking = null;		
		int contLocBytesPos = 0;
		int contLocBytesLen = 0;
		int contLocBytesMask = 0;
		
		/////////////
		//reposition to header so we can write it
		///////////
		ebh.openToEmptyBlock(outputStream); //TODO: make these static calls.
		
		writeHeader(
					HTTPRevisionDefaults.HTTP_1_1.getBytes(), //our supported revision
		 		    200, activeFieldRequestContext, 
		 		    etagBytes,  
		 		    contentType(), 
		 		    totalLengthWritten, 
		 		    isChunked, isServer,
		 		    contentLocationBacking, 
		 		    contLocBytesPos, contLocBytesLen, contLocBytesMask,
		 		    outputStream, 
		 		    1&(activeFieldRequestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
		
		ebh.finalizeLengthOfFirstBlock(outputStream);
				
		assert(outputStream.length()<=output.maxVarLen): "Header is too large or pipe max var size of "+output.maxVarLen+" is too small";
		//logger.trace("built new header response of length "+length);
		
		Pipe.publishWrites(output); //publish both the header and the payload which follows
		return true;
	}
	
	//return true if this was the entire payload.
	protected abstract boolean payload(AppendableByteWriter<?> payload,
			                          GraphManager gm, 
			                          ChannelReader params, 
			                          HTTPVerbDefaults verb);
	
	protected boolean continuation(AppendableByteWriter<?> payload,
			                       GraphManager gm) {
		return false;//only called when we need to chunk
	}	
	
	protected byte[] eTag() {
		return null;//override if the eTag and caching needs to be supported.
	}
	
	protected abstract byte[] contentType();
	
	protected boolean closeEveryRequest() {
		return false;
	}
	
}
