package com.ociweb.pronghorn.network.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.OrderSupervisorStage;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.AbstractRestStage;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableBuilder;

public abstract class AbstractAppendablePayloadResponseStage <   
                                T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractRestStage<T,R,V,H> {

	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final GraphManager graphManager;
	private AppendableBuilder payloadWorkspace;
		
	private static final Logger logger = LoggerFactory.getLogger(AbstractAppendablePayloadResponseStage.class);
	
	private long activeChannelId = -1;
	private int activeSequenceNo = -1;
	private int activeFieldRequestContext = -1;	
	private int workingPosition = 0;
	private Pipe<ServerResponseSchema> activeOutput = null;
	
	private int maximumAllocation = 1<<27; //128M largest file, should expose this
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
            Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
			 HTTPSpecification<T, R, V, H> httpSpec) {
			super(graphManager, inputs, outputs, httpSpec);
			
			this.inputs = inputs;
			this.outputs = outputs;		
			this.graphManager = graphManager;
			
			assert(inputs.length == inputs.length);
			
			this.supportsBatchedPublish = false;
			this.supportsBatchedRelease = false;
			
	}
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
			                 Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
							 HTTPSpecification<T, R, V, H> httpSpec, Pipe[] otherInputs) {
		super(graphManager, join(inputs,otherInputs), outputs, httpSpec);
		
		this.inputs = inputs;
		this.outputs = outputs;		
		this.graphManager = graphManager;
		
		assert(inputs.length == inputs.length);
	}

	@Override
	public void startup() {
		payloadWorkspace = new AppendableBuilder(maximumAllocation);
	}
	
	@Override
	public void run() {
		
		while ((activeChannelId != -1) && (null!=activeOutput) && PipeWriter.hasRoomForWrite(activeOutput)   ) {
			
			PipeWriter.presumeWriteFragment(activeOutput, ServerResponseSchema.MSG_TOCHANNEL_100);
		    PipeWriter.writeLong(activeOutput,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21, activeChannelId);
		    PipeWriter.writeInt(activeOutput,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, activeSequenceNo);
		    				    
		    DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(activeOutput);
		    DataOutputBlobWriter.openField(outputStream);
		    
		    appendRemainingPayload(activeOutput);
			
		}		
		
		//only do when previous is complete.
		if (null == activeOutput) {
			int i = this.inputs.length;
			while ((--i >= 0) && (activeChannelId == -1)) {			
				process(inputs[i], outputs[i]);			
			}
		}
		
	}


	
	private void process(Pipe<HTTPRequestSchema> input, 
			             Pipe<ServerResponseSchema> output) {
		
		//NOTE: the output writer is the high level while input is the low level.
		while ( PipeWriter.hasRoomForWrite(output) &&
				Pipe.hasContentToRead(input)) {
			
			//logger.trace("has room and has data to write out from "+input);
		    
	//      ServerCoordinator.inServerCount.incrementAndGet();
    //  	  ServerCoordinator.start = System.nanoTime();
			
			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

		        	activeChannelId = Pipe.takeLong(input);
		        	activeSequenceNo = Pipe.takeInt(input);
		        	
		        	int temp = Pipe.takeInt(input);//verb
    	    	    int routeId = temp>>>HTTPVerb.BITS;
	    	        int fieldVerb = HTTPVerb.MASK & temp;
		        			        	
		        	DataInputBlobReader<HTTPRequestSchema> paramStream = Pipe.openInputStream(input);//param
		        	
		        	int parallelRevision = Pipe.takeInt(input);
		        	int parallelId = parallelRevision >>> HTTPRevision.BITS;
		        	int fieldRevision = parallelRevision & HTTPRevision.MASK;

		        	activeFieldRequestContext = Pipe.takeInt(input);//context
		        			        	
		        	//must read context before calling this
		        	if (!sendResponse(output, fieldRevision, paramStream, 
		        			          (HTTPVerbDefaults)httpSpec.verbs[fieldVerb])) {

		        		HTTPUtil.publishStatus(activeChannelId, activeSequenceNo, 404, output); 
		        	}
		        	
		        	
				break;
		        case -1:
		           //requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
		    Pipe.releaseReadLock(input);
		    
		}
		
	}

	private final boolean sendResponse(Pipe<ServerResponseSchema> output, int fieldRevision, 
			                       DataInputBlobReader<HTTPRequestSchema> params, HTTPVerbDefaults verb) {
		
		//logger.info("sending:\n{}",payloadWorkspace);
        			
		PipeWriter.presumeWriteFragment(output, ServerResponseSchema.MSG_TOCHANNEL_100);
		PipeWriter.writeLong(output,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21, activeChannelId);
		PipeWriter.writeInt(output,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, activeSequenceNo);
						    
		DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(output);
		DataOutputBlobWriter.openField(outputStream);
		
		payloadWorkspace.clear();
		byte[] etagBytes = payload(payloadWorkspace, graphManager, params, verb); //should return error and take args?
        
        activeOutput = output;
		workingPosition = 0;
	
		final boolean isChunked = false;
		final boolean isServer = true;
		
		//NOTE: we can force redirects to this content location if desired.
		byte[] contentLocationBacking = null;		
		int contLocBytesPos = 0;
		int contLocBytesLen = 0;
		int contLocBytesMask = 0;
		
		writeHeader(httpSpec.revisions[fieldRevision].getBytes(), 
		 		    200, activeFieldRequestContext, 
		 		    etagBytes,  
		 		    contentType(), 
		 		    payloadWorkspace.byteLength(), 
		 		    isChunked, isServer,
		 		   contentLocationBacking, contLocBytesPos, contLocBytesLen,  contLocBytesMask,
		 		    outputStream, 
		 		    1&(activeFieldRequestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
		
		assert(outputStream.length()<=output.maxVarLen): "Header is too large or pipe max var size of "+output.maxVarLen+" is too small";
		//logger.trace("built new header response of length "+length);
		
		appendRemainingPayload(output);
		return true;
	}
	
	protected abstract byte[] payload(Appendable payload, GraphManager gm, DataInputBlobReader<HTTPRequestSchema> params, HTTPVerbDefaults verb);

	protected abstract byte[] contentType();
	
	private void appendRemainingPayload(Pipe<ServerResponseSchema> output) {
		
		DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(output);
		
		int sendLength;

		// div by 6 to ensure bytes room. //NOTE: could be faster if needed in the future.
		if ((sendLength = Math.min((payloadWorkspace.byteLength() - workingPosition),
				                       (outputStream.remaining()/6) )) >= 1) {
			
			//System.err.print(payloadWorkspace.substring(workingPosition, workingPosition+sendLength));
			//logger.info("send length {} vs {} ", sendLength, payloadWorkspace.length());
			
			workingPosition += payloadWorkspace.copyTo(sendLength, outputStream);

		}
		
		if (workingPosition==payloadWorkspace.byteLength()) {
		
			//System.err.println();
			//logger.info("done with sending \n{}",payloadWorkspace);
			
			
			activeFieldRequestContext |=  OrderSupervisorStage.END_RESPONSE_MASK;
			
			//NOTE: we MUST close this or the telemetry data feed will hang on the browser side
			//      TODO: we may want a way to define this in construction.
			activeFieldRequestContext |=  OrderSupervisorStage.CLOSE_CONNECTION_MASK;
			
			//mark all done.
			payloadWorkspace.clear();
			activeChannelId = -1;
			activeSequenceNo = -1;
			workingPosition = 0;
			activeOutput = null;
		} 
		
		assert(outputStream.length()<=output.maxVarLen): "Header is too large or pipe max var size of "+output.maxVarLen+" is too small";

		DataOutputBlobWriter.closeHighLevelField(outputStream, ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_PAYLOAD_25);
 		
		PipeWriter.writeInt(output,
				            ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24, 
							activeFieldRequestContext);
				
		PipeWriter.publishWrites(output);
	
	}

}
