package com.ociweb.pronghorn.network.module;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public abstract class AbstractAppendablePayloadResponseStage <   
                                T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> extends AbstractRestStage<T,R,V,H> {

	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final GraphManager graphManager;
	private StringBuilder payloadWorkspace;
		
	private static final Logger logger = LoggerFactory.getLogger(AbstractAppendablePayloadResponseStage.class);
	
	private long activeChannelId = -1;
	private int activeSequenceNo = -1;
	private int activeFieldRequestContext = -1;	
	private int workingPosition = 0;
	private Pipe<ServerResponseSchema> activeOutput = null;
	
	private static SecureRandom random;
	static {
		try {
			random = SecureRandom.getInstanceStrong();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static AtomicInteger eTagCounter = new AtomicInteger();  
	private final int eTagInt;
	private final int eTagRoot = random.nextInt();
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
            Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
			 HTTPSpecification<T, R, V, H> httpSpec) {
			super(graphManager, inputs, outputs, httpSpec);
			
			this.inputs = inputs;
			this.outputs = outputs;		
			this.graphManager = graphManager;
			this.eTagInt = eTagCounter.incrementAndGet();
			
			assert(inputs.length == inputs.length);
	}
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
			                 Pipe<HTTPRequestSchema>[] inputs, Pipe<ServerResponseSchema>[] outputs,
							 HTTPSpecification<T, R, V, H> httpSpec, Pipe[] otherInputs) {
		super(graphManager, join(inputs,otherInputs), outputs, httpSpec);
		
		this.inputs = inputs;
		this.outputs = outputs;		
		this.graphManager = graphManager;
		this.eTagInt = eTagCounter.incrementAndGet();
		
		assert(inputs.length == inputs.length);
	}

	@Override
	public void startup() {
		payloadWorkspace = new StringBuilder(2048);
	}
	
	@Override
	public void run() {
		
		while ((activeChannelId != -1) && (null!=activeOutput) && PipeWriter.hasRoomForWrite(activeOutput)   ) {
			
			//System.err.println(Pipe.hasRoomForWrite(activeOutput)+" "+activeOutput);
			
			PipeWriter.presumeWriteFragment(activeOutput, ServerResponseSchema.MSG_TOCHANNEL_100);
		    PipeWriter.writeLong(activeOutput,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21, activeChannelId);
		    PipeWriter.writeInt(activeOutput,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, activeSequenceNo);
		    				    
		    DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(activeOutput);
		    DataOutputBlobWriter.openField(outputStream);
		    
		    appendRemainingPayload(activeOutput);
			
		}		
		
		int i = this.inputs.length;
		while ((--i >= 0) && (activeChannelId == -1)) {			
			process(inputs[i], outputs[i]);			
		}
		
	}


	
	private void process(Pipe<HTTPRequestSchema> input, 
			             Pipe<ServerResponseSchema> output) {
		
		
		while ( PipeWriter.hasRoomForWrite(output) &&
				PipeReader.tryReadFragment(input)) {
			
			//logger.trace("has room and has data to write out from "+input);
		    
			int msgIdx = PipeReader.getMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

		        	activeChannelId = PipeReader.readLong(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_CHANNELID_21);
		        	activeSequenceNo = PipeReader.readInt(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_SEQUENCE_26);   			        	
		        	int temp = PipeReader.readInt(input, HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_VERB_23);
    	    	    int routeId = temp>>>HTTPVerb.BITS;
	    	        int fieldVerb = HTTPVerb.MASK & temp;
		        	
		        	activeFieldRequestContext = PipeReader.readInt(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_REQUESTCONTEXT_25);
		        	
		        	int parallelRevision = PipeReader.readInt(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_REVISION_24);
		        	int parallelId = parallelRevision >>> HTTPRevision.BITS;
		        	int fieldRevision = parallelRevision & HTTPRevision.MASK;
		        			        	
		        	DataInputBlobReader<HTTPRequestSchema> paramStream = PipeReader.inputStream(input, HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_PARAMS_32);
		        	
		        	if (!sendResponse(output, fieldRevision, paramStream, (HTTPVerbDefaults)httpSpec.verbs[fieldVerb])) {

		        		HTTPUtil.publishError(activeSequenceNo, 404, output, 
		        			activeChannelId, httpSpec, fieldRevision); 
		        	}
		        	
		        	
				break;
		        case -1:
		           //requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		    
		}
		
	}

	protected boolean sendResponse(Pipe<ServerResponseSchema> output, int fieldRevision, 
			                       DataInputBlobReader<HTTPRequestSchema> params, HTTPVerbDefaults verb) {
		
		payloadWorkspace.setLength(0);
		byte[] contentType = buildPayload(payloadWorkspace, graphManager, params, verb); //should return error and take args?
		if (null==contentType) {
			return false; //Can not write anything. This is the error case.
		}
		
		int length = payloadWorkspace.length();
		
		
		byte[] revision = httpSpec.revisions[fieldRevision].getBytes();
		int status=200;
		 

		///////////
		//not needed because we do the same thing for every request
		///////////
		//int fieldVerb = PipeReader.readInt(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_VERB_23);
		//ByteBuffer fieldParams = PipeReader.readBytes(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_PARAMS_32,ByteBuffer.allocate(PipeReader.readBytesLength(input,HTTPRequestSchema.MSG_RESTREQUEST_300_FIELD_PARAMS_32)));
		///////////		        	
		
		PipeWriter.presumeWriteFragment(output, ServerResponseSchema.MSG_TOCHANNEL_100);
		PipeWriter.writeLong(output,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21, activeChannelId);
		PipeWriter.writeInt(output,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23, activeSequenceNo);
						    
		DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(output);
		
		DataOutputBlobWriter.openField(outputStream);
						    

		activeOutput = output;
							
		workingPosition = 0;
		
		///long eTag = (((long)eTagRoot)<<32) + ((long)eTagInt);					
		byte[] etagBytes = null;//Appendables.appendHexDigits(new StringBuilder(), eTag).toString().getBytes(); 
        boolean isChunked = false;
		
		writeHeader(revision, 
		 		    status, activeFieldRequestContext, 
		 		    etagBytes,  
		 		    contentType, length, isChunked, true,
		 		    null, 0, 0,  0,
		 		    outputStream, 
		 		    1&(activeFieldRequestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
		
		//logger.trace("built new header response of length "+length);
		
		appendRemainingPayload(output);
		return true;
	}
	
	protected abstract byte[] buildPayload(Appendable payload, GraphManager gm, DataInputBlobReader<HTTPRequestSchema> params, HTTPVerbDefaults verb);


	private void appendRemainingPayload(Pipe<ServerResponseSchema> output) {
		
		DataOutputBlobWriter<ServerResponseSchema> outputStream = PipeWriter.outputStream(output);
		
		int sendLength;

		// div by 6 to ensure bytes room. //NOTE: could be faster if needed in the future.
		while ((sendLength = Math.min((payloadWorkspace.length()-workingPosition), (outputStream.remaining()/6) )) >= 1) { 
			outputStream.append(payloadWorkspace, workingPosition, workingPosition+sendLength);			    
			workingPosition+=sendLength;
		}
		
		if (workingPosition==payloadWorkspace.length()) {
		
			activeFieldRequestContext |=  OrderSupervisorStage.END_RESPONSE_MASK;
			
			//mark all done.
			payloadWorkspace.setLength(0);
			activeChannelId = -1;
			activeSequenceNo = -1;
			workingPosition = 0;
			activeOutput = null;
		} 
		
		DataOutputBlobWriter.closeHighLevelField(outputStream, ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_PAYLOAD_25);
 
		
		PipeWriter.writeInt(output,ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_REQUESTCONTEXT_24, 
							activeFieldRequestContext);
				
		PipeWriter.publishWrites(output);
	
	}

}
