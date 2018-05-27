package com.ociweb.pronghorn.network.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.HTTPUtilResponse;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.HeaderWritable;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.AppendableByteWriter;

public abstract class AbstractAppendablePayloadResponseStage <   
                                T extends Enum<T> & HTTPContentType,
								R extends Enum<R> & HTTPRevision,
								V extends Enum<V> & HTTPVerb,
								H extends Enum<H> & HTTPHeader> 
						extends PronghornStage {

	protected final HTTPSpecification<T,R,V, H> httpSpec;
	
	private final Pipe<HTTPRequestSchema>[] inputs;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final GraphManager graphManager;		
	private static final Logger logger = LoggerFactory.getLogger(AbstractAppendablePayloadResponseStage.class);
	
	private final int messageFragmentsRequired;	
	public final HTTPUtilResponse ebh = new HTTPUtilResponse();
	
	
	public AbstractAppendablePayloadResponseStage(GraphManager graphManager, 
            Pipe<HTTPRequestSchema>[] inputs, 
            Pipe<ServerResponseSchema>[] outputs,
			HTTPSpecification<T, R, V, H> httpSpec, int payloadSizeBytes) {
			super(graphManager, inputs, outputs);
			
			this.httpSpec = httpSpec;
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
			                 Pipe<HTTPRequestSchema>[] inputs,
			                 Pipe<ServerResponseSchema>[] outputs,
							 HTTPSpecification<T, R, V, H> httpSpec,
							 Pipe<?>[] otherInputs, int payloadSizeBytes) {
		super(graphManager, join(inputs,otherInputs), outputs);
		
		this.httpSpec = httpSpec;
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
		int i = this.inputs.length;
		while (--i >= 0) {			
			process(inputs[i], outputs[i]);			
		}
	}
	
	private void process(Pipe<HTTPRequestSchema> input, 
			                Pipe<ServerResponseSchema> output) {

		while (
				Pipe.hasRoomForWrite(output,
				messageFragmentsRequired * Pipe.sizeOf(output, ServerResponseSchema.MSG_TOCHANNEL_100) ) 
				&& Pipe.hasContentToRead(input)) {

			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

		        	long activeChannelId = Pipe.takeLong(input);
		        	int activeSequenceNo = Pipe.takeInt(input);
		        	
		        	int temp = Pipe.takeInt(input);//verb
    	    	    //int routeId = temp>>>HTTPVerb.BITS;
	    	        int fieldVerb = HTTPVerb.MASK & temp;
		        			        	
		        	DataInputBlobReader<HTTPRequestSchema> paramStream = Pipe.openInputStream(input);//param
		        	
		        	int parallelRevision = Pipe.takeInt(input);
		        	//int parallelId = parallelRevision >>> HTTPRevision.BITS;
		        	//int fieldRevision = parallelRevision & HTTPRevision.MASK;

		        	int activeFieldRequestContext = Pipe.takeInt(input);
	
				    ChannelWriter outputStream = HTTPUtilResponse.openHTTPPayload(ebh, output, activeChannelId, activeSequenceNo);
					
					boolean allWritten = payload(outputStream, graphManager, paramStream, (HTTPVerbDefaults)httpSpec.verbs[fieldVerb]); //should return error and take args?
					if (!allWritten) {
						throw new UnsupportedOperationException("Not yet implemented support for chunks");
						//TODO: can return false 404 if this is too large and we should.
							//					boolean sendResponse = true;
							//					if (!sendResponse) {
							//
							//		        		HTTPUtil.publishStatus(activeChannelId, activeSequenceNo, 404, output); 
							//		        	}
					}				
					
					HeaderWritable additionalHeaderWriter = null;
					
					HTTPUtilResponse.closePayloadAndPublish(ebh, eTag(), contentType(),
						output, activeChannelId, activeSequenceNo, activeFieldRequestContext,
						outputStream, additionalHeaderWriter);
		        	
		        			        	
		        	//must read context before calling this
		        	
		        	activeChannelId = -1;
				break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
		    Pipe.releaseReadLock(input);
		    
		}
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
	
	public byte[] eTag() {
		return null;//override if the eTag and caching needs to be supported.
	}
	
	public abstract HTTPContentType contentType();
	
}
