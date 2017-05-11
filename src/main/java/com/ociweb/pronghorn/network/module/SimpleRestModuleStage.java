package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.http.AbstractRestStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SimpleRestModuleStage<                 T extends Enum<T> & HTTPContentType,
													R extends Enum<R> & HTTPRevision,
													V extends Enum<V> & HTTPVerb,
													H extends Enum<H> & HTTPHeader> extends AbstractRestStage<T,R,V,H> {

    
    private final Pipe<HTTPRequestSchema> input;    
    private final Pipe<ServerResponseSchema> output;
    private final SimpleRestLogic logic;
    
    private final int MAX_TEXT_LENGTH = 64;
    private final Pipe<RawDataSchema> digitBuffer = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance,3,MAX_TEXT_LENGTH));
    
	protected SimpleRestModuleStage(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output,
			HTTPSpecification<T, R, V, H> httpSpec, SimpleRestLogic logic) {
		super(graphManager, input, output, httpSpec);
		
		this.input = input;
		this.output = output;
		this.logic = logic;
		
	}
	
	@Override
	public void startup() {
		digitBuffer.initBuffers();
	}

    @Override
	public void run() {
		
		
		if (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
			
			
			int id = Pipe.takeMsgIdx(input);
			if (HTTPRequestSchema.MSG_RESTREQUEST_300==id) {

				final DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.inputStream(input);
				final DataOutputBlobWriter<ServerResponseSchema> outputStream = Pipe.outputStream(output);
				
				//TODO: since this is on the same input/output pipes it can be the same instance.
				RestResponder<T> responder = new RestResponder<T>() {
					
					@Override
					public DataOutputBlobWriter<ServerResponseSchema> beginResponse(int status, T contentType, int length) {
						
						final long channelId = Pipe.takeLong(input); //pass along
						final int  sequenceId = Pipe.takeInt(input); //pass along, critical to ensure ordering.			
						
						
						final int  verb = Pipe.takeInt(input); 
						inputStream.openLowLevelAPIField();
						final int revision = Pipe.takeInt(input); //not used...
						final int context =  Pipe.peekInt(input); //do not move pointer forward since we will read it a gain later
														
						
						Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100);
						Pipe.addLongValue(channelId, output);
						Pipe.addIntValue(sequenceId, output);						
						outputStream.openField();
						
						byte[] revisionBytes = httpSpec.revisions[revision].getBytes();
												
						byte[] etagBytes = null;//TODO: nice feature to add later
												
						writeHeader(revisionBytes, status, context, etagBytes, contentType.getBytes(), 
									length, false, null, 0,0,0,
						outputStream, 1&(context>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
												
						return outputStream;
					}
					
				};
				
				
				logic.process(inputStream, responder); //client can read input stream and do work before telling responder status etc.
											
				outputStream.closeLowLevelField();
			
				final int context = Pipe.takeInt(input);				
				Pipe.addIntValue(context, output);
				
				Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100));
				Pipe.publishWrites(output);
				
				Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, id));
				Pipe.publishWrites(output);
				
				
				//TODO: change unwrap to low level as well.
				//TODO: router needs to send this the right message type...
			
				
			} else if (-1 == id) {
				
				Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
				Pipe.releaseReadLock(input);
				requestShutdown();
				
			} else {
				
				throw new UnsupportedOperationException();
				
			}
		}
		
		
	}

}
