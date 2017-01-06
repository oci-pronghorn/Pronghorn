package com.ociweb.pronghorn.network.module;

import com.ociweb.pronghorn.network.AbstractRestStage;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SimpleRestModuleStage<   T extends Enum<T> & HTTPContentType,
													R extends Enum<R> & HTTPRevision,
													V extends Enum<V> & HTTPVerb,
													H extends Enum<H> & HTTPHeaderKey> extends AbstractRestStage<T,R,V,H> {

    
    private final Pipe<HTTPRequestSchema> input;    
    private final Pipe<ServerResponseSchema> output;
    private final SimpleRestLogic logic;
    
	protected SimpleRestModuleStage(GraphManager graphManager, Pipe<HTTPRequestSchema> input, Pipe<ServerResponseSchema> output,
			HTTPSpecification<T, R, V, H> httpSpec, SimpleRestLogic logic) {
		super(graphManager, input, output, httpSpec);
		
		this.input = input;
		this.output = output;
		this.logic = logic;
		
	}

	@Override
	public void run() {
		
		
		if (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
			
			
			int id = Pipe.takeMsgIdx(input);
			if (HTTPRequestSchema.MSG_RESTREQUEST_300==id) {
		
				long channelId = Pipe.takeLong(input); //pass along
				int  sequenceId = Pipe.takeInt(input); //pass along, critical to ensure ordering.				
				int  verb = Pipe.takeInt(input); 
				DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.inputStream(input);
				inputStream.openLowLevelAPIField();
				
				
				
				int writeSize = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100);
				Pipe.addLongValue(channelId, output);
				Pipe.addIntValue(sequenceId, output);
			
				
				DataOutputBlobWriter<ServerResponseSchema> outputStream = Pipe.outputStream(output);
				outputStream.openField();
				
				logic.process(inputStream,outputStream);
							
				outputStream.closeLowLevelField();
							
				int revision = Pipe.takeInt(input); //not used...
				int context = Pipe.takeInt(input); //pass along, can close conection if we desire
				Pipe.addIntValue(context, output);
				
				Pipe.confirmLowLevelWrite(output, writeSize);
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
