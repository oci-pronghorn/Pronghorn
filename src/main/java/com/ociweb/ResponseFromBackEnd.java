package com.ociweb;

import com.ociweb.pronghorn.network.HTTPUtilResponse;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ResponseFromBackEnd extends PronghornStage {

	private final Pipe<NetResponseSchema>[] clientResponses;
	private final Pipe<ConnectionData>[] connectionId;
	private final Pipe<ServerResponseSchema>[] responses;
	
	public final HTTPUtilResponse ebh = new HTTPUtilResponse();
	
	public static ResponseFromBackEnd newInstance(GraphManager graphManager,
			Pipe<NetResponseSchema>[] clientResponses,
			Pipe<ConnectionData>[] connectionId,
			Pipe<ServerResponseSchema>[] responses) {
		return new ResponseFromBackEnd(graphManager, clientResponses, connectionId, responses);
	}	
	
	public ResponseFromBackEnd(GraphManager graphManager,
			Pipe<NetResponseSchema>[] clientResponses,
			Pipe<ConnectionData>[] connectionId,
			Pipe<ServerResponseSchema>[] responses) {
		super(graphManager, join(clientResponses,connectionId), responses);
		
		this.clientResponses = clientResponses;
		this.connectionId = connectionId;
		this.responses = responses;
		assert(clientResponses.length==connectionId.length);
		assert(clientResponses.length==responses.length);		
				
	}

	@Override
	public void run() {
		
		int i = clientResponses.length;
		while (--i >= 0) {
			process(clientResponses[i], connectionId[i], responses[i]);
		}
		
	}

	private void process(
			Pipe<NetResponseSchema> sourceResponse, 
			Pipe<ConnectionData> sourceConnectionData, 
			Pipe<ServerResponseSchema> output) {
			
		if ((!Pipe.hasContentToRead(sourceConnectionData)) ||
		    (!Pipe.hasContentToRead(sourceResponse)) ) {
			return;
		}
				
		/////////////////////////
		int idx = Pipe.takeMsgIdx(sourceConnectionData);
		long activeChannelId = Pipe.takeLong(sourceConnectionData);
		int activeSequenceNo = Pipe.takeInt(sourceConnectionData);
		
		Pipe.confirmLowLevelRead(sourceConnectionData, Pipe.sizeOf(sourceConnectionData, idx));
		Pipe.releaseReadLock(sourceConnectionData);
		
		////////////////////////
		
		int idx2 = Pipe.takeMsgIdx(sourceResponse);
		long channelIdx2 = Pipe.takeLong(sourceResponse);
		int xxx = Pipe.takeInt(sourceResponse);
		DataInputBlobReader<?> inputStream = Pipe.openInputStream(sourceResponse);
		StructuredReader reader = inputStream.structured();
		
		Pipe.confirmLowLevelRead(sourceResponse, Pipe.sizeOf(sourceResponse, idx2));
		Pipe.releaseReadLock(sourceResponse);
		
		/////////////////////////
		
		//TODO: do we have room to write for the proxy??
		
		ChannelWriter outputStream = HTTPUtilResponse.openHTTPPayload(ebh, output, 
				                     activeChannelId, 
				                     activeSequenceNo);
		
		
		//TODO: copy data from the input stream response and send it out as the body here.
		
		
		
		int activeFieldRequestContext = 0;
	
		
		///TODO: write custom headers from the client call?
		
		
		HTTPUtilResponse.closePayloadAndPublish(
				ebh, null, null, 
				output, activeChannelId, activeSequenceNo, 
				activeFieldRequestContext, outputStream);
		
		
		
	}

}
