package com.ociweb;

import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestToBackEnd extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ConnectionData>[] connectionId;
	private final Pipe<ClientHTTPRequestSchema>[] clientRequests;
	
	public static RequestToBackEnd newInstance(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests) {
		return new RequestToBackEnd(graphManager, inputPipes, connectionId, clientRequests);
	}	
	
	public RequestToBackEnd(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests) {
		
		super(graphManager, inputPipes, join(clientRequests,connectionId));
		this.inputPipes = inputPipes;
		this.connectionId = connectionId;
		this.clientRequests = clientRequests;
		assert(inputPipes.length == connectionId.length);
		assert(inputPipes.length == clientRequests.length);
		
	}

	@Override
	public void run() {
	
		int i = inputPipes.length;
		while (--i >= 0) {
			process(inputPipes[i], connectionId[i], clientRequests[i]);
		}
		
	}

	private void process(
			Pipe<HTTPRequestSchema> sourceRequest, 
			Pipe<ConnectionData> targetConnectionData, 
			Pipe<ClientHTTPRequestSchema> targetClientRequest
			) {
	
		while (Pipe.hasContentToRead(sourceRequest) && 
			   Pipe.hasRoomForWrite(targetConnectionData) &&
			   Pipe.hasRoomForWrite(targetClientRequest)
				) {
		
			int requestIdx = Pipe.takeMsgIdx(sourceRequest);

			long connectionId = Pipe.takeLong(sourceRequest);
			int sequenceNo = Pipe.takeInt(sourceRequest);
			
			ConnectionData.publishConnectionData(targetConnectionData, 
												connectionId, sequenceNo);
			
			int verb       = Pipe.takeInt(sourceRequest);
			
			DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(sourceRequest);
			StructuredReader reader = inputStream.structured();
			
			
//			ClientHTTPRequestSchema.publishHTTPGet(output, 
//					fieldDestination, fieldSession, 
//					fieldPort, fieldHost, 
//					fieldPath, fieldHeaders);
	
			
			
			int revision   = Pipe.takeInt(sourceRequest);
			int context    = Pipe.takeInt(sourceRequest);
			
			Pipe.confirmLowLevelRead(sourceRequest, Pipe.sizeOf(sourceRequest, requestIdx));
			Pipe.releaseReadLock(sourceRequest);
					
			
		}
		
		
	}





};

