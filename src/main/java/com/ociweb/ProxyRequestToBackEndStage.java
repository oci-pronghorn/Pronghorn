package com.ociweb;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ProxyRequestToBackEndStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ConnectionData>[] connectionId;
	private final Pipe<ClientHTTPRequestSchema>[] clientRequests;
	private final String targetHost;
	private final int targetPort;
	
	public static ProxyRequestToBackEndStage newInstance(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests,
			String targetHost, int targetPort) {
		return new ProxyRequestToBackEndStage(graphManager, inputPipes, 
				                    connectionId, clientRequests, 
				                    targetHost, targetPort);
	}	
	
	public ProxyRequestToBackEndStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes, 
			Pipe<ConnectionData>[] connectionId, 
			Pipe<ClientHTTPRequestSchema>[] clientRequests,
			String targetHost, int targetPort) {
		
		super(graphManager, inputPipes, join(clientRequests,connectionId));
		this.inputPipes = inputPipes;
		this.connectionId = connectionId;
		this.clientRequests = clientRequests;
		assert(inputPipes.length == connectionId.length);
		assert(inputPipes.length == clientRequests.length);
		this.targetHost = targetHost;
		this.targetPort = targetPort;
		
		assert(targetHost!=null);
		ClientCoordinator.registerDomain(targetHost);
	}

	@Override
	public void run() {
	
		int i = inputPipes.length;
		while (--i >= 0) {
			process(inputPipes[i], connectionId[i], clientRequests[i]);
		}
		
	}
	
//	httpRequestReader.structured().visit(HTTPHeader.class, (header,reader) -> {
//		  if (   (header != HTTPHeaderDefaults.HOST)
//	            	&& (header != HTTPHeaderDefaults.CONNECTION)	){
//	  
//	            	writer.write((HTTPHeader)header,
//	            			     httpRequestReader.getSpec(), 
//	            			     reader);
//         }
//});

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

			final long connectionId = Pipe.takeLong(sourceRequest);
			final int sequenceNo = Pipe.takeInt(sourceRequest);
						
			int verb       = Pipe.takeInt(sourceRequest);
			
			DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(sourceRequest);
		
			int fieldDestination = 0; //pipe index for the response
			int fieldSession = 0; //value for us to know which this belongs
			
			assert(fieldDestination>=0);
						
			int size = Pipe.addMsgIdx(targetClientRequest, ClientHTTPRequestSchema.MSG_HTTPGET_100);
			
			Pipe.addIntValue(fieldDestination, targetClientRequest);
			Pipe.addIntValue(fieldSession, targetClientRequest);
			Pipe.addIntValue(targetPort, targetClientRequest);
			Pipe.addUTF8(targetHost, targetClientRequest);
			
			StructuredReader reader = inputStream.structured();
			
			DataOutputBlobWriter<ClientHTTPRequestSchema> outputStream = Pipe.openOutputStream(targetClientRequest);			
			reader.readText(WebFields.proxyGet, outputStream);
			DataOutputBlobWriter.closeLowLevelField(outputStream);
						
			Pipe.addUTF8(null, targetClientRequest); //no headers
			
			Pipe.confirmLowLevelWrite(targetClientRequest, size);
			Pipe.publishWrites(targetClientRequest);
			
			int revision   = Pipe.takeInt(sourceRequest);
			final int context    = Pipe.takeInt(sourceRequest);
			
			//TODO: in the future this will be held by the connection so this part will not be needed
			//      also upon disconnection it will be auto disposed...
			int size2 = Pipe.addMsgIdx(targetConnectionData, ConnectionData.MSG_CONNECTIONDATA_1);
			Pipe.addLongValue(connectionId, targetConnectionData);
			Pipe.addIntValue(sequenceNo, targetConnectionData);
			Pipe.addIntValue(context, targetConnectionData);
			Pipe.confirmLowLevelWrite(targetConnectionData, size2);
			Pipe.publishWrites(targetConnectionData);
			
			Pipe.confirmLowLevelRead(sourceRequest, Pipe.sizeOf(sourceRequest, requestIdx));
			Pipe.releaseReadLock(sourceRequest);
								
		}
	}
}

