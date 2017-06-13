package com.ociweb.pronghorn.network.mqtt;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerFactory;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.PersistedBlobStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class MQTTClientGraphBuilder {


	public static Pipe<MQTTClientResponseSchema> buildMQTTClientGraph(GraphManager gm, Pipe<MQTTClientRequestSchema> clientRequest) {
		
		final boolean isTLS = true;
		
		int maxInFlight = 10;
		int maximumLenghOfVariableLengthFields = 4096;

		Pipe<MQTTClientResponseSchema> clientResponse = MQTTClientResponseSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
				
        //we are not defining he other side of the request and response....
		
		buildMQTTClientGraph(gm, isTLS, maxInFlight, maximumLenghOfVariableLengthFields, clientRequest, clientResponse);
		
		
		
		return clientResponse;
	}

	public static void buildMQTTServerBraph(GraphManager gm) {
		
//		ServerCoordinator coordinator;
//		boolean isLarge;
//		boolean isTLS;
//		ServerFactory factory;
//		NetGraphBuilder.buildSimpleServerGraph(gm, coordinator, isLarge, isTLS, factory);
		
	}
	
	public static void buildMQTTClientGraph(GraphManager gm, final boolean isTLS, int maxInFlight,
												int maximumLenghOfVariableLengthFields, Pipe<MQTTClientRequestSchema> clientRequest,
												Pipe<MQTTClientResponseSchema> clientResponse) {
		int minimumFragmentsOnRing = (maxInFlight*2)+10;
		final int connectionsInBits = 2;
		final int maxPartialResponses = 4;
		
		Pipe<MQTTClientToServerSchema> clientToServer = MQTTClientToServerSchema.instance.newPipe(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields); //from the application 
		
		final Pipe<MQTTServerToClientSchema> serverToClient = MQTTServerToClientSchema.instance.newPipe(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields); //from the response
		
		Pipe<MQTTIdRangeSchema> idGenNew = MQTTIdRangeSchema.instance.newPipe(4,0);
		Pipe<MQTTIdRangeSchema> idGenOld = MQTTIdRangeSchema.instance.newPipe(4,0);
		
		new IdGenStage(gm, idGenOld, idGenNew);		
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, isTLS);
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {			
			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] fromBroker,
									Pipe<ReleaseSchema> ackReleaseForResponseParser) {
				
				//parse the socket data, determine the message and write it to the out pipe
				new MQTTClientResponseStage(gm, ccm, fromBroker, ackReleaseForResponseParser, serverToClient); //releases Ids.		
			}
		};
				
		Pipe<PersistedBlobStoreSchema> persistancePipe = PersistedBlobStoreSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		Pipe<PersistedBlobLoadSchema> persistanceLoadPipe = PersistedBlobLoadSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		byte multi = 4;//x time the pipe size
		byte maxValueBits = 20;
		File rootFolder = null;
		try {
			rootFolder = new File(Files.createTempDirectory("mqttClientData").toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		new PersistedBlobStage(gm, persistancePipe, persistanceLoadPipe, multi, maxValueBits, rootFolder );
		
		int independentClients = 1; 
		
		Pipe<NetPayloadSchema>[] toBroker = Pipe.buildPipes(independentClients, NetPayloadSchema.instance.newPipeConfig(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields));
		
		//take input request and write the bytes to the broker socket
		int uniqueId = 1; //Different for each client instance. so ccm and toBroker can be shared across all clients.
		new MQTTClientToServerEncodeStage(gm, ccm, maxInFlight, uniqueId, clientToServer, persistancePipe, persistanceLoadPipe, toBroker);

		new MQTTClient(gm, clientRequest, idGenNew, serverToClient, clientResponse, idGenOld, clientToServer);
		
		NetGraphBuilder.buildSimpleClientGraph(gm, ccm, factory, toBroker);
	}
	
	
	
	
}
