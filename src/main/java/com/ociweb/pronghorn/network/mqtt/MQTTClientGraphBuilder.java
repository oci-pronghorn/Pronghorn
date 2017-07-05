package com.ociweb.pronghorn.network.mqtt;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.file.PersistedBlobStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.JSONTap;

public class MQTTClientGraphBuilder {


	public static Pipe<MQTTClientResponseSchema> buildMQTTClientGraph(GraphManager gm, Pipe<MQTTClientRequestSchema> clientRequest) {
		
		final boolean isTLS = true;
		
		int maxInFlight = 10;
		int maximumLenghOfVariableLengthFields = 4096;
		int rate = 1_200;

		Pipe<MQTTClientResponseSchema> clientResponse = MQTTClientResponseSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
				
        //we are not defining he other side of the request and response....
		
		buildMQTTClientGraph(gm, isTLS, maxInFlight, maximumLenghOfVariableLengthFields, clientRequest, clientResponse, rate);
				
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
												int maximumLenghOfVariableLengthFields, 
												Pipe<MQTTClientRequestSchema> clientRequest,
												Pipe<MQTTClientResponseSchema> clientResponse, 
												final long rate) {
		
		int minimumFragmentsOnRing = (maxInFlight*2)+10;
		final int connectionsInBits = 2;
		final int maxPartialResponses = 4;
		
		//TODO: the ack responses are hidden behind new requests and must take priority.
		//      one fix is to make this smaller??
		Pipe<MQTTClientToServerSchema> clientToServer = MQTTClientToServerSchema.instance.newPipe(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields); //from the application 
		Pipe<MQTTClientToServerSchemaAck> clientToServerAck = MQTTClientToServerSchemaAck.instance.newPipe(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields); //from the application 
		
		
		final Pipe<MQTTServerToClientSchema> serverToClient = MQTTServerToClientSchema.instance.newPipe(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields); //from the response
		
		Pipe<MQTTIdRangeSchema> idGenNew = MQTTIdRangeSchema.instance.newPipe(4,0);
		Pipe<MQTTIdRangeSchema> idGenOld = MQTTIdRangeSchema.instance.newPipe(4,0);
		
		IdGenStage idGenStage = new IdGenStage(gm, idGenOld, idGenNew);		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, idGenStage);
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, isTLS);
		
		ccm.setStageNotaProcessor(new PronghornStageProcessor() {
			//force all these to be hidden as part of the monitoring system
			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, stage);
			}
		});
		
		ClientResponseParserFactory factory = new ClientResponseParserFactory() {			
			@Override
			public void buildParser(GraphManager gm, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] fromBroker,
									Pipe<ReleaseSchema> ackReleaseForResponseParser) {
	
				//parse the socket data, determine the message and write it to the out pipe
				MQTTClientResponseStage responseStage = new MQTTClientResponseStage(gm, ccm, fromBroker, 
						ackReleaseForResponseParser, serverToClient); //releases Ids.	
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, responseStage);
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
		PersistedBlobStage persistedStage = new PersistedBlobStage(gm, persistancePipe, persistanceLoadPipe, multi, maxValueBits, rootFolder );
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, persistedStage);
		
		int independentClients = 1; 
		
		Pipe<NetPayloadSchema>[] toBroker = Pipe.buildPipes(independentClients, NetPayloadSchema.instance.newPipeConfig(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields));
		
		//take input request and write the bytes to the broker socket
		int uniqueId = 1; //Different for each client instance. so ccm and toBroker can be shared across all clients.
		MQTTClientToServerEncodeStage encodeStage = new MQTTClientToServerEncodeStage(gm, ccm, maxInFlight, uniqueId, clientToServer, clientToServerAck, persistancePipe, persistanceLoadPipe, toBroker);
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, encodeStage);
		
		
		//debug to watch the raw packes back from the server.
		Pipe<MQTTServerToClientSchema> serverToClient2 = JSONTap.attach(false, gm, serverToClient, System.out);
		
		
		MQTTClient mqttClient = new MQTTClient(gm, clientRequest, idGenNew, serverToClient2, clientResponse, idGenOld, clientToServer, clientToServerAck);
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, mqttClient);
		
		NetGraphBuilder.buildSimpleClientGraph(gm, ccm, factory, toBroker);
	}
	
	
	
	
}
