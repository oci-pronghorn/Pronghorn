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
import com.ociweb.pronghorn.network.schema.MQTTIdRangeControllerSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.file.FileGraphBuilder;
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
		
		buildMQTTClientGraph(gm, isTLS, maxInFlight, maximumLenghOfVariableLengthFields, 
							clientRequest, clientResponse, rate, (byte)2, (short)4);
				
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
												final long rate, byte connectionsInBits, 
												short maxPartialResponses) {
		
		if (isTLS && maximumLenghOfVariableLengthFields<(1<<15)) {
			maximumLenghOfVariableLengthFields = (1<<15);//ensure we have enough room for TLS work.
		}
		
		byte maxValueBits = (byte)Math.ceil(Math.log(maximumLenghOfVariableLengthFields)/Math.log(2));

		final Pipe<MQTTClientToServerSchema> clientToServer = MQTTClientToServerSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields); //from the application 
		final Pipe<MQTTClientToServerSchemaAck> clientToServerAck = MQTTClientToServerSchemaAck.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields); //from the application 
		final Pipe<MQTTServerToClientSchema> serverToClient = MQTTServerToClientSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields); //from the response
		
		Pipe<MQTTIdRangeSchema> idGenNew = MQTTIdRangeSchema.instance.newPipe(4,0);
		Pipe<MQTTIdRangeSchema> idGenOld = MQTTIdRangeSchema.instance.newPipe(16,0); //bigger because we need to unify fragments
		Pipe<MQTTIdRangeControllerSchema> idRangeControl = MQTTIdRangeControllerSchema.instance.newPipe(maxInFlight+2, 0);
		
		IdGenStage idGenStage = new IdGenStage(gm, idGenOld, idRangeControl, idGenNew);		
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

		File rootFolder = null;
		try {
			rootFolder = new File(Files.createTempDirectory("mqttClientData").toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		
//		//TODO: pass these 3 pipes ino the PersistedBlobStage
//		Pipe<FileManagerSchema> control = FileManagerSchema.instance.newPipe(10, 1000);
//		Pipe<RawDataSchema> inPipe = RawDataSchema.instance.newPipe(10, 1000);
//		Pipe<RawDataSchema> outPipe = RawDataSchema.instance.newPipe(10, 1000);		
//		FileBlobReadWriteStage fileReadWrite = new FileBlobReadWriteStage(gm, control, inPipe, outPipe, "filename");
		
		byte multi = 4;//x time the pipe size
		

		
//		Pipe<PersistedBlobLoadSchema> persistanceLoadPipe = PersistedBlobLoadSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
//		PersistedUnsafeBlobStage persistedStage = new PersistedUnsafeBlobStage(gm, 
//				                     persistancePipe, persistanceLoadPipe, 
//				                     multi, maxValueBits, rootFolder );
//		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, persistedStage);
		
		
		short inFlightCount = (short)maxInFlight;
		Pipe<PersistedBlobLoadSchema> persistanceLoadPipe = FileGraphBuilder.buildSequentialReplayer(
				gm, persistancePipe, multi, maxValueBits, inFlightCount,
				maximumLenghOfVariableLengthFields, rootFolder, null, rate);
		
		
		int independentClients = 1; 
		
		Pipe<NetPayloadSchema>[] toBroker = Pipe.buildPipes(independentClients, 
				                        NetPayloadSchema.instance.newPipeConfig(
				                        		maxInFlight+8,//extra space 
				                        		maximumLenghOfVariableLengthFields));
		
		//take input request and write the bytes to the broker socket
		int uniqueId = 1; //Different for each client instance. so ccm and toBroker can be shared across all clients.
		MQTTClientToServerEncodeStage encodeStage = new MQTTClientToServerEncodeStage(gm, 
				                                        ccm, maxInFlight, uniqueId, clientToServer, 
				                                        clientToServerAck, persistancePipe, persistanceLoadPipe, 
				                                        idRangeControl, toBroker);
		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, encodeStage);
		
		
		//debug to watch the raw packes back from the server.
		Pipe<MQTTServerToClientSchema> serverToClient2 = JSONTap.attach(false, gm, serverToClient, System.out);
		
		
		MQTTClientStage mqttClient = new MQTTClientStage(gm,
				clientRequest, idGenNew, serverToClient2, clientResponse, idGenOld, clientToServer, clientToServerAck);
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, mqttClient);
		
		int clientWriters = 1;				
		int responseUnwrapCount = 2;
		int clientWrapperCount = 2;
		int responseQueue = maxInFlight;
		int responseSize = maximumLenghOfVariableLengthFields;
		int releaseCount = maxInFlight;
		int netResponseCount = maxInFlight;
		int netResponseBlob = maximumLenghOfVariableLengthFields;
		int writeBufferMultiplier = 32;//bumped up to speed client writing
				
		NetGraphBuilder.buildClientGraph(gm, ccm, responseQueue, responseSize, toBroker,
				         responseUnwrapCount, clientWrapperCount,
				         clientWriters, releaseCount, netResponseCount,
				         netResponseBlob, factory, writeBufferMultiplier);
	}
	
	
	
	
}
