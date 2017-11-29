package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.schema.*;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.file.FileGraphBuilder;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.JSONTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;

public class MQTTClientGraphBuilder {

	public static final String BACKGROUND_COLOR = "darkolivegreen2";
	private static final Logger logger = LoggerFactory.getLogger(MQTTClientGraphBuilder.class);

	public static Pipe<MQTTClientResponseSchema> buildMQTTClientGraph(GraphManager gm, 
			Pipe<MQTTClientRequestSchema> clientRequest,
			String user, String pass, TLSCertificates tlsCertificates) {

		if (tlsCertificates == null) {
			tlsCertificates = TLSCertificates.defaultCerts;
		}
		int maxInFlight = 10;
		int maximumLenghOfVariableLengthFields = 4096;
		int rate = 1_200;

		Pipe<MQTTClientResponseSchema> clientResponse = MQTTClientResponseSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
				
        //we are not defining he other side of the request and response....
		
		short maxPartialResponses = (short)1;
		buildMQTTClientGraph(gm, tlsCertificates, maxInFlight, maximumLenghOfVariableLengthFields,
							clientRequest, clientResponse, rate, 
							(byte)2, maxPartialResponses, user, pass);
				
		return clientResponse;
	}

	
	public static void buildMQTTClientGraph(GraphManager gm, TLSCertificates tlsCertificates, int maxInFlight,
											int maximumLenghOfVariableLengthFields,
											Pipe<MQTTClientRequestSchema> clientRequest,
											Pipe<MQTTClientResponseSchema> clientResponse,
											final long rate, byte connectionsInBits,
											short maxPartialResponses,
											String username, String password) {
		
		byte[] cypherBlock = null; //default value if no user/pass is provided		
		if (username!=null && password!=null) {
			assert(username.length()>0);
			assert(password.length()>0);
			
			cypherBlock = new byte[16];
			SecureRandom sr = new SecureRandom((username+":"+password).getBytes());
			sr.nextBytes(cypherBlock);

			if (tlsCertificates == null) {
				tlsCertificates = TLSCertificates.defaultCerts;
			}
		} else {
			logger.info("Warning: MQTT persistance to disk is not encrypted because no user/pass provided.");
		}
		
		
		
		if (tlsCertificates != null && maximumLenghOfVariableLengthFields<(1<<15)) {
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
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate*10, idGenStage);
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, BACKGROUND_COLOR, idGenStage);
		
		ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, maxPartialResponses, tlsCertificates);
		
		ccm.setStageNotaProcessor(new PronghornStageProcessor() {
			//force all these to be hidden as part of the monitoring system
			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, stage);
				GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, BACKGROUND_COLOR, stage);
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
				GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, BACKGROUND_COLOR, responseStage);
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
		
		byte multiplierBeforeCompact = 4;//x time the pipe size
		

		
//		Pipe<PersistedBlobLoadSchema> persistanceLoadPipe = PersistedBlobLoadSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
//		PersistedUnsafeBlobStage persistedStage = new PersistedUnsafeBlobStage(gm, 
//				                     persistancePipe, persistanceLoadPipe, 
//				                     multi, maxValueBits, rootFolder );
//		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, persistedStage);
//		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "darkolivegreen2", persistedStage);
		
		short inFlightCount = (short)maxInFlight;
		Pipe<PersistedBlobLoadSchema> persistanceLoadPipe = 
				FileGraphBuilder.buildSequentialReplayer(
				gm, persistancePipe, multiplierBeforeCompact, maxValueBits, inFlightCount,
				maximumLenghOfVariableLengthFields, rootFolder, cypherBlock,
				rate*10, BACKGROUND_COLOR);
		
		
		int independentClients = 1; 
		
		Pipe<NetPayloadSchema>[] toBroker = Pipe.buildPipes(independentClients, 
				                        NetPayloadSchema.instance.newPipeConfig(
				                        		(2*maxInFlight+8),//extra space Now added extra for the replays!
				                        		maximumLenghOfVariableLengthFields));
		
		//take input request and write the bytes to the broker socket
		int uniqueId = 1; //Different for each client instance. so ccm and toBroker can be shared across all clients.
		MQTTClientToServerEncodeStage encodeStage = new MQTTClientToServerEncodeStage(gm, 
				                                        ccm, maxInFlight, uniqueId, clientToServer, 
				                                        clientToServerAck, persistancePipe, persistanceLoadPipe, 
				                                        idRangeControl, toBroker);
		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, encodeStage);
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, BACKGROUND_COLOR, encodeStage);

		
		//debug to watch the raw packes back from the server.
		Pipe<MQTTServerToClientSchema> serverToClient2 = JSONTap.attach(false, gm, serverToClient, System.out);

        ////////////////////
		//////////////in progress pass both pipes into MQTTClientStage
		//////////////////
//		Pipe<PersistedBlobStoreSchema> qos2toWrite = 
//				PersistedBlobStoreSchema.instance
//				  .newPipe(inFlightCount, 4);
//		Pipe<PersistedBlobLoadSchema>  qos2fromRead = 
//				FileGraphBuilder.buildSequentialReplayer(
//					gm, qos2toWrite,
//					multiplierBeforeCompact,  
//					(byte)16, //save the packedIds, max 1<<16 
//					inFlightCount, 
//					4, 
//					rootFolder, cypherBlock, 
//					rate);
		///////////////////////
		
		
		
		MQTTClientStage mqttClient = new MQTTClientStage(gm,
				clientRequest, idGenNew, serverToClient2,
				clientResponse, idGenOld, clientToServer,
				clientToServerAck);
		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, mqttClient);
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, BACKGROUND_COLOR, mqttClient);
		
		int clientWriters = 1;				
		int responseUnwrapCount = 2;
		int clientWrapperCount = 2;
		int responseQueue = maxInFlight;
		int releaseCount = maxInFlight;
		int netResponseCount = maxInFlight;
		int netResponseBlob = maximumLenghOfVariableLengthFields;
		int writeBufferMultiplier = 32;//bumped up to speed client writing
				
		NetGraphBuilder.buildClientGraph(gm, ccm, responseQueue, toBroker, responseUnwrapCount,
				         clientWrapperCount, clientWriters,
				         releaseCount, netResponseCount, factory,
				         writeBufferMultiplier);
	}
	
	
	
	
}
