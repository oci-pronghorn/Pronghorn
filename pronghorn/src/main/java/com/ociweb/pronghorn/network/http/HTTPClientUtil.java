package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class HTTPClientUtil {

	private static final int OFFSET_TO_CON_ID = 0xF&(ClientHTTPRequestSchema.MSG_GET_200_FIELD_CONNECTIONID_20-1);
	private static final byte[] WHITE_BYTES = " ".getBytes();
	private static final byte[] SLASH_BYTES = " /".getBytes();

	public static boolean showRequest = false;
	
	public static boolean showSentPayload = false;
	private final static Logger logger = LoggerFactory.getLogger(HTTPClientUtil.class);

	public static void cleanCloseConnection(Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection connectionToKill, Pipe<NetPayloadSchema> pipe) {
		
		//must consume each field so we can move forward, this is required or the position will be off...
		//matches ClientHTTPRequestSchema.MSG_CLOSECONNECTION_104
		int session = Pipe.takeInt(requestPipe);
		int port    = Pipe.takeInt(requestPipe);
		int hostId  = Pipe.takeInt(requestPipe);
		long conId  = Pipe.takeLong(requestPipe);

		//do not close that will be done by last stage
		//must be done first before we send the message
		connectionToKill.beginDisconnect();

		Pipe.presumeRoomForWrite(pipe);
		int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_DISCONNECT_203);
		Pipe.addLongValue(connectionToKill.getId(), pipe);//   NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, connectionToKill.getId());
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		
	}

	private static void processSlow(HTTPVerbDefaults verb, Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection clientConnection, Pipe<NetPayloadSchema> outputPipe,
			                        long now, int stageId) {
		
		clientConnection.setLastUsedTime(now);		        	
		//logger.info("sent get request down pipe {} ",outIdx);
		
		clientConnection.incRequestsSent();//count of messages can only be done here.
	
		Pipe.presumeRoomForWrite(outputPipe);
		
		   	int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
		   	
		   	assert(clientConnection.id!=-1) : "must have connectionId";
		   	Pipe.addLongValue(clientConnection.id, outputPipe); //, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
		   	Pipe.addLongValue(System.currentTimeMillis(), outputPipe);
		   	Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
		 	
						
			
			int userId = Pipe.takeInt(requestPipe);			
			assert(clientConnection.sessionId == userId);			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
			
			int port   = Pipe.takeInt(requestPipe);
			assert(clientConnection.port == port);
			
			int hostId = Pipe.takeInt(requestPipe);
			
			long connId = Pipe.takeLong(requestPipe);
		  	assert(-1 == connId) : "Should have called fast method if we know the connId";
		  	
		  	int routeId = Pipe.takeInt(requestPipe); //	ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11
		  	assert(routeId>=0);
		  	
			
			int meta = Pipe.takeByteArrayMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3
			int len  = Pipe.takeByteArrayLength(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
			int headersMeta = Pipe.takeByteArrayMetaData(requestPipe); // HEADER
			int headersLen  = Pipe.takeByteArrayMetaData(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
		    
	 	
			DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
			DataOutputBlobWriter.openField(activeWriter);			
			activeWriter.write(verb.getKeyBytes());
			
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
			    
			if (prePendSlash) {
				DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.BYTES_SPACE_SLASH, 0, HTTPClientRequestStage.BYTES_SPACE_SLASH.length);		
			} else {
				DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.BYTES_SPACE, 0, HTTPClientRequestStage.BYTES_SPACE.length);
			}
			
			//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
			Pipe.readBytes(requestPipe, activeWriter, meta, len);//, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, activeWriter);
	
			byte[] host = ClientCoordinator.registeredDomainBytes(hostId);			
			HeaderUtil.writeHeaderBeginning(host, 0, host.length, port, activeWriter);
			
			HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
			activeWriter.write(Pipe.byteBackingArray(headersMeta, requestPipe), headersPos, headersLen, Pipe.blobMask(requestPipe));
									
			HeaderUtil.writeHeaderEnding(activeWriter, true, (long) 0);
		
							                	
			DataOutputBlobWriter.closeLowLevelField(activeWriter);//, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
	
			if (showRequest) {	
				logger.info("\n"+new String(activeWriter.toByteArray())+"\n");
			}
			
			Pipe.confirmLowLevelWrite(outputPipe,pSize);
			Pipe.publishWrites(outputPipe);
	}

	public static void processPayloadSlow(HTTPVerbDefaults verb, long now, Pipe<ClientHTTPRequestSchema> requestPipe, 
			ClientConnection clientConnection,
			Pipe<NetPayloadSchema> outputPipe, int stageId) {
		
		assert(-1 != clientConnection.id);
		
		clientConnection.setLastUsedTime(now);
		clientConnection.incRequestsSent();//count of messages can only be done here.

		Pipe.presumeRoomForWrite(outputPipe);
				
			int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
			
			Pipe.addLongValue(clientConnection.id, outputPipe); //NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
			Pipe.addLongValue(System.currentTimeMillis(), outputPipe);
			Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
			
			DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
			DataOutputBlobWriter.openField(activeWriter);
			
			activeWriter.write(verb.getKeyBytes());
			
			int userId = Pipe.takeInt(requestPipe);
			assert(clientConnection.sessionId == userId);
			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
			
			int port   = Pipe.takeInt(requestPipe);
			assert(clientConnection.port == port);
			
			///////////////////////
			///host
			int hostId = Pipe.takeInt(requestPipe);

			////////////////
			//client connection
			long connectionId = Pipe.takeLong(requestPipe);//connectionId.
			int routeId = Pipe.takeInt(requestPipe);
			assert(routeId>=0);
			
		  	int meta = Pipe.takeByteArrayMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3
			int len  = Pipe.takeByteArrayLength(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
	
			if (prePendSlash) { 
				activeWriter.write(SLASH_BYTES);
			} else {
				activeWriter.write(WHITE_BYTES);
			}
			
			//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
			Pipe.readBytes(requestPipe, activeWriter, meta, len);
			
			int headersMeta = Pipe.takeByteArrayMetaData(requestPipe); // HEADER 7
			int headersLen  = Pipe.takeByteArrayLength(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
			int headerMask  = Pipe.blobMask(requestPipe);
					
			
			int payloadMeta = Pipe.takeByteArrayMetaData(requestPipe); //MSG_HTTPPOST_101_FIELD_PAYLOAD_5
			int payloadLen  = Pipe.takeByteArrayLength(requestPipe);
			
			byte[] hostBytes = ClientCoordinator.domainLookupArrayBytes[hostId];			
			HeaderUtil.writeHeaderBeginning(hostBytes, 0, hostBytes.length, headerMask, port, activeWriter);
			
			HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
			//callers custom headers are written where.
			activeWriter.write(Pipe.byteBackingArray(headersMeta, requestPipe), headersPos, headersLen, headerMask);	
			boolean keepOpen = true;
			
			HeaderUtil.writeHeaderEnding(activeWriter, keepOpen, (long) payloadLen);
			
			Pipe.readBytes(requestPipe, activeWriter, payloadMeta, payloadLen); //MSG_HTTPPOST_101_FIELD_PAYLOAD_5
			
			int postLen = DataOutputBlobWriter.closeLowLevelField(activeWriter);//, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
	
			if (showSentPayload) {
				logger.info("\n"+new String(activeWriter.toByteArray())+"\n");
			}
			
			Pipe.confirmLowLevelWrite(outputPipe,pSize);
			Pipe.publishWrites(outputPipe);
	
	}

	private static void processFast(HTTPVerbDefaults verb, Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection clientConnection,
								  Pipe<NetPayloadSchema> outputPipe, long now, int stageId) {
		
		clientConnection.incRequestsSent();//count of messages can only be done here, AFTER requestPipeLineIdx
		
		//long pos = Pipe.workingHeadPosition(outputPipe);
		
		final int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210); 	
		  
		Pipe.addLongValue(clientConnection.id, outputPipe); //, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
		Pipe.addLongValue(now, outputPipe);
		Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
		
		int userId = Pipe.takeInt(requestPipe);
		int port   = Pipe.takeInt(requestPipe);
		int hostId = Pipe.takeInt(requestPipe);
		
		long connId = Pipe.takeLong(requestPipe);
		int routeId = Pipe.takeInt(requestPipe);
		
    	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
    	assert(routeId>=0);
    			
		int meta = Pipe.takeByteArrayMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3
		int len  = Pipe.takeByteArrayLength(requestPipe);
		boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[Pipe.bytePosition(meta, requestPipe, len)&Pipe.blobMask(requestPipe)]);  
		
		int headersMeta = Pipe.takeByteArrayMetaData(requestPipe); // HEADER
		int headersLen  = Pipe.takeByteArrayMetaData(requestPipe);
		int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
		
		DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
		DataOutputBlobWriter.openField(activeWriter);

		activeWriter.write(verb.getKeyBytes());
		
		if (prePendSlash) {
			DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.BYTES_SPACE_SLASH, 0, HTTPClientRequestStage.BYTES_SPACE_SLASH.length);		
		} else {
			DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.BYTES_SPACE, 0, HTTPClientRequestStage.BYTES_SPACE.length);
		}
		
		//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
		Pipe.readBytes(requestPipe, activeWriter, meta, len);//, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3, activeWriter);
				
		byte[] domain = ClientCoordinator.registeredDomainBytes(hostId);
		
		HeaderUtil.writeHeaderBeginning(
				domain, 0, domain.length, 
				port, activeWriter);
		
		HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
		Pipe.readBytes(requestPipe, activeWriter, headersMeta, headersLen);
		
		HeaderUtil.writeHeaderEnding(activeWriter, true, (long) 0);  
		
		int msgLen = DataOutputBlobWriter.closeLowLevelField(activeWriter);//, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
		
		if ( showRequest) {
			logger.info("\n"+new String(activeWriter.toByteArray())+"\n");
		}
		
		Pipe.confirmLowLevelWrite(outputPipe,pSize);
		Pipe.publishWrites(outputPipe);

	}

	public static void processPayloadFast(HTTPVerbDefaults verb, long now, Pipe<ClientHTTPRequestSchema> requestPipe, 
										ClientConnection clientConnection,
										Pipe<NetPayloadSchema> outputPipe,
										int stageId) {
		
		clientConnection.setLastUsedTime(now);
		clientConnection.incRequestsSent();//count of messages can only be done here.
	
			Pipe.presumeRoomForWrite(outputPipe);
			
			int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
			
			Pipe.addLongValue(clientConnection.id, outputPipe);
			Pipe.addLongValue(System.currentTimeMillis(), outputPipe);
			Pipe.addLongValue(0L, outputPipe); 
			
			DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
			DataOutputBlobWriter.openField(activeWriter);
					   
			activeWriter.write(verb.getKeyBytes());
			
			int userId = Pipe.takeInt(requestPipe); //session id
			assert(clientConnection.sessionId == userId);
			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
			
        	/////////////////////
        	//port
			int port   = Pipe.takeInt(requestPipe); //port
			assert(clientConnection.port == port);
			
			///////////////////////
			///host
			int hostId = Pipe.takeInt(requestPipe);

			////////////////
			//client connection
			long connectionId = Pipe.takeLong(requestPipe);//connectionId.
			
			int routeId = Pipe.takeInt(requestPipe); //destination route
			assert(routeId>=0);
			///////////////////
			//path
		  	int meta = Pipe.takeByteArrayMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3
			int len  = Pipe.takeByteArrayLength(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
	
			if (prePendSlash) { 
				activeWriter.write(SLASH_BYTES);
			} else {
				activeWriter.write(WHITE_BYTES);
			}
			Pipe.readBytes(requestPipe, activeWriter, meta, len);
			
			////////////////
			//header
			int headersMeta = Pipe.takeByteArrayMetaData(requestPipe); // HEADER 7
			int headersLen  = Pipe.takeByteArrayLength(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
			int headerMask  = Pipe.blobMask(requestPipe);
			
			//////////////
			//payloads			
			int payloadMeta = Pipe.takeByteArrayMetaData(requestPipe); //MSG_HTTPPOST_101_FIELD_PAYLOAD_5
			int payloadLen  = Pipe.takeByteArrayLength(requestPipe);
							
			byte[] hostBytes = ClientCoordinator.domainLookupArrayBytes[hostId];			
			HeaderUtil.writeHeaderBeginning(hostBytes, 0, hostBytes.length, headerMask, port, activeWriter);
			
			HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
			//callers custom headers are written where.
			activeWriter.write(Pipe.byteBackingArray(headersMeta, requestPipe),
					           headersPos, headersLen, headerMask);	
			boolean keepOpen = true;
			
			HeaderUtil.writeHeaderEnding(activeWriter, keepOpen, (long) payloadLen);
			
			Pipe.readBytes(requestPipe, activeWriter, payloadMeta, payloadLen);
			
			int postLen = DataOutputBlobWriter.closeLowLevelField(activeWriter);//, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
	
			if (showSentPayload) {
				logger.info("\n"+new String(activeWriter.toByteArray())+"\n");
			}
			
			Pipe.confirmLowLevelWrite(outputPipe,pSize);
			Pipe.publishWrites(outputPipe);
	
	}

	public static void publish(HTTPVerbDefaults verb, Pipe<ClientHTTPRequestSchema> requestPipe,
								ClientConnection activeConnection, Pipe<NetPayloadSchema> pipe, long now, int stageId) {
				
		activeConnection.recordNowInFlight();
		
		long conId = Pipe.peekLong(requestPipe, OFFSET_TO_CON_ID);
		if (conId==-1) {
			processSlow(verb, requestPipe, activeConnection, pipe, now, stageId);
		} else {
			assert(conId>=0);
			processFast(verb, requestPipe, activeConnection, pipe, now, stageId);
		}
		
	}

	public static void publishWithPayload(HTTPVerbDefaults verb, Pipe<ClientHTTPRequestSchema> requestPipe,
										 ClientConnection activeConnection, Pipe<NetPayloadSchema> pipe, long now, int stageId) {
		
		activeConnection.recordNowInFlight();
		
		long conId = Pipe.peekLong(requestPipe, OFFSET_TO_CON_ID);
		if (conId==-1) {
			processPayloadSlow(verb, now, requestPipe, activeConnection, pipe, stageId);		
		} else {
			assert(conId>=0);
			processPayloadFast(verb, now, requestPipe, activeConnection, pipe, stageId);		
		}		
	}
}
