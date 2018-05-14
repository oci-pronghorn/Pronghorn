package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class HTTPClientUtil {

	private static final byte[] WHITE_BYTES = " ".getBytes();
	private static final byte[] SLASH_BYTES = " /".getBytes();
	private static final byte[] POST_BYTES = "POST".getBytes();

	public static boolean showRequest = false;
	
	public static boolean showSentPayload = false;
	private final static Logger logger = LoggerFactory.getLogger(HTTPClientUtil.class);

	public static void cleanCloseConnection(Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection connectionToKill, Pipe<NetPayloadSchema> pipe) {
	
		if (null!=requestPipe) {
			//required to move the position forward.
			Pipe.takeInt(requestPipe);//session
			Pipe.takeInt(requestPipe);//port
			Pipe.takeInt(requestPipe);//host
			Pipe.takeInt(requestPipe);//host
		}
		
		//do not close that will be done by last stage
		//must be done first before we send the message
		connectionToKill.beginDisconnect();

		Pipe.presumeRoomForWrite(pipe);
		int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_DISCONNECT_203);
		Pipe.addLongValue(connectionToKill.getId(), pipe);//   NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, connectionToKill.getId());
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		
	}

	public static void processGetSlow(long now, Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection clientConnection,
			Pipe<NetPayloadSchema> outputPipe, int stageId) {
		
		clientConnection.setLastUsedTime(now);		        	
		//logger.info("sent get request down pipe {} ",outIdx);
		
		clientConnection.incRequestsSent();//count of messages can only be done here.
	
		Pipe.presumeRoomForWrite(outputPipe);
		
		   	int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
		   	
		   	Pipe.addLongValue(clientConnection.id, outputPipe); //, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
		   	Pipe.addLongValue(System.currentTimeMillis(), outputPipe);
		   	Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
		 	
		 	
			DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
			DataOutputBlobWriter.openField(activeWriter);
			
			DataOutputBlobWriter.encodeAsUTF8(activeWriter,"GET");
			
			int routeId = Pipe.takeInt(requestPipe); //	ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_DESTINATION_11
			
			int userId = Pipe.takeInt(requestPipe);			
			assert(clientConnection.sessionId == userId);
			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
        	assert(routeId>=0);
        	clientConnection.recordDestinationRouteId(routeId);
			
			int port   = Pipe.takeInt(requestPipe);
			assert(clientConnection.port == port);
			
			int hostMeta = Pipe.takeRingByteMetaData(requestPipe);
			int hostLen  = Pipe.takeRingByteLen(requestPipe);
			int hostPos = Pipe.bytePosition(hostMeta, requestPipe, hostLen);
			
		  	int meta = Pipe.takeRingByteMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3
			int len  = Pipe.takeRingByteLen(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
			int headersMeta = Pipe.takeRingByteMetaData(requestPipe); // HEADER
			int headersLen  = Pipe.takeRingByteMetaData(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
		    
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
	
			if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
				DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
			} else {
				DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
			}
			
			//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
			Pipe.readBytes(requestPipe, activeWriter, meta, len);//, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, activeWriter);
	
			final byte[] hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);//, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
			final int hostMask    = Pipe.blobMask(requestPipe);	
			
			
			HeaderUtil.writeHeaderBeginning(hostBack, hostPos, hostLen, hostMask, activeWriter);
			
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

	public static void processPostSlow(long now, Pipe<ClientHTTPRequestSchema> requestPipe, 
			ClientConnection clientConnection,
			Pipe<NetPayloadSchema> outputPipe, int stageId) {
		
		clientConnection.setLastUsedTime(now);
		clientConnection.incRequestsSent();//count of messages can only be done here.
	
		Pipe.presumeRoomForWrite(outputPipe);
			
			int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
			
			Pipe.addLongValue(clientConnection.id, outputPipe); //NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
			Pipe.addLongValue(System.currentTimeMillis(), outputPipe);
			Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
			
			DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
			DataOutputBlobWriter.openField(activeWriter);
					   
			activeWriter.write(POST_BYTES);
			
			int routeId = Pipe.takeInt(requestPipe);
			int userId = Pipe.takeInt(requestPipe);
			assert(clientConnection.sessionId == userId);
			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
        	assert(routeId>=0);
        	clientConnection.recordDestinationRouteId(routeId);
			
			int port   = Pipe.takeInt(requestPipe);
			assert(clientConnection.port == port);
			
			int hostMeta = Pipe.takeRingByteMetaData(requestPipe);
			int hostLen  = Pipe.takeRingByteLen(requestPipe);
			int hostPos = Pipe.bytePosition(hostMeta, requestPipe, hostLen);
			
		  	int meta = Pipe.takeRingByteMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3
			int len  = Pipe.takeRingByteLen(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
	
			if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
				activeWriter.write(SLASH_BYTES);
			} else {
				activeWriter.write(WHITE_BYTES);
			}
			
			//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
			Pipe.readBytes(requestPipe, activeWriter, meta, len);
			
			int headersMeta = Pipe.takeRingByteMetaData(requestPipe); // HEADER 7
			int headersLen  = Pipe.takeRingByteLen(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
			
			
			int payloadMeta = Pipe.takeRingByteMetaData(requestPipe); //MSG_HTTPPOST_101_FIELD_PAYLOAD_5
			int payloadLen  = Pipe.takeRingByteLen(requestPipe);
			
			
			//For chunked must pass in -1
	
			//TODO: this field can no be any loger than 4G so we cant post anything larger than that
			//TODO: we also need support for chunking which will need multiple mesage fragments
			//TODO: need new message type for chunking/streaming post
			
			final byte[] hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);//, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
			final int backingMask    = Pipe.blobMask(requestPipe);	
			
			HeaderUtil.writeHeaderBeginning(hostBack, hostPos, hostLen, backingMask, activeWriter);
			
			HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
			//callers custom headers are written where.
			activeWriter.write(Pipe.byteBackingArray(headersMeta, requestPipe), headersPos, headersLen, backingMask);	
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

	public static void publishGetFast(Pipe<ClientHTTPRequestSchema> requestPipe, ClientConnection clientConnection,
			Pipe<NetPayloadSchema> outputPipe, long now, int stageId) {
		
		clientConnection.incRequestsSent();//count of messages can only be done here, AFTER requestPipeLineIdx
		
		//long pos = Pipe.workingHeadPosition(outputPipe);
		
		final int pSize = Pipe.addMsgIdx(outputPipe, NetPayloadSchema.MSG_PLAIN_210); 	
		  
		Pipe.addLongValue(clientConnection.id, outputPipe); //, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
		Pipe.addLongValue(now, outputPipe);
		Pipe.addLongValue(0, outputPipe); // NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
		
		int routeId = Pipe.takeInt(requestPipe);
		int userId = Pipe.takeInt(requestPipe);
		int port   = Pipe.takeInt(requestPipe);
		int hostMeta = Pipe.takeRingByteMetaData(requestPipe);
		int hostLen  = Pipe.takeRingByteLen(requestPipe);
		int hostPos = Pipe.bytePosition(hostMeta, requestPipe, hostLen);
		long connId = Pipe.takeLong(requestPipe);
		
    	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
    	assert(routeId>=0);
    	clientConnection.recordDestinationRouteId(routeId);
		
		int meta = Pipe.takeRingByteMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3
		int len  = Pipe.takeRingByteLen(requestPipe);
		boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[Pipe.bytePosition(meta, requestPipe, len)&Pipe.blobMask(requestPipe)]);  
		
		int headersMeta = Pipe.takeRingByteMetaData(requestPipe); // HEADER
		int headersLen  = Pipe.takeRingByteMetaData(requestPipe);
		int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
		
		DataOutputBlobWriter<NetPayloadSchema> activeWriter = Pipe.outputStream(outputPipe);
		DataOutputBlobWriter.openField(activeWriter);
	
		if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
			DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.GET_BYTES_SPACE_SLASH, 0, HTTPClientRequestStage.GET_BYTES_SPACE_SLASH.length);
	
		} else {
			DataOutputBlobWriter.write(activeWriter,HTTPClientRequestStage.GET_BYTES_SPACE, 0, HTTPClientRequestStage.GET_BYTES_SPACE.length);
		}
		
		//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
		Pipe.readBytes(requestPipe, activeWriter, meta, len);//, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3, activeWriter);
		
		HeaderUtil.writeHeaderBeginning(Pipe.byteBackingArray(hostMeta, requestPipe), hostPos, hostLen, Pipe.blobMask(requestPipe), activeWriter);
		HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
		Pipe.readBytes(requestPipe, activeWriter, headersMeta, headersLen);
		
		HeaderUtil.writeHeaderEnding(activeWriter, true, (long) 0);  
		
		int msgLen = DataOutputBlobWriter.closeLowLevelField(activeWriter);//, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
		
		Pipe.confirmLowLevelWrite(outputPipe,pSize);
		Pipe.publishWrites(outputPipe);

	}

	public static void processPostFast(long now, Pipe<ClientHTTPRequestSchema> requestPipe, 
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
					   
			activeWriter.writeByte('P');
			activeWriter.writeByte('O');
			activeWriter.writeByte('S');
			activeWriter.writeByte('T');
			
			int routeId = Pipe.takeInt(requestPipe); //destination route
			int userId = Pipe.takeInt(requestPipe); //session id
			assert(clientConnection.sessionId == userId);
			
        	assert(clientConnection.singleUsage(stageId)) : "Only a single Stage may update the clientConnection.";
        	assert(routeId>=0);
        	clientConnection.recordDestinationRouteId(routeId);
			
        	/////////////////////
        	//port
			int port   = Pipe.takeInt(requestPipe); //port
			assert(clientConnection.port == port);
			
			///////////////////////
			///host
			int hostMeta = Pipe.takeRingByteMetaData(requestPipe); //host
			int hostLen  = Pipe.takeRingByteLen(requestPipe);
			int hostPos = Pipe.bytePosition(hostMeta, requestPipe, hostLen);

			////////////////
			//client connection
			long connectionId = Pipe.takeLong(requestPipe);//connectionId.
			
			///////////////////
			//path
		  	int meta = Pipe.takeRingByteMetaData(requestPipe); //ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3
			int len  = Pipe.takeRingByteLen(requestPipe);
		    int first = Pipe.bytePosition(meta, requestPipe, len);					                	
		
		    boolean prePendSlash = (0==len) || ('/' != Pipe.byteBackingArray(meta, requestPipe)[first&Pipe.blobMask(requestPipe)]);
	
			if (prePendSlash) {
				activeWriter.writeByte(' ');
				activeWriter.writeByte('/');				
			} else {				
				activeWriter.writeByte(' ');
			}
			Pipe.readBytes(requestPipe, activeWriter, meta, len);
			
			////////////////
			//header
			int headersMeta = Pipe.takeRingByteMetaData(requestPipe); // HEADER 7
			int headersLen  = Pipe.takeRingByteLen(requestPipe);
			int headersPos  = Pipe.bytePosition(headersMeta, requestPipe, headersLen);
			
			//////////////
			//payloads			
			int payloadMeta = Pipe.takeRingByteMetaData(requestPipe); //MSG_HTTPPOST_101_FIELD_PAYLOAD_5
			int payloadLen  = Pipe.takeRingByteLen(requestPipe);
						
			//For chunked must pass in -1
	
			//TODO: this field can no be any loger than 4G so we cant post anything larger than that
			//TODO: we also need support for chunking which will need multiple mesage fragments
			//TODO: need new message type for chunking/streaming post
			
			final byte[] hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);//, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
			final int backingMask    = Pipe.blobMask(requestPipe);	
			
			HeaderUtil.writeHeaderBeginning(hostBack, hostPos, hostLen, backingMask, activeWriter);
			
			HeaderUtil.writeHeaderMiddle(activeWriter, HTTPClientRequestStage.implementationVersion);
			//callers custom headers are written where.
			activeWriter.write(Pipe.byteBackingArray(headersMeta, requestPipe),
					           headersPos, headersLen, backingMask);	
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
}
