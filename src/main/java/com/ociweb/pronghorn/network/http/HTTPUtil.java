package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.util.TrieParser;

public class HTTPUtil {

	private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);
    protected static final byte[] ZERO = new byte[] {'0'};
	
    private final static int CHUNK_SIZE = 1;
    private final static int CHUNK_SIZE_WITH_EXTENSION = 2;
    
	static final TrieParser chunkMap = buildChunkMap();
    
	public static void publishStatus(long channelId, int sequence,
	            					int status,
	            					Pipe<ServerResponseSchema> localOutput) {

		int channelIdHigh = (int)(channelId>>32); 
		int channelIdLow = (int)channelId;		
		
		publishStatus(sequence, status, channelIdHigh, channelIdLow, localOutput);
	}

	public static void publishStatus(int sequence, int status, int channelIdHigh, int channelIdLow,
			Pipe<ServerResponseSchema> localOutput) {
		int contentLength = 0;
		byte[] contentBacking = null;
		int contentPosition = 0;
		int contentMask = Integer.MAX_VALUE;
		
		simplePublish(ServerCoordinator.END_RESPONSE_MASK, sequence, status, localOutput, channelIdHigh, channelIdLow, null,
				      contentLength, contentBacking, contentPosition, contentMask);
	}
    
	private static TrieParser buildChunkMap() {
		  TrieParser chunkMap = new TrieParser(128,true);
	      chunkMap.setUTF8Value("%U\r\n", CHUNK_SIZE); //hex parser of U% does not require leading 0x
	      chunkMap.setUTF8Value("%U;%b\r\n", CHUNK_SIZE_WITH_EXTENSION);
	      return chunkMap;
	}

	public static void simplePublish(int requestContext, int sequence, int status,
			Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow, byte[] typeBytes,
			int contentLength, byte[] contentBacking, int contentPosition, int contentMask) {
		assert(contentLength>=0) : "This method does not support chunking";
		
		byte [] contentLocationBacking = null;
		int contentLocationBytesPos = 0;
		int contentLocationBytesLen = 0;
		int contentLocationBytesMask = 0;
		
		int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
	
	    Pipe.addIntValue(channelIdHigh, localOutput);
	    Pipe.addIntValue(channelIdLow, localOutput);
	    Pipe.addIntValue(sequence, localOutput);
	    
	    DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.outputStream(localOutput);        
	    writer.openField();
		boolean chunked = false;
		boolean server = false;
		
		AbstractRestStage.writeHeader(  HTTPRevisionDefaults.HTTP_1_1.getBytes(),
				                        status, requestContext, null, 
				                        typeBytes, 
				                        contentLength, 
						    		    chunked, server,
						    		    contentLocationBacking, contentLocationBytesPos,contentLocationBytesLen,contentLocationBytesMask,
						    		    writer, 1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
	    if (contentLength>0) {
			writer.write(contentBacking, contentPosition, contentLength, contentMask);
	    }
		writer.closeLowLevelField();          
	
	    Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
	    
	    Pipe.confirmLowLevelWrite(localOutput, headerSize);
	    Pipe.publishWrites(localOutput);
	    
	    //logger.info("published error {} ",status);
	}

	public final static String expectedGet = "GET /groovySum.json HTTP/1.1\r\n"+
	 "Host: 127.0.0.1\r\n"+
	 "Connection: keep-alive\r\n"+
	 "\r\n";
	public final static String expectedOK = "HTTP/1.1 200 OK\r\n"+
	"Content-Type: application/json\r\n"+
	"Content-Length: 30\r\n"+
	"Connection: open\r\n"+
	"\r\n"+
	"{\"x\":9,\"y\":17,\"groovySum\":26}\n";

}
