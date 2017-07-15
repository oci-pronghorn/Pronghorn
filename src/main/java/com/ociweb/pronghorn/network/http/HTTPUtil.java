package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ServerCoordinator;
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
    
	public static void publishError(int sequence, int status,
	            Pipe<ServerResponseSchema> localOutput, long channelId, 
	            HTTPSpecification<?,?,?,?> httpSpec, 
	            int revision) {
		publishError(sequence,status,localOutput,channelId,httpSpec,revision,-1);
	}
    
	public static void publishError(int sequence, int status,
            Pipe<ServerResponseSchema> localOutput, long channelId, 
            HTTPSpecification<?,?,?,?> httpSpec, 
            int revision, int contentType) {
				
		int channelIdHigh = (int)(channelId>>32); 
        int channelIdLow = (int)channelId;
        
        //this method will close the connection as soon as the response is sent.
		publishError(ServerCoordinator.END_RESPONSE_MASK | ServerCoordinator.CLOSE_CONNECTION_MASK,
				      sequence, status, localOutput, 
					  channelIdHigh,channelIdLow,httpSpec,revision,contentType);
		
	}
    
	private static TrieParser buildChunkMap() {
		  TrieParser chunkMap = new TrieParser(128,true);
	      chunkMap.setUTF8Value("%U\r\n", CHUNK_SIZE); //hex parser of U% does not require leading 0x
	      chunkMap.setUTF8Value("%U;%b\r\n", CHUNK_SIZE_WITH_EXTENSION);
	      return chunkMap;
	}

	public static void publishError(int requestContext, int sequence, int status,
	                            Pipe<ServerResponseSchema> localOutput, int channelIdHigh, 
	                            int channelIdLow, HTTPSpecification<?,?,?,?> httpSpec, 
	                            int revision, int contentType) {
	    
	    int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
	
	    Pipe.addIntValue(channelIdHigh, localOutput);
	    Pipe.addIntValue(channelIdLow, localOutput);
	    Pipe.addIntValue(sequence, localOutput);
	    
	    DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.outputStream(localOutput);        
	    writer.openField();
	    byte[] revBytes = httpSpec.revisions[revision].getBytes();
		AbstractRestStage.writeHeader( revBytes, status, requestContext, null, 
				                        contentType<0 ? null :httpSpec.contentTypes[contentType].getBytes(), 
						    		    0, false, false, null, 0,0,0,
						    		    writer, 1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
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
