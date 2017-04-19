package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class HTTPErrorUtil {

	private static final Logger logger = LoggerFactory.getLogger(HTTPErrorUtil.class);
    protected static final byte[] ZERO = new byte[] {'0'};
	
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
	    AbstractRestStage.writeHeader(httpSpec.revisions[revision].getBytes(), status, requestContext, null, contentType<0 ? null :httpSpec.contentTypes[contentType].getBytes(), 
	    		    ZERO, 0, 1, 1, false, null, 0,0,0,
	    		    writer, 1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
	    writer.closeLowLevelField();          
	
	    Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
	    
	    Pipe.confirmLowLevelWrite(localOutput, headerSize);
	    Pipe.publishWrites(localOutput);
	    
	    logger.info("published error {} ",status);
	}

}
