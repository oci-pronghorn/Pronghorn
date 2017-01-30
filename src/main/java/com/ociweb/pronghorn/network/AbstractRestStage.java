package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public abstract class AbstractRestStage< T extends Enum<T> & HTTPContentType,
                                         R extends Enum<R> & HTTPRevision,
                                         V extends Enum<V> & HTTPVerb,
                                         H extends Enum<H> & HTTPHeaderKey> extends PronghornStage {
   
    private static final byte[] RETURN_NEWLINE = "\r\n".getBytes();

	private static final byte[] EXPIRES_ZERO = "Expires: 0\r\n".getBytes();

	private static final byte[] PRAGMA_NO_CACHE = "Pragma: no-cache\r\n".getBytes();

	private static final byte[] CACHE_CONTROL_NO_CACHE = "Cache-Control: no-cache, no-store, must-revalidate\r\n".getBytes();

	protected final HTTPSpecification<T,R,V, H> httpSpec;
    
    protected static final byte[] OK_200        = " 200 OK\r\n".getBytes();
    protected static final byte[] Not_Found_404 = " 404 Not Found\r\n".getBytes();
    
    protected static final byte[] X_400 = " 400 OK\r\n".getBytes();
    protected static final byte[] X_500 = " 500 OK\r\n".getBytes();
    
    protected static final byte[] SERVER = "Server: GreenLightning\r\n".getBytes();//Apache/1.3.3.7 (Unix) (Red-Hat/Linux)".getBytes();
    protected static final byte[] ETAG = "ETag: ".getBytes();
    
    protected static final byte[] ZERO = new byte[] {'0'};
    
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestStage.class);
    
    
    
//    HTTP/1.1 200 OK
//    Server: nginx/1.10.0 (Ubuntu)
//    Date: Mon, 16 Jan 2017 17:18:27 GMT
//    Content-Type: application/json
//    Content-Length: 30
//    Last-Modified: Mon, 16 Jan 2017 16:50:59 GMT
//    Connection: keep-alive
//    ETag: "587cf9f3-1e"
//    Accept-Ranges: bytes
//
//    {"x":9,"y":17,"groovySum":26}
    
   private static final byte[] EXTRA_STUFF = "Date: Mon, 16 Jan 2017 17:18:27 GMT\r\nLast-Modified: Mon, 16 Jan 2017 16:50:59 GMT\r\nETag: \"587cf9f3-1e\"\r\nAccept-Ranges: bytes\r\n".getBytes();
    
    
    
    protected static final byte[][] CONNECTION = new byte[][] {
        "Connection: open\r\n".getBytes(),
        "Connection: close\r\n".getBytes()
    };
    
    protected static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    protected static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    protected static final byte[] CONTENT_LOCATION = "Content-Location: ".getBytes();
        
    
    protected AbstractRestStage(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, HTTPSpecification<T,R,V,H> httpSpec) {
        super(graphManager,inputs,outputs);

        this.httpSpec = httpSpec;
    }
    
    protected AbstractRestStage(GraphManager graphManager, Pipe input, Pipe[] outputs, HTTPSpecification<T,R,V,H> httpSpec) {
       super(graphManager, input, outputs);

       this.httpSpec = httpSpec;
    }
    
    protected AbstractRestStage(GraphManager graphManager, Pipe[] inputs, Pipe output, HTTPSpecification<T,R,V,H> httpSpec) {
        super(graphManager, inputs, output);
        
        this.httpSpec = httpSpec;
    }
    
    protected AbstractRestStage(GraphManager graphManager, Pipe input, Pipe output, HTTPSpecification<T,R,V,H> httpSpec) {
        super(graphManager,input,output);
        
        this.httpSpec = httpSpec;
    }
    

    protected int publishHeaderMessage(int originalRequestContext, int sequence, int thisRequestContext, int status,                                     
                                        Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow,
                                        HTTPSpecification<T,R,V,H> httpSpec, byte[] revision, byte[] contentType, 
                                        byte[] localSizeAsBytes, int localSizeAsBytesPos, int localSizeAsBytesLen, int localSizeAsByteMask, 
                                        byte[] localETagBytes, boolean reportServer,
                                        byte[] contLocBytes, int contLocBytesPos, int contLocBytesLen, int contLocBytesMask
                                            		
    		) {
        
        int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
        
        Pipe.addIntValue(channelIdHigh, localOutput);
        Pipe.addIntValue(channelIdLow, localOutput);
        Pipe.addIntValue(sequence, localOutput);        
        
//        logger.info("publish rest response for channel {} {} and seq {}",channelIdHigh, channelIdLow, sequence);
        
        DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.outputStream(localOutput);
        writer.openField();
        writeHeader(revision, 
        		    status, originalRequestContext, localETagBytes,  
        		    contentType, 
        		    localSizeAsBytes, localSizeAsBytesPos, localSizeAsBytesLen, localSizeAsByteMask, 
        		    reportServer,
        		    contLocBytes, contLocBytesPos, contLocBytesLen,  contLocBytesMask,
        		    writer);
        int bytesLength = writer.closeLowLevelField();
        
        Pipe.addIntValue( thisRequestContext , localOutput); //empty request context, set the full value last. 
        
        Pipe.confirmLowLevelWrite(localOutput, headerSize);
        int consumed = Pipe.publishWrites(localOutput);
        assert(consumed == bytesLength) : "header bytes length of "+bytesLength+" but total sent was "+consumed;
        //logger.debug("published header");
        
        return bytesLength;
    }
    
    protected void publishError(int requestContext, int sequence, int status,
                                Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow, HTTPSpecification<T,R,V, H> httpSpec, 
                                int revision, int contentType) {
        
        int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 

        Pipe.addIntValue(channelIdHigh, localOutput);
        Pipe.addIntValue(channelIdLow, localOutput);
        Pipe.addIntValue(sequence, localOutput);
        
        DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.outputStream(localOutput);        
        writer.openField();
        writeHeader(httpSpec.revisions[revision].getBytes(), status, requestContext, null, contentType<0 ? null :httpSpec.contentTypes[contentType].getBytes(), 
        		    ZERO, 0, 1, 1, false, null, 0,0,0,
        		    writer);
        writer.closeLowLevelField();          

        Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
        
        Pipe.confirmLowLevelWrite(localOutput, headerSize);
        Pipe.publishWrites(localOutput);
        
        logger.info("published error {} ",status);
    }
    
    
    //TODO: build better constants for these values needed.
    public static void writeHeader(byte[] revisionBytes, int status, int requestContext, byte[] etagBytes, byte[] typeBytes, 
    		                       byte[] lenAsBytes, int lenAsBytesPos, int lenAsBytesLen, int  lenAsBytesMask, boolean server,
    		                       byte[] contLocBytes, int contLocBytesPos, int contLocBytesLen, int contLocBytesMask,
    		                       DataOutputBlobWriter<ServerResponseSchema> writer) {
             
            //line one
            writer.write(revisionBytes);
            if (200==status) {
                writer.write(OK_200);
            } else {
            	if (404==status) {
            		writer.write(Not_Found_404);
            	} else if (400==status) {
                    writer.write(X_400);
                } else if (500==status) {
                    writer.write(X_500);
                } else {
                    throw new UnsupportedOperationException("Unknwown status "+status);
                }
            }
            
            //line two
            if (server) {
            	writer.write(SERVER);
            }
            
            if (null!=etagBytes) {
                writer.write(ETAG);
                writer.write(etagBytes); //ETag: "3f80f-1b6-3e1cb03b"
                writer.write(RETURN_NEWLINE);
            }          
            
            // CONTENT_LOCATION
            if (null!=contLocBytes) {
                writer.write(CONTENT_LOCATION);
                DataOutputBlobWriter.write(writer, contLocBytes, contLocBytesPos, contLocBytesLen, contLocBytesMask);
                writer.write(RETURN_NEWLINE);
            }
            
            //turns off all client caching
//            writer.write(CACHE_CONTROL_NO_CACHE);
//            writer.write(PRAGMA_NO_CACHE);
//            writer.write(EXPIRES_ZERO);
            
            //line three
            if (null!=typeBytes) {
                writer.write(CONTENT_TYPE);
                writer.write(typeBytes);
                writer.write(RETURN_NEWLINE);
            }
            
            //line four
            if (null!=lenAsBytes) {
                writer.write(CONTENT_LENGTH);
                DataOutputBlobWriter.write(writer, lenAsBytes, lenAsBytesPos, lenAsBytesLen, lenAsBytesMask);
                writer.write(RETURN_NEWLINE);
            }
            
            //line five            
            int closeIdx = 1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT);
            writer.write(CONNECTION[closeIdx]);
            writer.write(EXTRA_STUFF);
            writer.write(RETURN_NEWLINE);
            //now ready for content
    
    }
    

}
