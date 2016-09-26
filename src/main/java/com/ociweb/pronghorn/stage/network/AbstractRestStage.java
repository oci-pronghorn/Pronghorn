package com.ociweb.pronghorn.stage.network;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.network.config.HTTPContentType;
import com.ociweb.pronghorn.stage.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.stage.network.config.HTTPRevision;
import com.ociweb.pronghorn.stage.network.config.HTTPSpecification;
import com.ociweb.pronghorn.stage.network.config.HTTPVerb;
import com.ociweb.pronghorn.stage.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public abstract class AbstractRestStage< T extends Enum<T> & HTTPContentType,
                                         R extends Enum<R> & HTTPRevision,
                                         V extends Enum<V> & HTTPVerb,
                                         H extends Enum<H> & HTTPHeaderKey> extends PronghornStage {
   
    protected final HTTPSpecification<T,R,V, H> httpSpec;
    
    protected static final byte[] OK_200 = " 200 OK\n".getBytes();
    protected static final byte[] X_400 = " 400 OK\n".getBytes();
    protected static final byte[] X_500 = " 500 OK\n".getBytes();
    
    protected static final byte[] SERVER = "Server: Pronghorn\n".getBytes();//Apache/1.3.3.7 (Unix) (Red-Hat/Linux)".getBytes();
    protected static final byte[] ETAG = "ETag:".getBytes();
    
    protected static final byte[] ZERO = new byte[] {'0'};
        
    
    protected static final byte[][] CONNECTION = new byte[][] {
        "Connection: open\n".getBytes(),
        "Connection: close\n".getBytes()
    };
    
    protected static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    protected static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    
    
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
                                        DataOutputBlobWriter<ServerResponseSchema> writer, Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow,
                                        byte[] localRevisionBytes, byte[] localContentTypeBytes, byte[] localSizeAsBytes, byte[] localETagBytes) {
        
        int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
        //Pipe.addLongValue(channelId, localOutput);
        Pipe.addIntValue(channelIdHigh, localOutput);
        Pipe.addIntValue(channelIdLow, localOutput);
        Pipe.addIntValue(sequence, localOutput);
        Pipe.addIntValue( thisRequestContext , localOutput); //empty request context, set the full value last.                        
        writer.openField();
        writeHeader(localRevisionBytes, status, originalRequestContext, localETagBytes,  localContentTypeBytes, localSizeAsBytes, writer);
        int bytesLength = writer.closeLowLevelField();
        
        Pipe.confirmLowLevelWrite(localOutput, headerSize);
        Pipe.publishWrites(localOutput);
        return bytesLength;
    }
    
    protected void publishError(int requestContext, int sequence, int status,
                                DataOutputBlobWriter<ServerResponseSchema> writer, Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow, 
                                byte[] localRevisionBytes, byte[] localContentTypeBytes) {
        
        int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 

        Pipe.addIntValue(channelIdHigh, localOutput);
        Pipe.addIntValue(channelIdLow, localOutput);
        Pipe.addIntValue(sequence, localOutput);
        
        Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
        writer.openField();
        writeHeader(localRevisionBytes, status, requestContext, null, localContentTypeBytes, ZERO, writer);
        writer.closeLowLevelField();          
        
        Pipe.confirmLowLevelWrite(localOutput, headerSize);
        Pipe.publishWrites(localOutput);
    }
    
    
    //TODO: build better constants for these values needed.
    public void writeHeader(byte[] revisionBytes, int status, int requestContext, byte[] etagBytes, byte[] typeBytes, byte[] lenAsBytes, DataOutputBlobWriter<ServerResponseSchema> writer) {
             
            //line one
            writer.write(revisionBytes);
            if (200==status) {
                writer.write(OK_200);
            } else {
                if (400==status) {
                    writer.write(X_400);
                } else if (500==status) {
                    writer.write(X_500);
                } else {
                    throw new UnsupportedOperationException("Unknwown status "+status);
                }
            }
            
            //line two
            writer.write(SERVER);
            
            if (null!=etagBytes) {
                writer.write(ETAG);
                writer.write(etagBytes); //ETag: "3f80f-1b6-3e1cb03b"
                writer.write('\n');
            }            
            
            //line three
            if (null!=typeBytes) {
                writer.write(CONTENT_TYPE);
                writer.write(typeBytes);
                writer.write('\n');
            }
            
            //line four
            if (null!=lenAsBytes) {
                writer.write(CONTENT_LENGTH);
                writer.write(lenAsBytes);
                writer.write('\n');
            }
            
            //line five            
            int closeIdx = 1&(requestContext>>ServerConnectionWriterStage.CLOSE_CONNECTION_SHIFT);
            writer.write(CONNECTION[closeIdx]);
            writer.write('\n');
            //now ready for content
    
    }
    

}
