package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HTTPErrorStage <   T extends Enum<T> & HTTPContentType,
                                R extends Enum<R> & HTTPRevision,
                                V extends Enum<V> & HTTPVerb,
                                H extends Enum<H> & HTTPHeaderKey> extends AbstractRestStage<T,R,V,H> {

    Pipe<?> input;
    Pipe<ServerResponseSchema> output;
    
    /**
     * Send HTTP valid error messages
     * @param graphManager
     * @param input
     * @param output
     */    
    protected HTTPErrorStage(GraphManager graphManager,  Pipe<?> input, Pipe<ServerResponseSchema> output, HTTPSpecification<T,R,V,H> httpSpec) {
        super(graphManager, input, output, httpSpec);
        this.input = input;
        this.output = output;
    }

    
    
    @Override
    public void run() {
        
        while (Pipe.hasContentToRead(input)) {
            int msgId = Pipe.takeMsgIdx(input);
            
            //build up a body to send
            
            
            //Use common error publish method.
           // publishError(requestContext, sequence, status, writer, localOutput, channelIdHigh, channelIdLow, localRevisionBytes, localContentTypeBytes);
            
            //send body or set as unset for next entry.
            
            
            
            Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgId));
            Pipe.releaseReadLock(input);
            
        }
        
        
        
    }

}
