package com.ociweb.jfast.loader;

import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputTotals;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;

public class FASTEncoderTest {

    
    @Test
    public void testEncodeParallel() {
        
        //need to do this in parallel 
        
        //TODO: A, build a template generator that has 1 field
        //TODO: A, populate ring buffer with single field.
        
        
        FASTOutput fastOutput2 = new FASTOutputTotals();
        
        ////////////
        //writer
        ///////////
        int writeBuffer=4096;  //buffer size for the encoder
        int maxGroupCount=100;  //max group depth 
        boolean minimizeLatency=false;        
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput2, maxGroupCount, minimizeLatency);
        
        //////////////
        //Write all the new data into this ring buffer
        ////////////
//        byte[] byteConstants = null;
//        FieldReferenceOffsetManager from = null;
//        byte pBits = 22;
//        byte bBits = 20;
//        FASTRingBuffer queue = new FASTRingBuffer(pBits, bBits, byteConstants, from);
//                
//        //////////
//        //load encoder for this catalog
//        //////////
//        byte[] catBytes= null;
//        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes); 
//        
//        
//        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, queue, writerDispatch);
//        
//        
//        while (FASTRingBuffer.moveNext(queue)) {
//            if (queue.consumerData.isNewMessage()) {
//                msgs.incrementAndGet();
//            }
//            try{   
//                dynamicWriter.write();
//            } catch (FASTException e) {
//                System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(queue.from.tokens[writerDispatch.getActiveScriptCursor()]));
//                throw e;
//            }                            
//            grps++;
//        }
        
        
        
        
        
    }
    
    
    
    
    
}
