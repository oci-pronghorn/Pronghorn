package com.ociweb.jfast.stream;

import org.junit.Test;

import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class FASTRingBufferTest {

    

    @Test
    public void speedTest() {
        
        int k = 4;
        while (--k>=0) {
        
            byte maxFragDepth = 3;
            byte primaryBits = 8;
            byte charBits = 7;
            
            FASTRingBuffer rb = new FASTRingBuffer(primaryBits, charBits, null, maxFragDepth);
            int rbMask = rb.mask;
            int[] rbB = rb.buffer;
            PaddedLong pos =rb.addPos;
                    
            int testSize = 10000000;
            int messageSize = 47;
            
            
            long start = System.nanoTime();
            int i = testSize;
            while (--i>=0) {
                
                int j = messageSize;
                while (--j>=0) {
                    
                    FASTRingBuffer.addValue(rbB, rbMask, pos, i);
                    
                }
                FASTRingBuffer.unBlockFragment(rb);
                FASTRingBuffer.dump(rb);
            }
            long duration = System.nanoTime()-start;
            
            float milMessagePerSec = 1000f*(testSize/(float)duration);
            
            System.err.println("million messages per second:"+milMessagePerSec+" duration:"+duration);
            System.err.println("million fields per second:"+(messageSize*milMessagePerSec));
        }
        
        
    }


    
    
    
    
}
