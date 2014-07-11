package com.ociweb.jfast.stream;

import org.junit.Test;

import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class FASTRingBufferTest {

    @Test
    public void arrayWrite() {
        int size = 1<<11;
        int mask = size-1;
        
        long test = 100000000;
        
        long start = System.nanoTime();
        int target[] = new int[size];
        long i = test;
        while (--i>=0) {
            target[mask&(int)i] = (int)i;
        }
        data = target;
        long duration = System.nanoTime()-start;
        
        float bits = test*4*8;
        float rate = (test*4f*8f)/(float)duration; //bits per nano second.
        float mbps = rate*1000; //div top by 1m and bottom by 1b
        System.err.println("duration:"+duration+ " bits:"+bits+" bits per nano:"+rate+" mbps "+mbps);
        
    }
    int[] data;

    @Test
    public void speedTest() {
        
        int k = 2;
        while (--k>=0) {
        
            byte primaryBits = 8;
            byte charBits = 7;
            
            FASTRingBuffer rb = new FASTRingBuffer(primaryBits, charBits, null,  null, null);
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
                FASTRingBuffer.unBlockFragment(rb.headPos,rb.addPos);
                FASTRingBuffer.dump(rb);
            }
            long duration = System.nanoTime()-start;
            
            float milMessagePerSec = 1000f*(testSize/(float)duration);
            float milBitsPerSec = milMessagePerSec*messageSize*4*8;
            
            System.err.println();
            System.err.println("million bits per second:"+milBitsPerSec);
            System.err.println("million messages per second:"+milMessagePerSec+" duration:"+duration);
            System.err.println("million fields per second:"+(messageSize*milMessagePerSec));
        }
        
        
    }


    
    
    
    
}
