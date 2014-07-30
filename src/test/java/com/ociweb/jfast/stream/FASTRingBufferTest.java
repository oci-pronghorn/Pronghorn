package com.ociweb.jfast.stream;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class FASTRingBufferTest {

    @Test
    public void bytesWriteRead() {
        
        FASTRingBuffer rb = new FASTRingBuffer((byte)7, (byte)7, null,  null, null);
        
        byte[] source = new byte[]{(byte)1,(byte)2,(byte)3,(byte)4,(byte)5};
        
        //clear out the ring buffer
        FASTRingBuffer.dump(rb);
        
        //write one integer to the ring buffer
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos,7);
        
        //write array of bytes to ring buffer
        FASTRingBuffer.addByteArray(source, 0, source.length, rb);             
        
        //unblock for reading
        FASTRingBuffer.unBlockFragment(rb.headPos, rb.workingHeadPos);
                
        //read one integer back
        assertEquals(7, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(0, rb)));
        // constant from heap or dynamic from char ringBuffer
        int rawPos = FASTRingBuffer.readRingByteRawPos(1,rb);                     
        
        //read back the array
        byte[] data = FASTRingBuffer.readRingByteBuffers(rawPos, rb);
        int i = 0;
        while (i<source.length) {            
            assertEquals("index:"+i,source[i],data[i]);                       
            i++;
        }        
        
        //assertEquals(source,data);
        
        assertEquals(source.length, rb.readRingByteLen(1,rb));
                        
        
    }
    
    
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
        
        byte primaryBits = 10;
        byte charBits = 7;
        
        final FASTRingBuffer rb = new FASTRingBuffer(primaryBits, charBits, null,  null, null);
        final int rbMask = rb.mask;
        final int[] rbB = rb.buffer;

        final int testSize = 5000000;
        final int messageSize = 47;
        
        
        Runnable reader = new Runnable() {

            @Override
            public void run() {
                int k = 2;
                while (--k>=0) {
                    int i = testSize;
                    while (--i>=0) {
                        int j = messageSize;
                        while (FASTRingBuffer.contentRemaining(rb)<j) {
                        }
                        //System.err.println("A");
                        while (--j>=0) {
                            
                            FASTRingBufferReader.readInt(rb, j);
                            rb.workingTailPos.value++;
                            
                        }
                        rb.tailPos.lazySet(rb.workingTailPos.value);
                    }
                }
            }
            
        };
        new Thread(reader).start();
        
        int k = 2;
        while (--k>=0) {
        
            PaddedLong pos =rb.workingHeadPos;
                            
            
            
            long start = System.nanoTime();
            int i = testSize;
            while (--i>=0) {
                
                int j = messageSize;
                while (rb.availableCapacity()<j) {
                }
                //System.err.println("B");
                while (--j>=0) {
                    
                    FASTRingBuffer.addValue(rbB, rbMask, pos, i);
                    
                }
                rb.headPos.lazySet(rb.workingHeadPos.value);
            }
            while (FASTRingBuffer.contentRemaining(rb)>0) {
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
