package com.ociweb.jfast.stream;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class FASTRingBufferTest {

    @Test
    public void bytesWriteRead() {
        
        FASTRingBuffer rb = new FASTRingBuffer((byte)7, (byte)7, null,  FieldReferenceOffsetManager.TEST);
        
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
        assertEquals(7, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(0, rb.buffer, rb.mask, rb.workingTailPos)));
        // constant from heap or dynamic from char ringBuffer
        int rawPos = FASTRingBuffer.readRingByteRawPos(1, rb.buffer, rb.mask, rb.workingTailPos);                     
        
        //read back the array
        byte[] data = FASTRingBuffer.readRingByteBuffers(rawPos, rb);
        int i = 0;
        while (i<source.length) {            
            assertEquals("index:"+i,source[i],data[i]);                       
            i++;
        }        
        
        //assertEquals(source,data);
        
        assertEquals(source.length, FASTRingBuffer.readRingByteLen(1,rb.buffer,rb.mask,rb.workingTailPos));
                        
        
    }
    

    private long timeArrayCopy(int size, int mask, long test) {
        int target[] = new int[size];

        long start = System.nanoTime();
        long i = test;
        while (--i>=0) {
            target[mask&(int)i] = (int)i;
        }
        data = target;//assignment prevents escape analysis optimization
        return System.nanoTime()-start;
    }
    int[] data;

    @Test
    public void speedTest() {
        
        int size = 1<<11;
        int mask = size-1;   
        final int testSize = 20000000;
        float bits = testSize*4f*8f;  
        float rate = bits/(float)timeArrayCopy(size, mask, testSize); //bits per nano second.
        //do it again to get the better value now that it has been optimized
        rate = Math.max(rate, bits/(float)timeArrayCopy(size, mask, testSize));
        float theoreticalMaximumMillionBitsPerSecond = rate*1000; //div top by 1m and bottom by 1b
        
        System.err.println("theoretical maximum mbps:"+theoreticalMaximumMillionBitsPerSecond);
        
           
        
        
        byte primaryBits = 14;
        byte charBits = 7;
        
        final FASTRingBuffer rb = new FASTRingBuffer(primaryBits, charBits, null,  FieldReferenceOffsetManager.TEST);
        final int rbMask = rb.mask;
        final int[] rbB = rb.buffer;

        final int tests = 3;
        final int messageSize = 256;//256;//47;
        final int granularity = messageSize<<rb.chunkBits;
        

        
        Runnable reader = new Runnable() {

            @Override
            public void run() {
                int k = tests;
                while (--k>=0) {
                    int i = testSize;
                    
                    AtomicLong hp = rb.headPos;
                    AtomicLong tp = rb.tailPos;
                    
                    long headPosCache = hp.longValue();
                    long targetHead =  granularity+tp.longValue();
                    PaddedLong wrkTlPos = rb.workingTailPos;
                    
                    while (--i>=0) {
                        int j = messageSize;
                        
                        //wait for at least n messages to be available 
                        //waiting for headPos to change
                        
                        

                        headPosCache = FASTRingBuffer.spinBlock(hp, headPosCache, targetHead);
                        
                        
                        //read the record
                        while (--j>=0) {
                            
                            //int value = FASTRingBufferReader.readInt(rb, 0); //read from workingTailPosition + 0
                            int value = FASTRingBufferReader.readInt(rbB, rbMask, wrkTlPos, 0);
                            if (value!=j) {                                
                                fail("expected "+j+" but found "+i);
                            }   
                            wrkTlPos.value++; //TODO: C, reader has external inc but writer has internal inc
                            
                        }
                        
                        //allow writer to write up to new tail position
                        if (0==(i&rb.chunkMask) ) {  
                            FASTRingBuffer.publishWrites(tp, wrkTlPos);
                            targetHead = wrkTlPos.value + granularity;
                        }
                        
                    }
                }
            }
            
        };
        new Thread(reader).start();
        
        int k = tests;
        while (--k>=0) {
        
            long start = System.nanoTime();
            
            AtomicLong hp = rb.headPos;
            AtomicLong tp = rb.tailPos;
            
            int i = testSize;
            long tailPosCache = tp.get();
            long targetPos = hp.longValue()+granularity-rb.maxSize;
            PaddedLong wrkHdPos = rb.workingHeadPos;
            while (--i>=0) {
                
                int j = messageSize;
                //wait for room to fit one message
                //waiting on the tailPosition to move the others are constant for this scope.
                //workingHeadPositoin is same or greater than headPosition
                
                tailPosCache = FASTRingBuffer.spinBlock(tp, tailPosCache, targetPos);

                
                //write the record
                while (--j>=0) {                    
                    FASTRingBuffer.addValue(rbB, rbMask, wrkHdPos, j);  
                }
                //allow read thread to read up to new head position
                if (0==(i&rb.chunkMask) ) {
                    FASTRingBuffer.publishWrites(hp, wrkHdPos);
                    targetPos = wrkHdPos.value + granularity;
                    targetPos -= rb.maxSize;
                }
            }
            tailPosCache = FASTRingBuffer.spinBlock(tp, tailPosCache, hp.longValue());

            long duration = System.nanoTime()-start;
            
            float milMessagePerSec = 1000f*(testSize/(float)duration);
            float milBitsPerSec = milMessagePerSec*messageSize*4*8;
            float pctEff = milBitsPerSec/theoreticalMaximumMillionBitsPerSecond;
            
            System.err.println();
            System.err.println("million bits per second:"+milBitsPerSec+" effecient:"+pctEff);
            System.err.println("million messages per second:"+milMessagePerSec+" duration:"+duration);
            System.err.println("million fields per second:"+(messageSize*milMessagePerSec));
            
            //given the target gps what pct cpu is free? 
            
            assertTrue("Must be able to move data from one thread to the next no slower than 50% of a single thread array copy, only got:"+pctEff,pctEff>=.5);
        }
        
        
    }


    
    
    
    
}
