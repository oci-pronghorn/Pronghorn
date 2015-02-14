package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.addByteArray;
import static com.ociweb.pronghorn.ring.RingBuffer.addValue;
import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.dump;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTailTillMatchesHead;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;
import static com.ociweb.pronghorn.ring.RingBuffer.takeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.ring.RingBuffer;

public class RingBufferTest {

   //only here outside the scope of the method to prevent escape analysis from detecting that
   //the timing loop is doing no work and then optimizing it away.
   int[] data;
	
	
    @Test
    public void simpleBytesWriteRead() {
        
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
        RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
        
        byte[] testArray = new byte[]{(byte)1,(byte)2,(byte)3,(byte)4,(byte)5};
        int testInt = 7;
        
        //clear out the ring buffer
        dump(ring);
        
        //write one integer to the ring buffer
        addValue(ring, testInt);       
        
        //write array of bytes to ring buffer
        addByteArray(testArray, 0, testArray.length, ring);             
        
        //unblock for reading
        publishWrites(ring);
                
        //read one integer back and confirm it matches
        assertEquals(testInt, takeValue(ring)); 
        
        // constant from heap or dynamic from char ringBuffer
        int meta = takeRingByteMetaData(ring); //MUST take this one before the length they come in order       
                
        //confirm the length is the same
        int len = takeRingByteLen(ring);
        assertEquals(testArray.length, len); //MUST take this one second after the meta they come in order    
        
        //read back the array and confirm it matches
        int mask = byteMask(ring); //data often loops around end of array so this mask is required
        byte[] data = byteBackingArray(meta, ring);
        int offset = bytePosition(meta, ring, len);
        int c = testArray.length;
        while (--c >= 0) {
        	int i = c + offset;
        	assertEquals("index:"+i, testArray[i], data[i & mask]);        	
        }           
        
    }
    

    /**
     * Method used to record a baseline of the absolute fastest time possible on this platform.
     *   
     * @param size
     * @param mask
     * @param test
     * @return
     */
    private long timeArrayCopy(int size, int mask, long test) {
        int target[] = new int[size];

        long start = System.nanoTime();
        long i = test;
        while (--i>=0) {
            target[mask&(int)i] = (int)i;
        }
        data = target;//assignment prevents escape analysis optimization which would delete all this code at runtime
        return System.nanoTime()-start;
    }
    
    

    @Ignore
    public void primaryRingSpeedTest() {
        
        int size = 1<<11;
        int mask = size-1;   
        final int totalMesssages = 2000000;
        final int intSize = 4;
        float totalBitsInTest = totalMesssages*intSize*8f;  
        float rate = totalBitsInTest/(float)timeArrayCopy(size, mask, totalMesssages); //bits per nano second.
        int k = 1024;
        while (--k>=0) {
        	//do it again to get the better value now that it has been runtime optimized
        	rate = Math.max(rate, totalBitsInTest/(float)timeArrayCopy(size, mask, totalMesssages));
        }
        float theoreticalMaximumMillionBitsPerSecond = rate*1000; //div top by 1m and bottom by 1b
        
        System.out.println("theoretical maximum mbps:"+theoreticalMaximumMillionBitsPerSecond);
                                   
        
        //grouping fragments together gives a clear advantage but more latency
        //as this gets near half the primary bits size the performance drops off
        //tuning values here can greatly help throughput but should remain <= 1/3 of primary bits
        final int chunkBits = 4; 
        final int chunkMask = (1<<chunkBits)-1;
                
        byte primaryBits = 13;
        byte charBits = 7;        
        
        final int tests = 3;
        final int totalMessageFields = 256;//256;//47;
        final int granularity = totalMessageFields<<chunkBits;
        final int fill =  (1<<primaryBits)-granularity;
                
        
        
        ExecutorService exService = Executors.newSingleThreadExecutor();

                
        int testRunCount = tests;
        while (--testRunCount>=0) {
        	 
            long start = System.nanoTime();
            
            final RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryBits, charBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
            //creating an anonymous inner class that implements runnable so we can hand this
            //off to the execution service to be run on another thread while this thread does the writing.
            Runnable reader = new Runnable() {

                @Override
                public void run() {

                        int messageCount = totalMesssages;
                        
                        long headPosCache = spinBlockOnHead(headPosition(ring), tailPosition(ring)+granularity, ring);
                        
                        while (--messageCount>=0) {
                            int messageFieldCount = totalMessageFields;
                                                    
                            //read the message
                            while (--messageFieldCount>=0) {                            
                                int value = takeValue(ring);                             
                                //not calling normal unit test method because it will slow down the run
                                if (value!=messageFieldCount) {                                
                                    fail("expected "+messageFieldCount+" but found "+messageCount);
                                }                            
                            }
                            
                            //allow writer to write up to new tail position
                            if (0==(messageCount&chunkMask) ) {
                            	releaseReadLock(ring);
                            	if (messageCount>0) {
                            		headPosCache = spinBlockOnHead(headPosCache, tailPosition(ring)+granularity, ring);
                            		
                            	}
                            }                        
                        }
                }            
            };        
            
           
            exService.submit(reader);//this reader starts running immediately
                        
            
            
            int messageCount = totalMesssages;
            
            //keep local copy of the last time the tail was checked to avoid contention.
            long tailPosCache = spinBlockOnTail(tailPosition(ring), headPosition(ring)-fill, ring);
                        
            while (--messageCount>=0) {
                
                int messageFieldCount = totalMessageFields;                
                //write the record
                while (--messageFieldCount>=0) {                    
                    addValue(ring, messageFieldCount);  
                }
                
                if (0==(messageCount&chunkMask) ) {
                    publishWrites(ring);
                    //wait for room to fit one message
                    //waiting on the tailPosition to move the others are constant for this scope.
                    //workingHeadPositoin is same or greater than headPosition
                    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(ring)-fill, ring);
                }
            }
            //wait until the other thread is finished reading
            tailPosCache = spinBlockOnTailTillMatchesHead(tailPosCache, ring);
            
            long duration = System.nanoTime()-start;
            
            float milMessagePerSec = 1000f*(totalMesssages/(float)duration);
            float milBitsPerSec = milMessagePerSec*totalMessageFields*4*8;
            float pctEff = milBitsPerSec/theoreticalMaximumMillionBitsPerSecond;
            
            System.out.println();
            System.out.println("	million bits per second:"+milBitsPerSec+" effecient:"+pctEff);
            System.out.println("	million messages per second:"+milMessagePerSec+" duration:"+duration);
            System.out.println("	million fields per second:"+(totalMessageFields*milMessagePerSec));
            
            //given the target gps what pct cpu is free? 
            
            assertTrue("Must be able to move data from one thread to the next no slower than 33% of a single thread array copy, only got:"+pctEff,pctEff>=.2);
        }
        
        //clean shutdown of thread executor
        exService.shutdown();
        try {
			exService.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			//ignore we are exiting
		}
        
        System.out.println("goodbye");
        
    }


    @Ignore
    public void byteBlockRingSpeedTest() {
        
    	final byte[] testArray = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@".getBytes();//, this is a reasonable test message.".getBytes();
    	
        final int totalMesssages = 200000;
                                           
        //grouping fragments together gives a clear advantage but more latency
        //as this gets near half the primary bits size the performance drops off
        //tuning values here can greatly help throughput but should remain <= 1/3 of primary bits
        final int chunkBits = 4; 
        final int chunkMask = (1<<chunkBits)-1;
        
        //must be big enough for the message sizes between publish requests
        byte primaryBits = 16; 
        
        //given maximum bytes used per message and the maximum messages in the above queue how long does the 
        //inner queue have to be so it does not wrap on its self.
        int temp = ((1<<(primaryBits-1)))*testArray.length;
        int b = 0;
        while (temp!=0) {
        	b++;
        	temp>>=1;
        }

        byte charBits = (byte)b;        
                        
        final int tests = 3;
        final int totalMessageFields = 256; //Must not be bigger than primary buffer size
        final int granularity = totalMessageFields<<chunkBits;
        final int fill =  (1<<primaryBits)-granularity;
                
        ExecutorService exService = Executors.newFixedThreadPool(4);

                                
        //writes will only hang if the reader has died and there is no room in the queue.
        
        int testRunCount = tests;
        while (--testRunCount>=0) {     

            long start = System.nanoTime();
            
            final RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryBits, charBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
            //creating an anonymous inner class that implements runnable so we can hand this
            //off to the execution service to be run on another thread while this thread does the writing.
            Runnable reader = new Runnable() {

                @Override
                public void run() {           	
        	
    	                    int messageCount = totalMesssages;
    	                    
    	                    //only enter this block when we know there are records to read
    	                    long headPosCache = spinBlockOnHead(headPosition(ring), tailPosition(ring)+granularity, ring);	                    
    	                    while (--messageCount>=0) {
    	                        //read the message
    	                    	int messageFieldCount = totalMessageFields;
    	                        while (--messageFieldCount>=0) {
    	                        	
    	                        	int meta = takeRingByteMetaData(ring);
    	                        	int len = takeRingByteLen(ring);

    	                        	validateBytes(testArray, ring, granularity,	messageFieldCount, meta, len);
    	                            
    	                        }
    	                        
    	                        //allow writer to write up to new tail position
    	                        if (0==(messageCount&chunkMask) ) {
    	                        	releaseReadLock(ring);
    	                        	if (messageCount>0) {
    	                        		headPosCache = spinBlockOnHead(headPosCache, tailPosition(ring)+granularity, ring);	                        	    	                        		
    	                        	}
    	                        }	                        
    	                    }                    
                }                
            };
            
            
            exService.submit(reader);//this reader starts running immediately
                       
            
            int messageCount = totalMesssages;            
            //keep local copy of the last time the tail was checked to avoid contention.
            long tailPosCache = spinBlockOnTail(tailPosition(ring), headPosition(ring)-fill, ring);                        
            while (--messageCount>=0) {
                //write the record
                int messageFieldCount = totalMessageFields;
                while (--messageFieldCount>=0) {
                	addByteArray(testArray, 0, testArray.length, ring);
                }
                if (0==(messageCount&chunkMask) ) {
                    publishWrites(ring);
                    //wait for room to fit one message
                    //waiting on the tailPosition to move the others are constant for this scope.
                    //workingHeadPositoin is same or greater than headPosition
                    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(ring)-fill, ring);
                }
                                
            }
            //wait until the other thread is finished reading
            tailPosCache = spinBlockOnTailTillMatchesHead(tailPosCache, ring);

            long duration = System.nanoTime()-start;
            
            float milMessagePerSec = 1000f*(totalMesssages/(float)duration);
            int fieldSize = testArray.length;
            float milBitsPerSec = milMessagePerSec*totalMessageFields*fieldSize*8;
            
            System.out.println("byte transfer test");
            System.out.println("	million bits per second:"+milBitsPerSec);
            System.out.println("	million messages per second:"+milMessagePerSec+" duration:"+duration);
            System.out.println("	million fields per second:"+(totalMessageFields*milMessagePerSec));
            
        }

        //clean shutdown of thread executor
        exService.shutdown();
        try {
			exService.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			//ignore we are exiting
		}
    }
    
	private void validateBytes(final byte[] testArray, final RingBuffer ring, final int granularity,
			int messageFieldCount, int meta, int len) {
		
		try {

			if (meta < 0) {
				fail("meta should only contain normal values no constants in this test");
			}

			// not calling normal unit test method because it will slow down the
			// run
			if (testArray.length != len) {
				fail("expected " + testArray.length + " but found " + len
						+ " gr " + granularity + "   working tail pos "
						+ ring.workingTailPos.value);
			}

			// not checking every byte for equals because it would slow down
			// this test
			if (0 == (messageFieldCount & 0x3F)) {
				// read back the array and confirm it matches
				int mask = byteMask(ring); // data often loops around end of
											// array so this mask is required

				byte[] data = byteBackingArray(meta, ring);
				int offset = bytePosition(meta, ring, len);
				int c = testArray.length;
				while (--c >= 0) {
					int i = offset + c;
					// System.err.println("c "+c);
					int tmp = i & mask;
					if ((int) testArray[c] != (int) data[tmp]) {

						System.err.println(Arrays.toString(Arrays.copyOfRange(
								data, tmp - 10, tmp))
								+ " "
								+ Arrays.toString(Arrays.copyOfRange(data, tmp,
										tmp + 10)));

						fail(" index:" + c + " expected " + testArray[c]
								+ " but found " + data[tmp] + " at " + i
								+ " or " + tmp + " vs " + c);
					}
				}

			}

		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(-1);
		}
		
	}    
    
}
