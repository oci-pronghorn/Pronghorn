package com.ociweb.pronghorn.pipe;

import static com.ociweb.pronghorn.pipe.Pipe.addByteArray;
import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.headPosition;
import static com.ociweb.pronghorn.pipe.Pipe.publishWrites;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

public class PipeTest {

   //only here outside the scope of the method to prevent escape analysis from detecting that
   //the timing loop is doing no work and then optimizing it away.
   int[] data;
	
	
    @Test
    public void simpleBytesWriteRead() {
        
    	byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
    	byte byteRingSizeInBits = 16;
    	
        Pipe pipe = new Pipe(new PipeConfig(RawDataSchema.instance, 1<<primaryRingSizeInBits, 1<<byteRingSizeInBits));
        pipe.initBuffers();
        
        byte[] testArray = new byte[]{(byte)1,(byte)2,(byte)3,(byte)4,(byte)5};
        int testInt = 7;
        
        //clear out the ring buffer
        Pipe.publishWorkingTailPosition(pipe, Pipe.headPosition(pipe));
        
        //write one integer to the ring buffer
        ///Pipe.addIntValue(testInt, pipe);       
        
        //write array of bytes to ring buffer
        addByteArray(testArray, 0, testArray.length, pipe);             
        
        //unblock for reading
        publishWrites(pipe);
                
        //read one integer back and confirm it matches
        //assertEquals(testInt, takeValue(pipe)); 
        
        // constant from heap or dynamic from char ringBuffer
        int meta = takeRingByteMetaData(pipe); //MUST take this one before the length they come in order       
                
        //confirm the length is the same
        int len = takeRingByteLen(pipe);
        assertEquals(testArray.length, len); //MUST take this one second after the meta they come in order    
        
        //read back the array and confirm it matches
        int mask = blobMask(pipe); //data often loops around end of array so this mask is required
        byte[] data = byteBackingArray(meta, pipe);
        int offset = bytePosition(meta, pipe, len);
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
            
            final Pipe ring = new Pipe(new PipeConfig(RawDataSchema.instance, 1<<primaryBits, 1<<charBits));
            //creating an anonymous inner class that implements runnable so we can hand this
            //off to the execution service to be run on another thread while this thread does the writing.
            Runnable reader = new Runnable() {

                @Override
                public void run() {

                        int messageCount = totalMesssages;
						long lastCheckedValue = headPosition(ring);
						while ( lastCheckedValue < tailPosition(ring)+granularity) {
							Pipe.spinWork(ring);//TODO: WARNING this may hang when using a single thread scheduler
						    lastCheckedValue = Pipe.headPosition(ring);
						}
                        
                        long headPosCache = lastCheckedValue;
                        
                        while (--messageCount>=0) {
                            int messageFieldCount = totalMessageFields;
                                                    
                            //read the message
                            while (--messageFieldCount>=0) {                            
                                int value = Pipe.takeInt(ring);                             
                                //not calling normal unit test method because it will slow down the run
                                if (value!=messageFieldCount) {                                
                                    fail("expected "+messageFieldCount+" but found "+messageCount);
                                }                            
                            }
                            
                            //allow writer to write up to new tail position
                            if (0==(messageCount&chunkMask) ) {
                            	Pipe.releaseReadLock(ring);
                            	if (messageCount>0) {
                            		long lastCheckedValue1 = headPosCache;
									while ( lastCheckedValue1 < tailPosition(ring)+granularity) {
										Pipe.spinWork(ring);//TODO: WARNING this may hang when using a single thread scheduler
									    lastCheckedValue1 = Pipe.headPosition(ring);
									}
									headPosCache = lastCheckedValue1;
                            		
                            	}
                            }                        
                        }
                }            
            };        
            
           
            exService.submit(reader);//this reader starts running immediately
                        
            
            
            int messageCount = totalMesssages;
			long lastCheckedValue = tailPosition(ring);
			while (null==Pipe.slab(ring) || lastCheckedValue < headPosition(ring)-fill) {
				Pipe.spinWork(ring);
			    lastCheckedValue = Pipe.tailPosition(ring);
			}
            
            //keep local copy of the last time the tail was checked to avoid contention.
            long tailPosCache = lastCheckedValue;
                        
            while (--messageCount>=0) {
                
                int messageFieldCount = totalMessageFields;                
                //write the record
                while (--messageFieldCount>=0) {                    
                    Pipe.addIntValue(messageFieldCount, ring);  
                }
                
                if (0==(messageCount&chunkMask) ) {
                    publishWrites(ring);
					long lastCheckedValue1 = tailPosCache;
					while (null==Pipe.slab(ring) || lastCheckedValue1 < headPosition(ring)-fill) {
						Pipe.spinWork(ring);
					    lastCheckedValue1 = Pipe.tailPosition(ring);
					}
                    //wait for room to fit one message
                    //waiting on the tailPosition to move the others are constant for this scope.
                    //workingHeadPositoin is same or greater than headPosition
                    tailPosCache = lastCheckedValue1;
                }
            }
            //wait until the other thread is finished reading
            while ( Pipe.contentRemaining(ring) > 0 ) {
            }
            
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
            
            final Pipe pipe = new Pipe(new PipeConfig(RawDataSchema.instance, 1<<primaryBits, 1<<charBits));
            //creating an anonymous inner class that implements runnable so we can hand this
            //off to the execution service to be run on another thread while this thread does the writing.
            Runnable reader = new Runnable() {

                @Override
                public void run() {           	
        	
    	                    int messageCount = totalMesssages;
							long lastCheckedValue1 = headPosition(pipe);
							while ( lastCheckedValue1 < tailPosition(pipe)+granularity) {
								Pipe.spinWork(pipe);//TODO: WARNING this may hang when using a single thread scheduler
							    lastCheckedValue1 = Pipe.headPosition(pipe);
							}
    	                    
    	                    //only enter this block when we know there are records to read
    	                    long headPosCache = lastCheckedValue1;	                    
    	                    while (--messageCount>=0) {
    	                        //read the message
    	                    	int messageFieldCount = totalMessageFields;
    	                        while (--messageFieldCount>=0) {
    	                        	
    	                        	int meta = takeRingByteMetaData(pipe);
    	                        	int len = takeRingByteLen(pipe);

    	                        	validateBytes(testArray, pipe, granularity,	messageFieldCount, meta, len);
    	                            
    	                        }
    	                        
    	                        //allow writer to write up to new tail position
    	                        if (0==(messageCount&chunkMask) ) {
    	                        	Pipe.releaseReadLock(pipe);
    	                        	if (messageCount>0) {
    	                        		long lastCheckedValue = headPosCache;
										while ( lastCheckedValue < tailPosition(pipe)+granularity) {
											Pipe.spinWork(pipe);//TODO: WARNING this may hang when using a single thread scheduler
										    lastCheckedValue = Pipe.headPosition(pipe);
										}
										headPosCache = lastCheckedValue;	                        	    	                        		
    	                        	}
    	                        }	                        
    	                    }                    
                }                
            };
            
            
            exService.submit(reader);//this reader starts running immediately
                       
            
            int messageCount = totalMesssages;
			long lastCheckedValue = tailPosition(pipe);
			while (null==Pipe.slab(pipe) || lastCheckedValue < headPosition(pipe)-fill) {
				Pipe.spinWork(pipe);
			    lastCheckedValue = Pipe.tailPosition(pipe);
			}            
            //keep local copy of the last time the tail was checked to avoid contention.
            long tailPosCache = lastCheckedValue;                        
            while (--messageCount>=0) {
                //write the record
                int messageFieldCount = totalMessageFields;
                while (--messageFieldCount>=0) {
                	addByteArray(testArray, 0, testArray.length, pipe);
                }
                if (0==(messageCount&chunkMask) ) {
                    publishWrites(pipe);
					long lastCheckedValue1 = tailPosCache;
					while (null==Pipe.slab(pipe) || lastCheckedValue1 < headPosition(pipe)-fill) {
						Pipe.spinWork(pipe);
					    lastCheckedValue1 = Pipe.tailPosition(pipe);
					}
                    //wait for room to fit one message
                    //waiting on the tailPosition to move the others are constant for this scope.
                    //workingHeadPositoin is same or greater than headPosition
                    tailPosCache = lastCheckedValue1;
                }
                                
            }
            //wait until the other thread is finished reading
            while ( Pipe.contentRemaining(pipe) > 0 ) {
            }

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
    
	private void validateBytes(final byte[] testArray, final Pipe pipe, final int granularity,
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
						+ Pipe.getWorkingTailPosition(pipe));
			}

			// not checking every byte for equals because it would slow down
			// this test
			if (0 == (messageFieldCount & 0x3F)) {
				// read back the array and confirm it matches
				int mask = blobMask(pipe); // data often loops around end of
											// array so this mask is required

				byte[] data = byteBackingArray(meta, pipe);
				int offset = bytePosition(meta, pipe, len);
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
			fail();
		}
		
	}    
    
	
	@Test
	public void splitsAgreementTest() {
		//
		//the splitGroups must match the same distribution as splitPipes
		//
		for(int fullLen = 1; fullLen<70; fullLen++) {
			
			for(int groups = 1; groups<fullLen; groups++) {
				
				int[] index = Pipe.splitGroups(groups, fullLen);
				
				Pipe[] pipes = new Pipe[fullLen];
				int f = fullLen;
				while (--f>=0) {
					pipes[f] = new Pipe(new PipeConfig(RawDataSchema.instance));
				}
				
				Pipe[][] splits = Pipe.splitPipes(groups, pipes);
				
				//test split 
				int x = fullLen;
				while (--x>=0) {			
					Pipe p = pipes[x];
					int whereToFind = index[x];			
					Pipe[] expectedIn = splits[whereToFind];			
					assertTrue(contains(expectedIn, p));
					
					int r = groups;
					while (--r>=0) {
						if (r!=whereToFind) {
							assertFalse(contains(splits[r], p));
						}				
					}			
					
				}
			}
		}
	}


	private boolean contains(Pipe[] expectedIn, Pipe p) {
		int x = expectedIn.length;
		while (--x>=0) {
			if (expectedIn[x]==p) {
				return true;
			}
		}
		return false;
	}
	
}
