package com.ociweb.jfast.ring;

import static com.ociweb.jfast.ring.RingBuffer.addByteArray;
import static com.ociweb.jfast.ring.RingBuffer.headPosition;
import static com.ociweb.jfast.ring.RingBuffer.publishWrites;
import static com.ociweb.jfast.ring.RingBuffer.releaseReadLock;
import static com.ociweb.jfast.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.jfast.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.jfast.ring.RingBuffer.tailPosition;
import static com.ociweb.jfast.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.jfast.ring.RingBuffer.takeRingByteMetaData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class RingBufferPipeline {

	private final byte[] testArray = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@".getBytes();//, this is a reasonable test message.".getBytes();
	private final int testMessages = 100000;
	private final int stages = 2;
	private final byte primaryBits   = 20;
	private final byte secondaryBits = 26;
		
    private final int chunkBits = 4; 
    private final int chunkMask = (1<<chunkBits)-1;
    
	@Test
	public void pipelineExample() {
		 		 
				 
		 //create all the threads, one for each stage
		 ExecutorService service = Executors.newFixedThreadPool(stages);
		 
		 //build all the rings
		 int j = stages-1;
		 RingBuffer[] rings = new RingBuffer[j];
		 while (--j>=0)  {
			 rings[j] = new RingBuffer(primaryBits,secondaryBits);
		 }
		 
		 //start the timer		 
		 long start = System.nanoTime();
		 
		 //add all the stages start running
		 j = 0;
		 service.submit(createStage(rings[j]));
//		 int i = stages-2;
//		 while (--i>=0) {
//			 service.submit(copyStage(rings[j++], rings[j]));			 
//		 }
		 service.submit(dumpStage(rings[j]));
		 
		 //prevents any new jobs from getting submitted
		 service.shutdown();
		 //blocks until all the submitted runnables have stopped
		 try {
			service.awaitTermination(10, TimeUnit.MINUTES);
		 } catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		 }
		 long duration = System.nanoTime()-start;
		 
		 
		 		 
	}

	 
	
	 
	private Runnable createStage(final RingBuffer outputRing) {
		
		
		return new Runnable() {

			@Override
			public void run() {
								
		      int fill = 2;
		      
	          int messageCount = testMessages;            
	          //keep local copy of the last time the tail was checked to avoid contention.
	          long tailPosCache = spinBlockOnTail(tailPosition(outputRing), headPosition(outputRing)-fill, outputRing);                        
	          while (--messageCount>=0) {
	        	  
	              //write the record
                  addByteArray(testArray, 0, testArray.length, outputRing);

	              if (0==(messageCount & chunkMask) ) {
	                  publishWrites(outputRing);
	                  //wait for room to fit one message
	                  //waiting on the tailPosition to move the others are constant for this scope.
	                  //workingHeadPositoin is same or greater than headPosition
	                  tailPosCache = spinBlockOnTail(tailPosCache, headPosition(outputRing)-fill, outputRing);
	              }            
	          }

	          //send negative length as poison pill to exit all runnables  
	      	  addByteArray(testArray, 0, -1, outputRing);
	      	  System.out.println("finished writing:"+testMessages);
			}
		};
	}

	private Runnable copyStage(RingBuffer inputRing, RingBuffer outputRing) {
		
		
		
		
		// TODO Auto-generated method stub
		return null;
	}
	
	private Runnable dumpStage(final RingBuffer inputRing) {
		
		return new Runnable() {

			long total = 0;
			
            @Override
            public void run() {           	
    	            	
            		    int granularity = 4;
            	
	                    //only enter this block when we know there are records to read
	                    long headPosCache = spinBlockOnHead(headPosition(inputRing), tailPosition(inputRing)-granularity, inputRing);	
	                    long messageCount = 0;
	                    System.out.println("started");
	                    while (true) {
	                        //read the message
	                   // 	System.out.println(messageCount);
	                        	
                        	int meta = takeRingByteMetaData(inputRing);
                        	int len = takeRingByteLen(inputRing);
                        	
                        	if (len<0) {
                        		System.out.println("exited after reading: Msg:" + messageCount+" Bytes:"+total);
                        		return;
                        	}
                        	
                        	total += len;
                        	
                        	//doing nothing with the data
	                        
	                        //allow writer to write up to new tail position
                        	 if (0==(messageCount & chunkMask) ) {
                        	//	 System.out.println(messageCount);
                        		 releaseReadLock(inputRing);
                        		 headPosCache = spinBlockOnHead(headPosCache, tailPosition(inputRing)-granularity, inputRing);	                        	    	                        		
                        	 }
	                        messageCount++;
	                    }   
            }                
        };
	}

	
	
//    @Test
//    public void byteBlockRingSpeedTest() {
//        
//    	//TODO: making this string long causes collision and ovderwrite!!
//    	final byte[] testArray = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@".getBytes();//, this is a reasonable test message.".getBytes();
//    	
//        final int totalMesssages = 200000;
//                                           
//        //grouping fragments together gives a clear advantage but more latency
//        //as this gets near half the primary bits size the performance drops off
//        //tuning values here can greatly help throughput but should remain <= 1/3 of primary bits
//        final int chunkBits = 4; 
//        final int chunkMask = (1<<chunkBits)-1;
//        
//        //must be big enough for the message sizes between publish requests
//        byte primaryBits = 16; 
//        
//        //given maximum bytes used per message and the maximum messages in the above queue how long does the 
//        //inner queue have to be so it does not wrap on its self.
//        int temp = ((1<<(primaryBits-1)))*testArray.length;
//        int b = 0;
//        while (temp!=0) {
//        	b++;
//        	temp>>=1;
//        }
//
//        byte charBits = (byte)b;        
//                        
//        final int tests = 3;
//        final int totalMessageFields = 256; //Must not be bigger than primary buffer size
//        final int granularity = totalMessageFields<<chunkBits;
//        final int fill =  (1<<primaryBits)-granularity;
//                
//        ExecutorService exService = Executors.newFixedThreadPool(4);
//
//                                
//        //writes will only hang if the reader has died and there is no room in the queue.
//        
//        int testRunCount = tests;
//        while (--testRunCount>=0) {     
//
//            long start = System.nanoTime();
//            
//            final RingBuffer ring = new RingBuffer(primaryBits, charBits);
//            //creating an anonymous inner class that implements runnable so we can hand this
//            //off to the execution service to be run on another thread while this thread does the writing.
//            Runnable reader = new Runnable() {
//
//                @Override
//                public void run() {           	
//        	
//    	                    int messageCount = totalMesssages;
//    	                    
//    	                    //only enter this block when we know there are records to read
//    	                    long headPosCache = spinBlockOnHead(headPosition(ring), tailPosition(ring)+granularity, ring);	                    
//    	                    while (--messageCount>=0) {
//    	                        //read the message
//    	                    	int messageFieldCount = totalMessageFields;
//    	                        while (--messageFieldCount>=0) {
//    	                        	
//    	                        	int meta = takeRingByteMetaData(ring);
//    	                        	int len = takeRingByteLen(ring);
//
//    	                        	validateBytes(testArray, ring, granularity,	messageFieldCount, meta, len);
//    	                            
//    	                        }
//    	                        
//    	                        //allow writer to write up to new tail position
//    	                        if (0==(messageCount&chunkMask) ) {
//    	                        	releaseReadLock(ring);
//    	                        	if (messageCount>0) {
//    	                        		headPosCache = spinBlockOnHead(headPosCache, tailPosition(ring)+granularity, ring);	                        	    	                        		
//    	                        	}
//    	                        }	                        
//    	                    }                    
//                }                
//            };
//            
//            
//            exService.submit(reader);//this reader starts running immediately
//                       
//            
//            int messageCount = totalMesssages;            
//            //keep local copy of the last time the tail was checked to avoid contention.
//            long tailPosCache = spinBlockOnTail(tailPosition(ring), headPosition(ring)-fill, ring);                        
//            while (--messageCount>=0) {
//                //write the record
//                int messageFieldCount = totalMessageFields;
//                while (--messageFieldCount>=0) {
//                	addByteArray(testArray, 0, testArray.length, ring);
//                }
//                if (0==(messageCount&chunkMask) ) {
//                    publishWrites(ring);
//                    //wait for room to fit one message
//                    //waiting on the tailPosition to move the others are constant for this scope.
//                    //workingHeadPositoin is same or greater than headPosition
//                    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(ring)-fill, ring);
//                }
//                                
//            }
//            //wait until the other thread is finished reading
//            tailPosCache = spinBlockOnTailTillMatchesHead(tailPosCache, ring);
//
//            long duration = System.nanoTime()-start;
//            
//            float milMessagePerSec = 1000f*(totalMesssages/(float)duration);
//            int fieldSize = testArray.length;
//            float milBitsPerSec = milMessagePerSec*totalMessageFields*fieldSize*8;
//            
//            System.out.println("byte transfer test");
//            System.out.println("	million bits per second:"+milBitsPerSec);
//            System.out.println("	million messages per second:"+milMessagePerSec+" duration:"+duration);
//            System.out.println("	million fields per second:"+(totalMessageFields*milMessagePerSec));
//            
//        }
//
//        //clean shutdown of thread executor
//        exService.shutdown();
//        try {
//			exService.awaitTermination(10, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//			//ignore we are exiting
//		}
//    }
	
	 
	
}
