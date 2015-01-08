package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class RingBufferPipeline {

	
	private final byte[] testArray = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@".getBytes();//, this is a reasonable test message.".getBytes();
	private final int testMessages = 9000000;
	private final int stages = 3;
	private final byte primaryBits   = 19;
	private final byte secondaryBits = 27;//TODO: Warning if this is not big enough it will hang. but not if we fix the split logic.
    
	@Test
	public void pipelineExample() {		 		 
		 boolean highLevelAPI = false;		 
		
		 //create all the threads, one for each stage
		 ExecutorService service = Executors.newFixedThreadPool(stages);
		 
		 //build all the rings
		 int j = stages-1;
		 RingBuffer[] rings = new RingBuffer[j];
		 while (--j>=0)  {
			 rings[j] = new RingBuffer(primaryBits,secondaryBits);
		 }
		 
		 //start the timer		 
		 long start = System.currentTimeMillis();
		 
		 //add all the stages start running
		 j = 0;
		 service.submit(simpleFirstStage(rings[j], highLevelAPI));
		 int i = stages-2;
		 while (--i>=0) {
			 service.submit(copyStage(rings[j++], rings[j]));			 
		 }
		 service.submit(dumpStage(rings[j]));
		 
		 //prevents any new jobs from getting submitted
		 service.shutdown();
		 //blocks until all the submitted runnables have stopped
		 try {
			service.awaitTermination(10, TimeUnit.MINUTES);
		 } catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		 }
		 long duration = System.currentTimeMillis()-start;
		 
		 long bytes = testMessages * testArray.length;
		 long bpms = (bytes*8)/duration;
		 System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpms/1000000f)+" pipeline "+stages);
		 
		 		 
	}

	 	
	 
	private Runnable simpleFirstStage(final RingBuffer outputRing, boolean highLevelAPI) {
		
		
		if (highLevelAPI) {
			return new Runnable() {
				final int MESSAGE_IDX = 0;
				
				@Override
				public void run() {
					 int messageCount = testMessages;  
					 while (--messageCount>=0) {
						 RingWalker.blockWriteFragment(outputRing, MESSAGE_IDX);
						 RingWriter.writeBytes(outputRing, testArray);
		                 publishWrites(outputRing);
					 }
					 addNullByteArray(outputRing);
			      	 publishWrites(outputRing); //must publish the posion or it just sits here and everyone down stream hangs
			      	 System.out.println("finished writing:"+testMessages);
				}
			};		
		} else {
			return new Runnable() {
	
				@Override
				public void run() {
									
				  int messageSize = 2;	
			      int fill =  (1<<primaryBits)-messageSize;
			      
		          int messageCount = testMessages;            
		          //keep local copy of the last time the tail was checked to avoid contention.
		          long head = -fill;
		          long tailPosCache = spinBlockOnTail(tailPosition(outputRing), head, outputRing);                        
		          while (--messageCount>=0) {
		        	  
		              //write the record
	                  addByteArray(testArray, 0, testArray.length, outputRing);
	                  
	                  publishWrites(outputRing);
	                  head +=messageSize;
	                  //wait for room to fit one message
	                  //waiting on the tailPosition to move the others are constant for this scope.
	                  //workingHeadPositoin is same or greater than headPosition
	                  tailPosCache = spinBlockOnTail(tailPosCache, head, outputRing);
	          
		          }
	
		          //send negative length as poison pill to exit all runnables  
		      	  addNullByteArray(outputRing);
		      	  publishWrites(outputRing); //must publish the posion or it just sits here and everyone down stream hangs
		      	  System.out.println("finished writing:"+testMessages);
				}
			};
		}
	}

	//NOTE: this is an example of a stage that reads from one ring buffer and writes to another.
	private Runnable copyStage(final RingBuffer inputRing, final RingBuffer outputRing) {
		
		return new Runnable() {

			@Override
			public void run() {
                //only enter this block when we know there are records to read
    		    long inputTarget = 2;
                long headPosCache = spinBlockOnHead(headPosition(inputRing), inputTarget, inputRing);	
                                
                //two per message, and we only want half the buffer to be full
                long outputTarget = 2-(1<<primaryBits);//this value is negative
                
                int mask = byteMask(outputRing); // data often loops around end of array so this mask is required
                long tailPosCache = spinBlockOnTail(tailPosition(outputRing), outputTarget, outputRing);
                while (true) {
                    //read the message
                    // 	System.out.println("reading:"+messageCount);
                    	
                	int meta = takeRingByteMetaData(inputRing);
                	int len = takeRingByteLen(inputRing);
                	
                	byte[] data = byteBackingArray(meta, inputRing);
                	int offset = bytePosition(meta, inputRing, len);

                	tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, outputRing);
                	 //write the record

					
					if (len<0) {
						releaseReadLock(inputRing);  
						addNullByteArray(outputRing);
						publishWrites(outputRing);
						return;
					}

					
					//TODO: there is a more elegant way to do this but ran out of time.
					if ((offset&mask) > ((offset+len-1) & mask)) {
						
						//rolled over the end of the buffer
						 int len1 = 1+mask-(offset&mask);
						 addByteArray(data, offset&mask, len1, outputRing);
						 addByteArray(data, 0, len-len1 ,outputRing);
						 outputTarget+=4; 
						 
					} else {						
						
						//simple add bytes
						 addByteArray(data, offset&mask, len, outputRing);
						 outputTarget+=2;
					}
					
                    publishWrites(outputRing);
                    
                    releaseReadLock(inputRing);  

                	
                	//block until one more byteVector is ready.
                	inputTarget += 2;
                	headPosCache = spinBlockOnHead(headPosCache, inputTarget, inputRing);	                        	    	                        		
                    
                }  
			}
		};
	}
	
	private Runnable dumpStage(final RingBuffer inputRing) {
		
		return new Runnable() {

			long total = 0;
			
            @Override
            public void run() {           	
    	            	
                    //only enter this block when we know there are records to read
        		    long target = 2;
                    long headPosCache = spinBlockOnHead(headPosition(inputRing), target, inputRing);	
                    long messageCount = 0;
                    while (true) {
                        //read the message
                        // 	System.out.println("reading:"+messageCount);
                        	
                    	int meta = takeRingByteMetaData(inputRing);
                    	int len = takeRingByteLen(inputRing);
                    	
    					byte[] data = byteBackingArray(meta, inputRing);
    					int offset = bytePosition(meta, inputRing, len);
    					int mask = byteMask(inputRing);
   					
    					
                    	//doing nothing with the data
                    	releaseReadLock(inputRing);

                    	if (len<0) {
                    		System.out.println("exited after reading: Msg:" + messageCount+" Bytes:"+total);
                    		return;
                    	}
                    	
                    	messageCount++;
                    	
                    	total += len;

                    	//block until one more byteVector is ready.
                    	target += 2;
                    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		
                        
                    }   
                    
            }                
        };
	}

	
	 
	
}
