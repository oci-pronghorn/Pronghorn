package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.*;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class RingBufferPipeline {
	
	private final byte[] testArray = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@".getBytes();//, this is a reasonable test message.".getBytes();
	private final int testMessages = 9000000;
	private final int stages = 3;
	private final byte primaryBits   = 19;
	private final byte secondaryBits = 24;//TODO: Warning if this is not big enough it will hang. but not if we fix the split logic.
    
	//The limiting factor for these tests is not the data copy but it is the contention over the hand-off when the head/tail are modified.
	//so by setting matchMask to reduce the calls to publish a dramatic performance increase can be seen.  This will increase latency.
	private final int batchMask = 0xFFF;
	
	@Test
	public void pipelineExample() {		 		 
		 boolean highLevelAPI = true;		 
		
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
			 service.submit(copyStage(rings[j++], rings[j], highLevelAPI));			 
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
		 
		 long bytes = testMessages * (long)testArray.length;
		 long bpms = (bytes*8)/duration;
		 long msgPerMs = testMessages/duration;
		 System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpms/1000000f)+" pipeline "+stages+" msg/ms:"+msgPerMs+" MsgSize:"+testArray.length);
	 		 
	}
	 	
	 
	private Runnable simpleFirstStage(final RingBuffer outputRing, boolean highLevelAPI) {
				
		if (highLevelAPI) {
			return new Runnable() {
				final int MESSAGE_IDX = 0;
				
				@Override
				public void run() {
					try {
						 int messageCount = testMessages;  
	
						 while (--messageCount>=0) {
							
							 RingWalker.blockWriteFragment(outputRing, MESSAGE_IDX);
							
							 //TODO: need field specific method for writing 
							 RingWriter.writeBytes(outputRing, testArray, 0, testArray.length);
								 
							 if (0==(batchMask&messageCount)) {
								 publishWrites(outputRing);
							 }
						 }
						 
						 addNullByteArray(outputRing);
				      	 publishWrites(outputRing); //must publish the posion or it just sits here and everyone down stream hangs
				      	 System.out.println("finished writing:"+testMessages);
					} catch (Throwable t) {
						t.printStackTrace();
					}
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
	                  
				 	  if (0==(batchMask&messageCount)) {
						 publishWrites(outputRing);
				 	  }
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
	private Runnable copyStage(final RingBuffer inputRing, final RingBuffer outputRing, boolean highLevelAPI) {

		if (highLevelAPI) {
			return new Runnable() {
				
				final int MSG_ID = 0;
				final int FIELD_ID = 0;
				
				@Override
				public void run() {
					try {					
						int length = 0;
						do {
							
							if (RingWalker.tryReadFragment(inputRing)) {
								assert(RingWalker.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
								
								
								//wait until the target ring has room for this message
								if (RingWalker.tryWriteFragment(outputRing, MSG_ID)) {
									
									//copy this message from one ring to the next
									//NOTE: in the normal world I would expect the data to be modified before getting moved.
									length = RingReader.copyBytes(inputRing, outputRing, FIELD_ID);							
															
				//					System.err.println(RingBuffer.headPosition(outputRing)+" length "+length);
									
									//release the data from the input ring we are done with this message
									releaseReadLock(inputRing);  
									
									//publish the new message to the next ring buffer
									publishWrites(outputRing);
								} else {
									Thread.yield();//do something meaningful while we wait for space to write our new data
								}
							} else {
								Thread.yield();//do something meaningful while we wait for new data
							}
							//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
						} while (length!=-1);
						
				      	float latencyAt50th = RingBuffer.responseTime(inputRing)/1000000f;//convert ns down to ms
				      	System.out.println("Latency for input to copy stage: "+latencyAt50th+"ms");
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			};
			
		} else {
				
			return new Runnable() {
	
				@Override
				public void run() {
	                //only enter this block when we know there are records to read
	    		    long inputTarget = 2;
	                long headPosCache = spinBlockOnHead(headPosition(inputRing), inputTarget, inputRing);	
	                int msgCount = testMessages;   
	                
	                //two per message, and we only want half the buffer to be full
	                long outputTarget = 2-(1<<primaryBits);//this value is negative
	                
	                long tailPosCache = spinBlockOnTail(tailPosition(outputRing), outputTarget, outputRing);
	                int mask = byteMask(outputRing); // data often loops around end of array so this mask is required
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
	
						RingBuffer.addByteArrayWithMask(outputRing, mask, len, data, offset);						
						outputTarget+=2;
						
						 if (0==(batchMask& --msgCount)) {
							 publishWrites(outputRing);
						 }
	                    
	                    releaseReadLock(inputRing);  
	
	                	
	                	//block until one more byteVector is ready.
	                	inputTarget += 2;
	                	headPosCache = spinBlockOnHead(headPosCache, inputTarget, inputRing);	                        	    	                        		
	                    
	                }  
	               // assertEquals(0,msgCount);
				}
			};
		}
	}
	
	private Runnable dumpStage(final RingBuffer inputRing) {
		
		return new Runnable() {
			
            @Override
            public void run() {      
            	try{
             	    long total = 0;
    	            	
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
            	} catch (Throwable t) {
            		t.printStackTrace();
            	}
            }                
        };
	}

	
	 
	
}
