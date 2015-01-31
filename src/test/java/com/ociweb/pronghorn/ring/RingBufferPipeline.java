package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.*;
import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ociweb.pronghorn.ring.route.SplitterStage;

public class RingBufferPipeline {
	
	private static final String testString = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@@";
	//using length of 61 because it is prime and will wrap at odd places
	private final byte[] testArray = testString.getBytes();//, this is a reasonable test message.".getBytes();
	private final int testMessages = 10000000;
	private final int stages = 4;
	private final byte primaryBits   = 16;
	private final byte secondaryBits = 21;//TODO: Warning if this is not big enough it will hang. but not if we fix the split logic.
    
	private final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	//The limiting factor for these tests is not the data copy but it is the contention over the hand-off when the head/tail are modified.
	//so by setting matchMask to reduce the calls to publish a dramatic performance increase can be seen.  This will increase latency.
	private final int batchMask = 0xFF;
	
	@Test
	public void pipelineExampleHighLevelTaps() {	
		
		pipelineTest(true, true);
		
	}
	
	
	@Test
	public void pipelineExampleLowLevelTaps() {	
		
		pipelineTest(false, true);
		
	}

	@Test
	public void pipelineExampleHighLevel() {	
	
		 pipelineTest(true, false);
	 		 
	}

	//TODO: Test must validate data
	//TODO: dup stage must dup and have x conusmers test.
	
	@Test
	public void pipelineExampleLowLevel() {	
		
		 pipelineTest(false, false);
	 		 
	}
	

	private void pipelineTest(boolean highLevelAPI, boolean useTaps) {
	
		 //should be args
		 boolean monitor = false;
		 
		 int stagesBetweenSourceAndSink = stages -2;
		 
		 //                          monitor dumpers           taps are all daemon 
		 int daemonThreads = (monitor ? (stages-1) : 0) + (useTaps ? stagesBetweenSourceAndSink : 0);
		 int schcheduledThreads = 1;
		 int normalThreads =    2/* source and sink*/   + (useTaps ? 0 : stagesBetweenSourceAndSink);
		 
		 //build all 3 executors
		 ScheduledThreadPoolExecutor scheduledService = new ScheduledThreadPoolExecutor(schcheduledThreads, daemonThreadFactory());
		 ExecutorService daemonService = daemonThreads<=0 ? null : Executors.newFixedThreadPool(daemonThreads, daemonThreadFactory());
		 ExecutorService normalService = Executors.newFixedThreadPool(normalThreads);
		 
		 
		 //build all the rings
		 int j = stages-1;
		 RingBuffer[] rings = new RingBuffer[j];
		 
		 RingBufferMonitorStage[] monitorStages = null;
		 RingBuffer[] monitorRings = null;
		 FieldReferenceOffsetManager montorFROM = null;
		 if (monitor) {
			monitorStages = new RingBufferMonitorStage[j];
		 	monitorRings = new RingBuffer[j];
		 	montorFROM = RingBufferMonitorStage.buildFROM();
		 }
		 
		 while (--j>=0)  {
			 rings[j] = new RingBuffer(primaryBits, secondaryBits);
			 
			 //test by starting at different location in the ring to force roll over.
			 rings[j].reset(rings[j].maxSize-13,rings[j].maxByteSize-101);
	  		 
			 if (monitor) {
				 monitorRings[j] = new RingBuffer((byte)16,(byte)2,null,montorFROM);
				 monitorStages[j] = new RingBufferMonitorStage(rings[j], monitorRings[j]);	
				 scheduledService.scheduleAtFixedRate(monitorStages[j], j, 40, TimeUnit.MILLISECONDS);			 
				 daemonService.submit(dumpMonitor(monitorRings[j]));
			 }
		 }
		 
		 
		 
		 //start the timer		 
		 long start = System.currentTimeMillis();
		 
		 //add all the stages start running
		 
		 j = 0;
		 normalService.submit(simpleFirstStage(rings[j], highLevelAPI));
		 int i = stagesBetweenSourceAndSink;
		 while (--i>=0) {
			 if (useTaps) { //TODO: this has a hack to detect the shut down but we need a much better design. this split is 3x faster than per record copy
				 daemonService.submit(new SplitterStage(rings[j++], rings[j]));
			 } else {
			 
				 normalService.submit(copyStage(rings[j++], rings[j], highLevelAPI));		
			 }
			 
		 }
		 normalService.submit(dumpStage(rings[j], highLevelAPI));
		 
		 System.out.println();
		 System.out.println("                        testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTaps? "using taps":""));
		 
		 //prevents any new jobs from getting submitted
		 normalService.shutdown();
		 //blocks until all the submitted runnables have stopped
		 try {
			normalService.awaitTermination(10, TimeUnit.MINUTES);
		 } catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		 }
		 
		 //TODO: should we flush all the monitoring?
		 
		 
		 long duration = System.currentTimeMillis()-start;
		 
		 long bytes = testMessages * (long)testArray.length;
		 long bpms = (bytes*8)/duration;
		 long msgPerMs = testMessages/duration;
		 System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpms/1000000f)+" stages:"+stages+" msg/ms:"+msgPerMs+" MsgSize:"+testArray.length);
	}

	private ThreadFactory daemonThreadFactory() {
		return new ThreadFactory(){

			@Override
			public Thread newThread(Runnable r) {
				Thread t= new Thread(r);
				t.setDaemon(true);
				return t;
			}};
	}
	 	

	private Runnable simpleFirstStage(final RingBuffer outputRing, boolean highLevelAPI) {
				
				
		if (highLevelAPI) {
			return new Runnable() {
				final int MESSAGE_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;	
				final int FIELD_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
				 
				@Override
				public void run() {
					try {
						 int messageCount = testMessages; 
						 RingWalker.setPublishBatchSize(outputRing, batchMask);
	
						 while (--messageCount>=0) {
							
							 RingWalker.blockWriteFragment(outputRing, MESSAGE_LOC);
							 RingWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length);
							 RingWalker.publishWrites(outputRing);

						 }
						 RingWalker.blockingFlush(outputRing);
						 
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
									
				  int messageSize = msgSize;
			      int fill =  (1<<primaryBits)-messageSize;
			      
		          int messageCount = testMessages;            
		          //keep local copy of the last time the tail was checked to avoid contention.
		          long head = headPosition(outputRing) - fill;
		          long tailPosCache = spinBlockOnTail(tailPosition(outputRing), head, outputRing);                        
		          while (--messageCount>=0) {
		        	  
		              //write the record
		        	  RingBuffer.addMsgIdx(outputRing, 0);
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
		          RingBuffer.addMsgIdx(outputRing, -1);
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
				
				final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
				final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
				
				@Override
				public void run() {
					try {			
			//			RingWalker.setReleaseBatchSize(inputRing, 8);
						RingWalker.setPublishBatchSize(outputRing, 64);
						
						int msgId = 0;
						do {
							if (RingWalker.tryReadFragment(inputRing)) {
								assert(RingWalker.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
								msgId = RingWalker.getMsgIdx(inputRing);
																
								//wait until the target ring has room for this message
								if (0==msgId && RingWalker.tryWriteFragment(outputRing, MSG_ID)) {
																		
									//copy this message from one ring to the next
									//NOTE: in the normal world I would expect the data to be modified before getting moved.
									RingReader.copyBytes(inputRing, outputRing, FIELD_ID);							

									RingWalker.publishWrites(outputRing);
										
								} else {
									Thread.yield();//do something meaningful while we wait for space to write our new data
								}
							} else {
								Thread.yield();//do something meaningful while we wait for new data
							}
							//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
						} while (msgId!=-1);
												
						RingBuffer.releaseReadLock(inputRing); //release all slots back to producer (not strictly needed as we are exiting)						
						RingWalker.blockingFlush(outputRing);
						

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
	    		    long inputTarget = tailPosition(inputRing) + msgSize;
	                long headPosCache = spinBlockOnHead(headPosition(inputRing), inputTarget, inputRing);	
	                int msgCount = testMessages;   
	                
	                //two per message, and we only want half the buffer to be full
	                long tailPosition = tailPosition(outputRing);
	                long outputTarget = tailPosition + msgSize-(1<<primaryBits);//this value is negative TODO: this target will be a problem for var lenght messages!!
	                
					long tailPosCache = spinBlockOnTail(tailPosition, outputTarget, outputRing);
	                int mask = byteMask(outputRing); // data often loops around end of array so this mask is required
	                while (true) {
	                    //read the message
	                    // 	System.out.println("reading:"+messageCount);
	                    int msgId = RingBuffer.takeValue(inputRing);	
	                	int meta = takeRingByteMetaData(inputRing);
	                	int len = takeRingByteLen(inputRing);
	                	
	                	byte[] data = byteBackingArray(meta, inputRing);
	                	int offset = bytePosition(meta, inputRing, len);
	
	                	tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, outputRing);
	                	 //write the record
	
						
						if (len<0) {
							releaseReadLock(inputRing); 
							RingBuffer.addMsgIdx(outputRing, -1);
							addNullByteArray(outputRing);
							publishWrites(outputRing);
							return;
						}
	
						RingBuffer.addMsgIdx(outputRing, 0);
						RingBuffer.addByteArrayWithMask(outputRing, mask, len, data, offset);						
						outputTarget+=msgSize;
						
						 if (0==(batchMask& --msgCount)) {
							//publish the new messages to the next ring buffer in batches
							 publishWrites(outputRing);
							 releaseReadLock(inputRing);
						 }
  	                	
	                	//block until one more byteVector is ready.
	                	inputTarget += msgSize;
	                	headPosCache = spinBlockOnHead(headPosCache, inputTarget, inputRing);	                        	    	                        		
	                    
	                }  
	             //   assertEquals(0,msgCount);
				}
			};
		}
	}
	
	private Runnable dumpStage(final RingBuffer inputRing, boolean highLevelAPI) {
		if (highLevelAPI) {
			return new Runnable() {
				
				final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
				final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
				
	            @Override
	            public void run() {      
	            	try{
			//			RingWalker.setReleaseBatchSize(inputRing, 32);
						
	            		int lastPos = -1;
						int msgId = 0;
						do {
							
							if (RingWalker.tryReadFragment(inputRing)) {
								assert(RingWalker.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
								msgId = RingWalker.getMsgIdx(inputRing);
								
								if (msgId>=0) {
									//check the data
									int len = RingReader.readBytesLength(inputRing, FIELD_ID);
									assertEquals(testArray.length,len);
									
									//test that pos moves as expected
									int pos = RingReader.readBytesPosition(inputRing, FIELD_ID);
//									if (lastPos>=0) {
//										assertEquals((lastPos+len)&inputRing.byteMask,pos);
//									} 
//									lastPos = pos;
									
									byte[] dat = RingReader.readBytesBackingArray(inputRing, FIELD_ID);
									
//									assertTrue("\nexpected:\n"+testString+"\nfound:\n"+RingReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString(),
//											    RingReader.eqASCII(inputRing, FIELD_ID, testString));
//									

								}
							} else {
								Thread.yield();//do something meaningful while we wait for new data
							}
							//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
						} while (msgId!=-1);
	            		
						releaseReadLock(inputRing);
				      	
	            	} catch (Throwable t) {
	            		RingBuffer.shutDown(inputRing);
	            		t.printStackTrace();
	            	}
	            }                
	        };
		} else {
			return new Runnable() {
				
	            @Override
	            public void run() {      
	            	try{
	             	    long total = 0;
	    	            	
	                    //only enter this block when we know there are records to read
	        		    long target = msgSize+tailPosition(inputRing);
	                    long headPosCache = spinBlockOnHead(headPosition(inputRing), target, inputRing);	
	                    long messageCount = 0;
	                    while (true) {
	                        //read the message
	                        // 	System.out.println("reading:"+messageCount);
	                        int msgId = RingBuffer.takeValue(inputRing); 	
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
	                    	target += msgSize;
	                    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		
	                        
	                    }   
	            	} catch (Throwable t) {
	            		t.printStackTrace();
	            	}
	            }                
	        };
		}
	}

	public Runnable dumpMonitor(final RingBuffer inputRing) {
		return new Runnable() {
			
            @Override
            public void run() {      
            	try{
            		int monitorMessageSize = RingBuffer.from(inputRing).fragDataSize[0];
            		
            		
                    //only enter this block when we know there are records to read
        		    long target = monitorMessageSize;
                    long headPosCache = spinBlockOnHead(headPosition(inputRing), target, inputRing);	
                    long messageCount = 0;
                    while (true) {
                        //read the message
                        int msgId = RingBuffer.readValue(0, inputRing.buffer,inputRing.mask,inputRing.workingTailPos.value); 
                        
                        if (msgId<0) {   
                        	System.out.println("exited after reading: " + messageCount+" monitor samples");
                        	return;
                        }
                        
                        long time = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_TIME_LOC);
                        long head = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_HEAD_LOC);
                        long tail = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_TAIL_LOC);
                        int tmpId = RingReader.readInt(inputRing, RingBufferMonitorStage.TEMPLATE_MSG_LOC);
                        
                        inputRing.workingTailPos.value+=monitorMessageSize;
                                 
                        int queueDepth = (int)(head-tail);
                        //vs what?
                        
                        
                       // System.err.println(time+"  "+head+"  "+tail+"   "+tmpId);
    					
                    	//doing nothing with the data
   						releaseReadLock(inputRing);

                    	
                    	messageCount++;
                    	
                    	//block until one more byteVector is ready.
                    	target += monitorMessageSize;
                    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		
                        
                    }  
            		
            
			      	
            	} catch (Throwable t) {
            		//just exit.
            		//t.printStackTrace();
            	}
            }                
        };
	}
	
	 
	
}
