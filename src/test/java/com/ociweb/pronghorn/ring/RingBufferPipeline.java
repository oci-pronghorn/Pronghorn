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
	
	private static final String testString1 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@@";
	private static final String testString = testString1+testString1;//+testString1;
	//using length of 61 because it is prime and will wrap at odd places
	private final byte[] testArray = testString.getBytes();//, this is a reasonable test message.".getBytes();
	private final int testMessages = 40000;//100000;//10000000; //TODO: AA, must bump this out one more after the byte var rel pos logic is fixed.
	private final int stages = 3;
	private final byte primaryBits   = 17; 
	private final byte secondaryBits = 24;//TODO: Warning if this is not big enough it will hang. but not if we fix the split logic.
    
	private final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	//The limiting factor for these tests is not the data copy but it is the contention over the hand-off when the head/tail are modified.
	//so by setting matchMask to reduce the calls to publish a dramatic performance increase can be seen.  This will increase latency.
	private final int batchMask = 0xFF;
	
	@Test
	public void pipelineExampleHighLevelTaps() {			
		pipelineTest(true, true, false);		
	}
	
	@Test
	public void pipelineExampleLowLevelTaps() {			
		pipelineTest(false, true, false);		
	}

	@Test
	public void pipelineExampleHighLevel() {		
		 pipelineTest(true, false, false);	 		 
	}

	@Test
	public void pipelineExampleLowLevel() {			
		 pipelineTest(false, false, false);	 		 
	}
	
	@Test
	public void pipelineExampleHighLevelTapsWithMonitor() {			
		pipelineTest(true, true, true);		
	}
	
	@Test
	public void pipelineExampleLowLevelTapsWithMonitor() {			
		pipelineTest(false, true, true);		
	}

	@Test
	public void pipelineExampleHighLevelWithMonitor() {		
		 pipelineTest(true, false, true);	 		 
	}

	@Test
	public void pipelineExampleLowLevelWithMonitor() {			
		 pipelineTest(false, false, true);	 		 
	}
	
	
	
	//TODO: Test must validate data
	//TODO: dup stage must dup and have x conusmers test.
	

	private void pipelineTest(boolean highLevelAPI, boolean useTaps, boolean monitor) {
			 
		 assertEquals("For "+FieldReferenceOffsetManager.RAW_BYTES.name+" expected no need to add field.",
				      0,FieldReferenceOffsetManager.RAW_BYTES.fragNeedsAppendedCountOfBytesConsumed[0]);
			
		
		 int stagesBetweenSourceAndSink = stages -2;
		 final int splits = 4;
		 
		 int daemonThreads = (useTaps ? stagesBetweenSourceAndSink : 0);
		 int schcheduledThreads = 2;
		
		 int normalThreads =    2/* source and sink*/   + (useTaps ? splits-1 : stagesBetweenSourceAndSink);
		 int totalThreads = daemonThreads+schcheduledThreads+normalThreads;
		 
//		 if (totalThreads > Runtime.getRuntime().availableProcessors()) {
//			 System.err.println("test skipped on this hardware, needs "+totalThreads+" cores.");
//			 return;
//		 }
		 
		 
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
			 assertEquals("For "+rings[j].consumerData.from.name+" expected no need to add field.",0,rings[j].consumerData.from.fragNeedsAppendedCountOfBytesConsumed[0]);
		
			 
			 //test by starting at different location in the ring to force roll over.
			 rings[j].reset(rings[j].maxSize-13,rings[j].maxByteSize-101);
	  		 
			 if (monitor) {
				 monitorRings[j] = new RingBuffer((byte)16,(byte)2,null,montorFROM);
				 //assertTrue(mo)
				 monitorStages[j] = new RingBufferMonitorStage(rings[j], monitorRings[j]);	
				 
				 //this is a bit complex may be better to move this inside on thread?
				 scheduledService.scheduleAtFixedRate(monitorStages[j], j*5, 40, TimeUnit.MILLISECONDS);	
				 scheduledService.scheduleAtFixedRate(dumpMonitor(monitorRings[j]), j*10, 40, TimeUnit.MILLISECONDS);	
			 }
		 }
		 		 
		 
		 //start the timer		 
		 long start = System.currentTimeMillis();
		 
		 //add all the stages start running
		 
		 j = 0;
		 normalService.submit(simpleFirstStage(rings[j], highLevelAPI));
		 int i = stagesBetweenSourceAndSink;
		 while (--i>=0) {
			 if (useTaps) {
				 					 
				 RingBuffer[] splitsBuffers = new RingBuffer[splits];
				 splitsBuffers[0] = rings[j+1];//must jump ahead because we are setting this early
				 if (splits>1) {
					 int k = splits;
					 while (--k>0) {
						 splitsBuffers[k] = new RingBuffer(primaryBits, secondaryBits);
						 ///
						 normalService.submit(dumpStage(splitsBuffers[k], highLevelAPI));
					 }
				 } 
				 daemonService.submit(new SplitterStage(rings[j++], splitsBuffers));
			     //scheduledService.scheduleAtFixedRate(new SplitterStage(rings[j++], splitsBuffers), 1, 5, TimeUnit.MICROSECONDS);
				 
			 } else {			 
				 normalService.submit(copyStage(rings[j++], rings[j], highLevelAPI));		
			 }
			 
		 }
		 normalService.submit(dumpStage(rings[j], highLevelAPI));
		 
		 System.out.println("########################################################## Testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTaps? "using "+splits+" taps ":"")+(monitor?"monitored":"")+" totalThreads:"+totalThreads);
		 
		 
		 
		 //prevents any new jobs from getting submitted
		 normalService.shutdown();
		// System.err.println("waiting for finish");
		 //blocks until all the submitted runnables have stopped
		 try {
			normalService.awaitTermination(20, TimeUnit.SECONDS);
		 } catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		 }
		 
		 //TODO: should we flush all the monitoring?
		 //System.err.println("waiting for finish dddddddd");
		 
		 
		 int t = rings.length;
		 while (--t>=0) {
			 assertFalse("Unexpected error in thread, see console output",RingBuffer.isShutdown(rings[t]));
		 }
		 if (monitor) {
			 t = monitorRings.length;
			 while (--t>=0) {
				 assertFalse("Unexpected error in thread, see console output",RingBuffer.isShutdown(monitorRings[t]));
			 }
		 }
		 
		 
		 long duration = System.currentTimeMillis()-start;
		 
		 long bytes = testMessages * (long)testArray.length;
		 long bpms = (bytes*8)/duration;
		 long msgPerMs = testMessages/duration;
		 System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpms/1000000f)+" stages:"+stages+" msg/ms:"+msgPerMs+" MsgSize:"+testArray.length);
		 
		 if (daemonThreads>0) {
			 daemonService.shutdownNow();
		 	daemonService=null;
		 }
		 scheduledService.shutdownNow();
		 scheduledService=null;
		 
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
						 boolean debug = false;
						 if (debug) {		 
							 System.out.println("finished writing:"+testMessages);
						 }
					} catch (Throwable t) {
						RingBuffer.shutdown(outputRing);
						t.printStackTrace();
					}
				}
			};		
		} else {
			return new Runnable() {
	
				@Override
				public void run() {
					try{
									
					  int messageSize = msgSize;
				      int fill =  (1<<primaryBits)-messageSize;
				      
				      
			          int messageCount = testMessages;            
			          //keep local copy of the last time the tail was checked to avoid contention.
			          long head = headPosition(outputRing) - fill;
			          long tailPosCache = tailPosition(outputRing);                        
			          while (--messageCount>=0) {
			        	  
			        	  tailPosCache = spinBlockOnTail(tailPosCache, head, outputRing);
			        	  			        	  
			              //write the record
			        	  RingBuffer.addMsgIdx(outputRing, 0);
		                  
			        	  addByteArray(testArray, 0, testArray.length, outputRing);
		                  
		               //TODO: AAAAAAA why is this needed here, THIS IS FIXING SMOE CASE ON THE ROLLOVER THAT IS FAILING. IN THE SPLITTER?
			        	  //need something to fix the example file read/write   
		//		        	  outputRing.bytesHeadPos.lazySet(outputRing.byteWorkingHeadPos.value); 

			        	 
			        	  
			        	  //WE have published teh next working head to read from but the OLD bytes position.
							 publishWrite(outputRing);

							 
							 
					//		 outputRing.bytesHeadPos.lazySet(outputRing.byteWorkingHeadPos.value); 
					 	 // }
							 
		                  head += messageSize;		          
			          }
			          
			          tailPosCache = spinBlockOnTail(tailPosCache, head, outputRing);
			         // RingWalker.blockingFlush(outputRing);
			          
			          //send negative length as poison pill to exit all runnables  
			          RingBuffer.addMsgIdx(outputRing, -1);
			      	  addNullByteArray(outputRing);
			      	  
			 //     	  outputRing.bytesHeadPos.lazySet(outputRing.byteWorkingHeadPos.value);
			      	  publishWrite(outputRing); //must publish the posion or it just sits here and everyone down stream hangs
			      	
						 boolean debug = false;
						 if (debug) {		 
							 System.out.println("finished writing:"+testMessages);
						 }
					} catch (Throwable t) {
						t.printStackTrace();
						RingBuffer.shutdown(outputRing);
					}
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
			//			RingWalker.setPublishBatchSize(outputRing, 64);
						
						int msgId = 0;
						do {
							if (RingWalker.tryReadFragment(inputRing)) { 
								assert(RingWalker.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
								msgId = RingWalker.getMsgIdx(inputRing);
								
								
								//wait until the target ring has room for this message
								if (0==msgId) {
//									String fromString = RingReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString();
//									if (!fromString.equals(testString)) {
//										System.err.println(fromString);
//										System.err.println(testString);
//										
//										System.exit(-1);
//									}
									
									RingWalker.blockWriteFragment(outputRing,MSG_ID);
									//copy this message from one ring to the next
									//NOTE: in the normal world I would expect the data to be modified before getting moved.
									int len = RingReader.copyBytes(inputRing, outputRing, FIELD_ID);							
									
									//fromString = RingReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString();
								//	RingWriter.writeASCII(outputRing, FIELD_ID, fromString);
									
									

									RingWalker.publishWrites(outputRing);
										
									if (!FieldReferenceOffsetManager.USE_VAR_COUNT) {
										//using the low level seems like this is required
										inputRing.byteWorkingTailPos.value+=len;
									}
								} 
							} else {
								Thread.yield();//do something meaningful while we wait for new data
							}
							//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
						} while (msgId!=-1);
																	
						RingWalker.blockingFlush(outputRing);
						

					} catch (Throwable t) {
						RingBuffer.shutdown(inputRing);
						RingBuffer.shutdown(outputRing);
						t.printStackTrace();
					}
				}
			};
			
		} else {
				
			return new Runnable() {
	
				@Override
				public void run() {
					try{
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
		                	
		                	
//	                    	if (FieldReferenceOffsetManager.USE_VAR_COUNT) {
//	                    		//using the low level seems like this is required
//	                    		if (len>=0) {
//	                    			inputRing.byteWorkingTailPos.value+=len;
//	                    		}
//	                    	}
//	                    	
		                	
		                	int offset = bytePosition(meta, inputRing, len);
		                			
									
		
							
							if (len<0) {
								releaseMessageReadLock(inputRing); 
								RingBuffer.addMsgIdx(outputRing, -1);
								addNullByteArray(outputRing);
								publishWrite(outputRing);
								return;
							}
		
							RingBuffer.addMsgIdx(outputRing, 0);
						//	System.err.println(" pos :"+offset+" len :"+len);
							RingBuffer.addByteArrayWithMask(outputRing, mask, len, data, offset);	
							
							
							
							
							tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, outputRing);
							//write the record
							outputTarget+=msgSize;
							
							// if (0==(batchMask& --msgCount)) {
								//publish the new messages to the next ring buffer in batches
								 publishWrite(outputRing);
								 releaseMessageReadLock(inputRing);
							// }
	  	                	
		                	//block until one more byteVector is ready.
		                	inputTarget += msgSize;
		                	headPosCache = spinBlockOnHead(headPosCache, inputTarget, inputRing);	                        	    	                        		
		                    
		                }  
					} catch (Throwable t) {
						RingBuffer.shutdown(inputRing);
						RingBuffer.shutdown(outputRing);
						t.printStackTrace();
					}
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
							
							//try also releases previously read fragments
							if (RingWalker.tryReadFragment(inputRing)) {
														
								
								assert(RingWalker.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
								msgId = RingWalker.getMsgIdx(inputRing);
								
								if (msgId>=0) {																	
									
									//check the data
									int len = RingReader.readBytesLength(inputRing, FIELD_ID);
									assertEquals(testArray.length,len);
									
//									int tail = inputRing.bytesTailPos.get();
//									System.err.println("tail:"+tail);
									
									//test that pos moves as expected
									int pos = RingReader.readBytesPosition(inputRing, FIELD_ID);
									if (lastPos>=0) {
										assertEquals((lastPos+len)&inputRing.byteMask, pos&inputRing.byteMask);
									} 
									lastPos = pos;
																											
									//This block causes a dramatic slow down of the work!!
									if (!RingReader.eqASCII(inputRing, FIELD_ID, testString)) {
										fail("\nexpected:\n"+testString+"\nfound:\n"+RingReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString() );
									}
									
									if (!FieldReferenceOffsetManager.USE_VAR_COUNT) {
											//using the low level seems like this is required
											inputRing.byteWorkingTailPos.value+=len;
									}

								}
							} else {
								Thread.yield();//do something meaningful while we wait for new data
							}
							
							//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
						} while (msgId!=-1);
						
				      	
	            	} catch (Throwable t) {
	            		RingBuffer.shutdown(inputRing);
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
	             	    int lastPos = -1;
	             	   
	                    //only enter this block when we know there are records to read
	        		    long target = 1+tailPosition(inputRing);
	                    long headPosCache = headPosition(inputRing);
	                    long messageCount = 0;
	                    while (true) {
	                        //read the message
	                    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	  

	                        int msgId = RingBuffer.takeValue(inputRing); 	
	                        if (msgId<0) {                     	
	                        	boolean debug = false;
	                        	if (debug) {
	                        		System.out.println("exited after reading: Msg:" + messageCount+" Bytes:"+total);
	                        	}
	                        	return;
	                        }
	                    	int meta = takeRingByteMetaData(inputRing);
	                    	int len = takeRingByteLen(inputRing);
	                    	assertEquals(testArray.length,len);

	                    	int pos = bytePosition(meta, inputRing, len);//has side effect of moving the byte pointer!!
	                    	
							if (lastPos>=0) {
								assertEquals((lastPos+len)&inputRing.byteMask,pos&inputRing.byteMask);
							} 
							lastPos = pos;
								
	    					byte[] data = byteBackingArray(meta, inputRing);
	    					int mask = byteMask(inputRing);
	   					
	    					
	    					//This block causes a dramatic slow down of the work!!
	    					int i = len;
	    					while (--i>=0) {
	    						if (testArray[i]!=data[(pos+i)&mask]) {
	    							fail("String does not match at index "+i+" of "+len);
	    						}
	    					}
	    					
	                    	//doing nothing with the data
	   						releaseMessageReadLock(inputRing);
	
	                    	
	                    	messageCount++;
	                    	
	                    	total += len;
	
	                    	//block until one more byteVector is ready.
	                    	target += msgSize;
	                    }   
	            	} catch (Throwable t) {
	            		RingBuffer.shutdown(inputRing);
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
               //     while (true) 
                    {
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
                        
                        
                    //TODO: AAA there is no text field used here so the last value will always be zero and we need to pull it.    
                        
                        inputRing.workingTailPos.value+=monitorMessageSize;
                                 
                        int queueDepth = (int)(head-tail);
                        //vs what?
                        
                        
                       // System.err.println(time+"  "+head+"  "+tail+"   "+tmpId);
    					
                    	//doing nothing with the data
   						releaseMessageReadLock(inputRing);

                    	
                    	messageCount++;
                    	
                    	//block until one more byteVector is ready.
                    	target += monitorMessageSize;
                    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		
                        
                    }  
            		
            
			      	
            	} catch (Throwable t) {
            		//RingBuffer.shutDown(inputRing);
            		//t.printStackTrace();
            	}
            }                
        };
	}
	
	 
	
}
