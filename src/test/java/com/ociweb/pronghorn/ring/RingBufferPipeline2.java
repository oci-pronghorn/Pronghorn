package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.addByteArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.route.RoundRobinRouteStage2;
import com.ociweb.pronghorn.ring.route.SplitterStage2;
import com.ociweb.pronghorn.ring.stage.PronghornStage;
import com.ociweb.pronghorn.ring.threading.StageManager;
//import com.ociweb.pronghorn.ring.util.PipelineThreadPoolExecutor;
import com.ociweb.pronghorn.ring.threading.ThreadPerStageManager;

public class RingBufferPipeline2 {
	
	private final class DumpMonitorStage extends PronghornStage {
		private final RingBuffer inputRing;

		private DumpMonitorStage(GraphManager gm,RingBuffer inputRing) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
		}

		@Override
		public boolean exhaustedPoll() {  

				int monitorMessageSize = RingBuffer.from(inputRing).fragDataSize[0];
								
		        //only enter this block when we know there are records to read
			    long target = monitorMessageSize;
		        long headPosCache = spinBlockOnHead(headPosition(inputRing), target, inputRing);	
		        long messageCount = 0;

		            //read the message
		            int msgId = RingBuffer.readValue(0, inputRing.buffer,inputRing.mask,inputRing.workingTailPos.value); 
		            
		            if (msgId<0) {   
		            	System.out.println("exited after reading: " + messageCount+" monitor samples");
		            	return false;
		            }
		            
		            long time = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_TIME_LOC);
		            long head = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_HEAD_LOC);
		            long tail = RingReader.readLong(inputRing, RingBufferMonitorStage.TEMPLATE_TAIL_LOC);
		            int tmpId = RingReader.readInt(inputRing, RingBufferMonitorStage.TEMPLATE_MSG_LOC);
		            
		            
		        //TODO: AAAAAA this is a mixed high low problem and must be converted to one side or the other
		            //TODO: AAAAA need a monitor object that manages all this information so it does not cultter the businss logic
		            //TODO: AAAAA need method on that object for building up the tree?
		            
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

			return true;
		}
	}

	private final class DumpStageLowLevel extends PronghornStage {
		private final RingBuffer inputRing;
		private final boolean useRoute;

		private DumpStageLowLevel(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
		}

		@Override
		public boolean exhaustedPoll() {  

				RingBuffer.setReleaseBatchSize(inputRing, 8);
				
		 	    long total = 0;
		 	    int lastPos = -1;
		 	   
		        //only enter this block when we know there are records to read
			    long target = 1+tailPosition(inputRing);
		        long headPosCache = headPosition(inputRing);
		        long messageCount = 0;
		        long expectedMessageCount = useRoute?testMessages/splits:testMessages;
		        
		        while (true) {
		        	
		        	if (messageCount==expectedMessageCount) {
		        		//quit early there are no more message coming because this is how many were sent.
		        		//System.err.println("done after "+messageCount+" messages and "+total+" bytes ");
		        		RingBuffer.releaseAll(inputRing);
		               	return false;
		        	}
		        	
		            //read the message
		        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);
		        	
		            int msgId = RingBuffer.takeMsgIdx(inputRing);
		            if (msgId<0) {  
		            	//System.err.println("done after "+messageCount+" messages and "+total+" bytes ");
		            	assertEquals(testMessages,useRoute? messageCount*splits: messageCount);
		            			            	
		            	RingBuffer.releaseAll(inputRing);
						
		            	return false;
		            }
		        	int meta = takeRingByteMetaData(inputRing);
		        	int len = takeRingByteLen(inputRing);
		        	assertEquals(testArray.length,len);

		        	//converting this to the position will cause the byte posistion to increment.
		        	int pos = bytePosition(meta, inputRing, len);//has side effect of moving the byte pointer!!
		        	
					if (lastPos>=0) {
						assertEquals((lastPos+len)&inputRing.byteMask,pos&inputRing.byteMask);
					} 
					lastPos = pos;
											
					byte[] data = byteBackingArray(meta, inputRing);
					int mask = byteMask(inputRing);
				
					if (deepTest) {
						//This block causes a dramatic slow down of the work!!
						int i = testArray.length;
						while (--i>=0) {
							if (testArray[i]==data[(pos+i)&mask]) {		    									
							} else {
								fail("String does not match at index "+i+" of "+len+"   tailPos:"+inputRing.tailPos.get()+" byteFailurePos:"+(pos+i)+" masked "+((pos+i)&mask));
								
							}
						}
					}
					
					releaseReadLock(inputRing);
		            	
		        	messageCount++;
		        	
		        	total += len;

		        	//block until one more byteVector is ready.
		        	target += msgSize;
		        }   

	//		return false;//TODO: B, refactor to not loop here.
		}
	}

	private final class DumpStageHighLevel extends PronghornStage {
		private final RingBuffer inputRing;
		private final boolean useRoute;
		final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;

		private DumpStageHighLevel(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
		}

		@Override
		public boolean exhaustedPoll() {

				long expectedMessageCount = useRoute?testMessages/splits:testMessages;
			//	RingWalker.setReleaseBatchSize(inputRing, 8);
				
				int msgCount=0;
				int lastPos = -1;
				int msgId = 0;
				do {
		        	if (msgCount==expectedMessageCount) {
		        		//quit early there are no more message coming because this is how many were sent.
		        		//System.err.println("done after "+messageCount+" messages and "+total+" bytes ");
		               	return false;
		        	}
					
					//try also releases previously read fragments
					if (RingReader.tryReadFragment(inputRing)) {
												
						
						assert(RingReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
						msgId = RingReader.getMsgIdx(inputRing);
						
						if (msgId>=0) {																	
							msgCount++;
							//check the data
							int len = RingReader.readBytesLength(inputRing, FIELD_ID);
							assertEquals(testArray.length,len);
																
							//test that pos moves as expected
							int pos = RingReader.readBytesPosition(inputRing, FIELD_ID);
							if (lastPos>=0) {
								assertEquals((lastPos+len)&inputRing.byteMask, pos&inputRing.byteMask);
							} 
							lastPos = pos;
											
							//This block causes a dramatic slow down of the work!!
							if (deepTest) {
								if (!RingReader.eqASCII(inputRing, FIELD_ID, testString)) {
									fail("\nexpected:\n"+testString+"\nfound:\n"+RingReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString() );
								}
							}


						}
					} else {
						Thread.yield();//do something meaningful while we wait for new data
					}
					
					//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
				} while (msgId!=-1);
				//System.err.println("done");
				
				RingReader.releaseReadLock(inputRing);
				

			return false;// TODO: B, refactor to not spin here.
		}
	}

	private final class CopyStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final RingBuffer inputRing;

		private CopyStageLowLevel(GraphManager gm,RingBuffer outputRing, RingBuffer inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;
		}

		@Override
		public boolean exhaustedPoll() {
				
				RingBuffer.setReleaseBatchSize(inputRing, 8);
				RingBuffer.setPublishBatchSize(outputRing, 8);
				
		        //only enter this block when we know there are records to read
			    long nextHeadTarget = tailPosition(inputRing) + 2;
			    long headPosCache = headPosition(inputRing);
			    
		        //two per message, and we only want half the buffer to be full
		        long tailPosCache = tailPosition(outputRing);
		        
			    //keep local copy of the last time the tail was checked to avoid contention.
		        long nextTailTarget = headPosition(outputRing) - (outputRing.maxSize- msgSize);
		        
		        
		        int mask = byteMask(outputRing); // data often loops around end of array so this mask is required
		        int msgId = 0;
		        do {
		        	
		        	//is there data to be written
		        	headPosCache = spinBlockOnHead(headPosCache, nextHeadTarget, inputRing); //TOOD: AAAA, need assert to catch long block here.
		        	nextHeadTarget += msgSize;

		            //read the message
		            msgId = RingBuffer.takeMsgIdx(inputRing);
					
					if (msgId==0) {	   															
		            	int meta = takeRingByteMetaData(inputRing);
		            	int len = takeRingByteLen(inputRing);
		            	//is there room to write
		            	tailPosCache = spinBlockOnTail(tailPosCache, nextTailTarget, outputRing);
		            	nextTailTarget += msgSize;
		            	
		            	RingBuffer.addMsgIdx(outputRing, 0);
						RingBuffer.addByteArrayWithMask(outputRing, mask, len, byteBackingArray(meta, inputRing), bytePosition(meta, inputRing, len));	
								
						publishWrites(outputRing);
						releaseReadLock(inputRing);
					} 
					 
		        }  while (msgId!=-1);
		        
		        
				tailPosCache = spinBlockOnTail(tailPosCache, nextTailTarget, outputRing);
				
				RingBuffer.publishEOF(outputRing);
				
				RingBuffer.releaseAll(inputRing);
				
				assert(RingBuffer.contentRemaining(inputRing)==0);
		        
				return false;// TODO: B, refactor to not spin here.

		}
	}

	private final class CopyStageHighLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final RingBuffer inputRing;
		final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;

		private CopyStageHighLevel(GraphManager gm, RingBuffer outputRing, RingBuffer inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;
		}

		@Override
		public boolean exhaustedPoll() {
		
				RingReader.setReleaseBatchSize(inputRing, 8);
				RingWriter.setPublishBatchSize(outputRing, 8);
				
				int msgId = 0;
				do {
					if (RingReader.tryReadFragment(inputRing)) { 
						assert(RingReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
						msgId = RingReader.getMsgIdx(inputRing);
						
						
						//wait until the target ring has room for this message
						if (0==msgId) {
							RingWriter.blockWriteFragment(outputRing,MSG_ID);
							//copy this message from one ring to the next
							//NOTE: in the normal world I would expect the data to be modified before getting moved.
							RingReader.copyBytes(inputRing, outputRing, FIELD_ID, FIELD_ID);
							RingWriter.publishWrites(outputRing);

						} 
					} else {
						Thread.yield();//do something meaningful while we wait for new data
					}
					//exit the loop logic is not defined by the ring but instead is defined by data/usage, in this case we use a null byte array aka (-1 length)
				} while (msgId!=-1);
															
				RingWriter.publishEOF(outputRing);
				RingReader.releaseReadLock(inputRing);
				
				return false;// TODO: B, refactor to not spin here.
		}
	}

	private final class ProductionStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;

		private ProductionStageLowLevel(GraphManager gm, RingBuffer outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
		}

		@Override
		public boolean exhaustedPoll() {
			
			  RingBuffer.setPublishBatchSize(outputRing, 8);
				
			  int messageSize = msgSize;
			  long messageCount = testMessages;            
		      //keep local copy of the last time the tail was checked to avoid contention.
		      long nextTailTarget = headPosition(outputRing) - (outputRing.maxSize-messageSize);
		      long tailPosCache = tailPosition(outputRing);                        
		      while (--messageCount>=0) {
		    	  
		    	  //block until we have room
		    	  tailPosCache = spinBlockOnTail(tailPosCache, nextTailTarget, outputRing);
		    	  nextTailTarget += messageSize;		          
		    	  			        	  
		          //write the record
		    	  RingBuffer.addMsgIdx(outputRing, 0);
		          
		    	  //write in order
		    	  addByteArray(testArray, 0, testArray.length, outputRing);
		    	  
				  publishWrites(outputRing);
				  
		      }
		      
		      tailPosCache = spinBlockOnTail(tailPosCache, nextTailTarget, outputRing);

		      RingBuffer.publishEOF(outputRing);

			return false;//do not come back
		}
	}

	private final class ProductionStageHighLevel extends PronghornStage {
		private final RingBuffer outputRing;
		final int MESSAGE_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		final int FIELD_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;

		private ProductionStageHighLevel(GraphManager gm, RingBuffer outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
		}

		@Override
		public boolean exhaustedPoll() {

			 long messageCount = testMessages; 
			 RingWriter.setPublishBatchSize(outputRing, 8);

			 while (--messageCount>=0) {
				
				 RingWriter.blockWriteFragment(outputRing, MESSAGE_LOC);
				 RingWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length);							 
				 RingWriter.publishWrites(outputRing);
				 
			 }
			 RingWriter.publishEOF(outputRing);

			return false;//do not come back
			
		}
	}

	private static final int TIMEOUT_SECONDS = 30;
	private static final String testString1 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@@";
	private static final String testString = testString1+testString1+testString1+testString1+testString1+testString1+testString1+testString1;
	//using length of 61 because it is prime and will wrap at odd places
	private final byte[] testArray = testString.getBytes();//, this is a reasonable test message.".getBytes();
	private final long testMessages = 1000000; 
	private final int stages = 4;
	private final int splits = 2;
		
	private final boolean deepTest = false;//can be much faster if we change the threading model
		 
	private final byte primaryBits   = 6; 
	private final byte secondaryBits = 15;
    
	private final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];

	
	@Test
	public void pipelineExampleHighLevelRoute() {			
		pipelineTest(true, false, true, true);		
	}
	
	@Test
	public void pipelineExampleLowLevelRoute() {			
		pipelineTest(false, false, true, true);		
	}
	
	@Test
	public void pipelineExampleHighLevelRouteWithMonitor() {			
		pipelineTest(true, true, true, true);		
	}
	
	@Test
	public void pipelineExampleLowLevelRouteWithMonitor() {			
		pipelineTest(false, true, true, true);		
	}

	
	
//	@Test
//	public void pipelineExampleHighLevelSplits() {			
//		pipelineTest(true, false, true, false);		
//	}
//	
//	@Test
//	public void pipelineExampleLowLevelSplits() {			
//		pipelineTest(false, false, true, false);		
//	}
//	
//	@Test
//	public void pipelineExampleHighLevelSplitsWithMonitor() {			
//		pipelineTest(true, true, true, false);		
//	}
//	
//	@Test
//	public void pipelineExampleLowLevelSplitsWithMonitor() {			
//		pipelineTest(false, true, true, false);		
//	}

	

	@Test
	public void pipelineExampleHighLevel() {		
		 pipelineTest(true, false, false, true);	 		 
	}

	@Test
	public void pipelineExampleLowLevel() {			
		 pipelineTest(false, false, false, true);
	}

	@Test
	public void pipelineExampleHighLevelWithMonitor() {		
		 pipelineTest(true, true, false, true);	 		 
	}

	@Test
	public void pipelineExampleLowLevelWithMonitor() {			
		 pipelineTest(false, true, false, true);	 		 
	}
	
		

	private void pipelineTest(boolean highLevelAPI, boolean monitor, boolean useTap, boolean useRouter) {
		final GraphManager gm = new GraphManager();
		StageManager.resetAsserts();//needed for unit tests that run in the same JVM
		StageManager normalService = new ThreadPerStageManager(gm);

		
		System.out.println();
		 	 
		 
		 assertEquals("For "+FieldReferenceOffsetManager.RAW_BYTES.name+" expected no need to add field.",
				      0,FieldReferenceOffsetManager.RAW_BYTES.fragNeedsAppendedCountOfBytesConsumed[0]);
			
		
		 int stagesBetweenSourceAndSink = stages -2;
		 
		 int daemonThreads = (useTap ? stagesBetweenSourceAndSink : 0);
		 int schcheduledThreads = 1;
		
		 int normalThreads =    2/* source and sink*/   + ((useTap ? splits : 1)*stagesBetweenSourceAndSink); 
		 int totalThreads = daemonThreads+schcheduledThreads+normalThreads;

		 
		 ScheduledThreadPoolExecutor scheduledService = new ScheduledThreadPoolExecutor(schcheduledThreads, daemonThreadFactory());

		 
		 
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
			 rings[j] = new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
			 			 
			 assertEquals("For "+rings[j].consumerData.from.name+" expected no need to add field.",0,rings[j].consumerData.from.fragNeedsAppendedCountOfBytesConsumed[0]);
		
			 
			 //test by starting at different location in the ring to force roll over.
			 rings[j].reset(rings[j].maxSize-13,rings[j].maxByteSize-101);
	  		 
			 if (monitor) {
				 monitorRings[j] = new RingBuffer(new RingBufferConfig((byte)16, (byte)2, null, montorFROM));
				 //assertTrue(mo)
				 monitorStages[j] = new RingBufferMonitorStage(rings[j], monitorRings[j]);	
				 
				 //this is a bit complex may be better to move this inside on thread?
				 scheduledService.scheduleAtFixedRate(monitorStages[j], j*5, 41, TimeUnit.MILLISECONDS);	
				 final RingBuffer mon = monitorRings[j];
				 scheduledService.scheduleAtFixedRate(new Runnable() {
					PronghornStage stage =  new DumpMonitorStage(gm, mon);
					@Override
					public void run() {
						stage.exhaustedPoll();
					}
					 
				 }
						 
						 , 100+(j*5), 47, TimeUnit.MILLISECONDS);	
			 }
		 }
		 		 
		 
		 //start the timer		 
		 final long start = System.currentTimeMillis();
		 
		 //add all the stages start running
		 j = 0;
		RingBuffer outputRing = rings[j];
		
		 normalService.submit(highLevelAPI ? new ProductionStageHighLevel(gm, outputRing) : new ProductionStageLowLevel(gm, outputRing));
		 int i = stagesBetweenSourceAndSink;
		 while (--i>=0) {
			 if (useTap & 0==i) { //only do taps on first stage or this test could end up using many many threads.		 
				 
				 RingBuffer[] splitsBuffers = new RingBuffer[splits];
				 splitsBuffers[0] = rings[j+1];//must jump ahead because we are setting this early
				 if (splits>1) {
					 int k = splits;
					 while (--k>0) {
						 splitsBuffers[k] = new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
						RingBuffer inputRing = splitsBuffers[k];
						boolean useRoute = useTap&useRouter;
						 ///
						 normalService.submit(highLevelAPI ? new DumpStageHighLevel(gm, inputRing, useRoute) : new DumpStageLowLevel(gm, inputRing, useRoute));
					 }
				 } 
				 
				 
			     if (useRouter) {
			    	 int r = splitsBuffers.length;
			    	 while (--r>=0) {
			    		 RingBuffer.setPublishBatchSize(splitsBuffers[r],8);
			    		 
			    	 }
			    	 RingReader.setReleaseBatchSize(rings[j], 8); 
			    	 normalService.submit(new RoundRobinRouteStage2(gm, rings[j++], splitsBuffers));
			     } else {
			    	 normalService.submit(new SplitterStage2(gm, rings[j++], splitsBuffers)); 
			     }
			 } else {			 
				 RingBuffer inputRing = rings[j++];
				RingBuffer outputRing1 = rings[j];
				normalService.submit(highLevelAPI ? new CopyStageHighLevel(gm, outputRing1, inputRing) : new CopyStageLowLevel(gm, outputRing1, inputRing));		
			 }
			 
		 }
		RingBuffer inputRing = rings[j];
		boolean useRoute = useTap&useRouter;
		 normalService.submit(highLevelAPI ? new DumpStageHighLevel(gm, inputRing, useRoute) : new DumpStageLowLevel(gm, inputRing, useRoute));
		 
		 System.out.println("########################################################## Testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTap? "using "+splits+(useRouter?" router ":" splitter "):"")+(monitor?"monitored":"")+" totalThreads:"+totalThreads);
		 
		
		 //blocks until all the submitted runnables have stopped
		
			 //this timeout is set very large to support slow machines that may also run this test.
			boolean cleanExit = normalService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (!cleanExit) {
				//dump the Queue data
				int k=0;
				while (k<rings.length){
					
					System.err.println("Ring "+k+"  "+rings[k].toString());
										
					k++;
				}
				
			}
			
			assertTrue("Test timed out, forced shut down of stages",cleanExit); //the tests are all getting cut here
			
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
			//TODO: should we flush all the monitoring?
			
			long duration = System.currentTimeMillis()-start;
			
			long bytes = testMessages * (long)testArray.length;
			long bpSec = (1000l*bytes*8l)/duration;
			
			long msgPerMs = testMessages/duration;
			System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpSec/1000000000f)+" stages:"+stages+" msg/ms:"+msgPerMs+" MsgSize:"+testArray.length);

				t = rings.length;
				while (--t>=0) {
					RingBuffer.shutdown(rings[t]);
				}
				if (monitor) {
					t = monitorRings.length;
					while (--t>=0) {
						RingBuffer.shutdown(monitorRings[t]);
					}
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
	
	 
	
}
