package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.addByteArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
		private int monitorMessageSize;
		private long nextTargetHeadPos;
		private long messageCount = 0;
		private long headPosCache;
		
		private DumpMonitorStage(GraphManager gm,RingBuffer inputRing) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.monitorMessageSize = RingBuffer.from(inputRing).fragDataSize[0];
			this.nextTargetHeadPos = monitorMessageSize;
			this.headPosCache = inputRing.headPos.longValue();
		}

		@Override
		public void run() {  

			do {				
	            if (headPosCache < nextTargetHeadPos) {
					headPosCache = inputRing.headPos.longValue();
					if (headPosCache < nextTargetHeadPos) {
						return; //this is a state-less stage so it must return false not true unless there is good reason.
					}
				}
	        
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
	        	nextTargetHeadPos += monitorMessageSize;
		        	                       	    	                        		
			} while (true);
		}
	}

	private final class DumpStageLowLevel extends PronghornStage {
		private final RingBuffer inputRing;
		private final boolean useRoute;
		private long total = 0;
		private int lastPos = -1;
		
		//only enter this block when we know there are records to read
		private long nextTargetHead;
		private long headPosCache;
		private long messageCount = 0;

		private DumpStageLowLevel(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
			RingBuffer.setReleaseBatchSize(inputRing, 8);
			nextTargetHead = RingBuffer.EOF_SIZE + tailPosition(inputRing);
			headPosCache = headPosition(inputRing);		
		}

		@Override
		public void run() {  
		        	
			do {
				//System.err.println(headPosCache+"  "+nextTargetHead+"  "+messageCount+"vs"+testMessages);
				
			        if (headPosCache < nextTargetHead) {
						headPosCache = inputRing.headPos.longValue();
						if (headPosCache < nextTargetHead) {
							return; //come back later when we find more content
						}
					}
			        //block until one more byteVector is ready.
			        nextTargetHead += msgSize;
		        	
		            int msgId = RingBuffer.takeMsgIdx(inputRing);
		            if (msgId<0) {  
		            	assertEquals(testMessages,useRoute? messageCount*splits: messageCount);		            			            	
		            	RingBuffer.releaseAll(inputRing);	
		            	return;
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

		        	
			} while(true);
			
		}
	}

	private final class DumpStageHighLevel extends PronghornStage {
		private final RingBuffer inputRing;
		private final boolean useRoute;
		final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
		int msgCount=0;

		private DumpStageHighLevel(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
			//	RingWalker.setReleaseBatchSize(inputRing, 8);
		}

		@Override
		public void run() {
				
				int lastPos = -1;
				
					//try also releases previously read fragments
					while (RingReader.tryReadFragment(inputRing)) {												
						
						assert(RingReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
						int msgId = RingReader.getMsgIdx(inputRing);
						
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

						} else if (-1 == msgId) {
							assertEquals(testMessages,useRoute? msgCount*splits: msgCount);	
							RingReader.releaseReadLock(inputRing);
							//System.err.println("done");
							return;
						}
					} 
					return;
	
		}
	}

	private final class CopyStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final RingBuffer inputRing;
		
		private long nextHeadTarget;
		private long headPosCache;
		
		//two per message, and we only want half the buffer to be full
		private long tailPosCache;
		
		//keep local copy of the last time the tail was checked to avoid contention.
		private long nextTailTarget;
		private int mask;

		private CopyStageLowLevel(GraphManager gm,RingBuffer outputRing, RingBuffer inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;

			//RingBuffer.setReleaseBatchSize(inputRing, 8);
			//RingBuffer.setPublishBatchSize(outputRing, 8);
			
			//only enter this block when we know there are records to read
			this.nextHeadTarget = tailPosition(inputRing) + RingBuffer.EOF_SIZE;
			this.headPosCache = headPosition(inputRing);
			
			//two per message, and we only want half the buffer to be full
			this.tailPosCache = tailPosition(outputRing);
			
			//keep local copy of the last time the tail was checked to avoid contention.
			this.nextTailTarget = headPosition(outputRing) - (outputRing.maxSize- msgSize);			
			
			this.mask = byteMask(outputRing); // data often loops around end of array so this mask is required
		}

		@Override
		public void run() {
				
			do {
					//TODO: B, need to find a way to make this pattern easy
					//must update headPosCache but only when we need to 
			        if (headPosCache < nextHeadTarget) {
						headPosCache = inputRing.headPos.longValue();
						if (headPosCache < nextHeadTarget) {
							return;
						}
					}

			        if (tailPosCache < nextTailTarget) {
			        	tailPosCache = outputRing.tailPos.longValue();
						if (tailPosCache < nextTailTarget) {
							return;
						}
					}
			        					
					nextHeadTarget += msgSize;
					
					//read the message
		        	int msgId = RingBuffer.takeMsgIdx(inputRing);
					
					if (msgId==0) {	   															
		            	int meta = takeRingByteMetaData(inputRing);
		            	int len = takeRingByteLen(inputRing);
		            	//is there room to write
		            	nextTailTarget += msgSize;
		            	
		            	RingBuffer.addMsgIdx(outputRing, 0);
						RingBuffer.addByteArrayWithMask(outputRing, mask, len, byteBackingArray(meta, inputRing), bytePosition(meta, inputRing, len));	
								
						RingBuffer.publishWrites(outputRing);
						RingBuffer.releaseReadLock(inputRing);
					} else if (-1 == msgId) {							
						RingBuffer.publishEOF(outputRing);						
						RingBuffer.releaseAll(inputRing);						
						assert(RingBuffer.contentRemaining(inputRing)==0);						
						return;						
					}
			}while (true);	 
				
		}
	}

	private final class CopyStageHighLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final RingBuffer inputRing;
		final int MSG_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		final int FIELD_ID = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
		int msgId=-2;

		private CopyStageHighLevel(GraphManager gm, RingBuffer outputRing, RingBuffer inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;

			RingReader.setReleaseBatchSize(inputRing, 8);
			RingWriter.setPublishBatchSize(outputRing, 8);
		}

		@Override
		public void run() {
				do {
					if (msgId<0) {
				        if (RingReader.tryReadFragment(inputRing)) { 
							assert(RingReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
							msgId = RingReader.getMsgIdx(inputRing);
						} else {
							return;
						}
					}
					//wait until the target ring has room for this message
					if (0==msgId) {
						if (RingWriter.tryWriteFragment(outputRing,MSG_ID)) {
							//copy this message from one ring to the next
							//NOTE: in the normal world I would expect the data to be modified before getting moved.
							RingReader.copyBytes(inputRing, outputRing, FIELD_ID, FIELD_ID);
							RingWriter.publishWrites(outputRing);
							msgId = -2;
						} else {
							return;
						}
					} else if (-1==msgId) {
						RingWriter.publishEOF(outputRing);	 //TODO, hidden blocking call			
						RingReader.releaseReadLock(inputRing);
						assert(RingBuffer.contentRemaining(inputRing)==0);
						return;
					}
				} while (true);

		}
	}

	private final class ProductionStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private long messageCount;
		private long nextTailTarget;
		private long tailPosCache;                        
		
		private ProductionStageLowLevel(GraphManager gm, RingBuffer outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
			this.messageCount = testMessages;            
			this.nextTailTarget = headPosition(outputRing) - (outputRing.maxSize-msgSize);
			this.tailPosCache = tailPosition(outputRing); 
			RingBuffer.setPublishBatchSize(outputRing, 8);
		}

		@Override
		public void run() {
			
			 do {
		        if (tailPosCache < nextTailTarget) {
		        	tailPosCache = outputRing.tailPos.longValue();
					if (tailPosCache < nextTailTarget) {
						return;
					}
				}
		        nextTailTarget += msgSize;	
		        
		        if (--messageCount>=0) {
			          //write the record
			    	  RingBuffer.addMsgIdx(outputRing, 0);
			    	  addByteArray(testArray, 0, testArray.length, outputRing);
					  publishWrites(outputRing);
		        } else {
				      RingBuffer.publishEOF(outputRing);
				      shutdown();
				      return;
		        }		        
		        
			 } while (true);   
		}
	}

	private final class ProductionStageHighLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final int MESSAGE_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
		private final int FIELD_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
		private long messageCount = testMessages; 

		private ProductionStageHighLevel(GraphManager gm, RingBuffer outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
			RingWriter.setPublishBatchSize(outputRing, 8);
		}

		@Override
		public void run() {

			 while (messageCount>0) {				
				 if (RingWriter.tryWriteFragment(outputRing, MESSAGE_LOC)) {
					 RingWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length);							 
					 RingWriter.publishWrites(outputRing);
					 messageCount--;
				 } else {
					 return;
				 }
			 }
			 RingWriter.publishEOF(outputRing);	
			 shutdown();
 			 return;//do not come back			
		}
	}

	private static final int TIMEOUT_SECONDS = 120;
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

	
	@Test
	public void pipelineExampleHighLevelSplits() {			
		pipelineTest(true, false, true, false);		
	}
	
	@Test
	public void pipelineExampleLowLevelSplits() {			
		pipelineTest(false, false, true, false);		
	}
	
	@Test
	public void pipelineExampleHighLevelSplitsWithMonitor() {			
		pipelineTest(true, true, true, false);		
	}
	
	@Test
	public void pipelineExampleLowLevelSplitsWithMonitor() {			
		pipelineTest(false, true, true, false);		
	}
	
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
	
		 GraphManager gm = new GraphManager();
		
		 StageManager normalService = new ThreadPerStageManager(gm);
						
		 System.out.println();
				 
		 assertEquals("For "+FieldReferenceOffsetManager.RAW_BYTES.name+" expected no need to add field.",
				      0,FieldReferenceOffsetManager.RAW_BYTES.fragNeedsAppendedCountOfBytesConsumed[0]);
					
		 int stagesBetweenSourceAndSink = stages -2;
		 
		 int daemonThreads = (useTap ? stagesBetweenSourceAndSink : 0);
		 int schcheduledThreads = 1;
		
		 int normalThreads =    2/* source and sink*/   + ((useTap ? splits : 1)*stagesBetweenSourceAndSink); 
		 int totalThreads = daemonThreads+schcheduledThreads+normalThreads;

		 		 
		 //build all the rings
		 int j = stages-1;
		 RingBuffer[] rings = new RingBuffer[j];
		 
		 PronghornStage[] monitorStages = null;

		 		 
		 RingBuffer[] monitorRings = null;
		 FieldReferenceOffsetManager montorFROM = null;
		 if (monitor) {
			monitorStages = new PronghornStage[j];
		 	monitorRings = new RingBuffer[j];
		 	montorFROM = RingBufferMonitorStage.buildFROM();
		 }
		 
		 byte ex = (byte)(useRouter ? 0 : 1);
		 
		 while (--j>=0)  {
			 
			 if (stages-2==j) {
				 //need to make this ring bigger when the splitter is used
				 rings[j] = new RingBuffer(new RingBufferConfig((byte)(primaryBits+ex), (byte)(secondaryBits+ex), null,  FieldReferenceOffsetManager.RAW_BYTES));
			 }  else {
				 rings[j] = new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
			 } 
			 
			 assertEquals("For "+rings[j].ringWalker.from.name+" expected no need to add field.",0,rings[j].ringWalker.from.fragNeedsAppendedCountOfBytesConsumed[0]);
		
			 
			 //test by starting at different location in the ring to force roll over.
			 rings[j].reset(rings[j].maxSize-13,rings[j].maxByteSize-101);
	  		 
			 if (monitor) {
				 monitorRings[j] = new RingBuffer(new RingBufferConfig((byte)16, (byte)2, null, montorFROM));
				 //assertTrue(mo)
				 monitorStages[j] = new RingBufferMonitorStage2(gm, rings[j], monitorRings[j]);	
				 
				 //this is a bit complex may be better to move this inside on thread?
				
				 GraphManager.setScheduleRate(gm, 41000000, monitorStages[j]);
				 
				 final RingBuffer mon = monitorRings[j];
				 
				 GraphManager.setScheduleRate(gm, 47000000, new DumpMonitorStage(gm, mon));
				 
			 }
		 }
		 		 

		 
		 //add all the stages start running
		 j = 0;
		RingBuffer outputRing = rings[j];
		
		 GraphManager.setContinuousRun(gm, highLevelAPI ? new ProductionStageHighLevel(gm, outputRing) : new ProductionStageLowLevel(gm, outputRing));
		 int i = stagesBetweenSourceAndSink;
		 while (--i>=0) {
			 if (useTap & 0==i) { //only do taps on first stage or this test could end up using many many threads.		 
				 
				 RingBuffer[] splitsBuffers = new RingBuffer[splits];
				 splitsBuffers[0] = rings[j+1];//must jump ahead because we are setting this early
				 assert(splitsBuffers[0].pBits == ex+primaryBits);
				 if (splits>1) {
					 int k = splits;
					 while (--k>0) {
						 splitsBuffers[k] = new RingBuffer(new RingBufferConfig((byte)(primaryBits+ex), (byte)(secondaryBits+ex), null,  FieldReferenceOffsetManager.RAW_BYTES));
						RingBuffer inputRing = splitsBuffers[k];
						boolean useRoute = useTap&useRouter;
						 ///
						 GraphManager.setContinuousRun(gm, highLevelAPI ? new DumpStageHighLevel(gm, inputRing, useRoute) : new DumpStageLowLevel(gm, inputRing, useRoute));
					 }
				 } 
				 
				 
			     if (useRouter) {
			    	 int r = splitsBuffers.length;
			    	 while (--r>=0) {
			    		 RingBuffer.setPublishBatchSize(splitsBuffers[r],8);
			    		 
			    	 }
			    	 RingReader.setReleaseBatchSize(rings[j], 8); 
			    	 GraphManager.setContinuousRun(gm, new RoundRobinRouteStage2(gm, rings[j++], splitsBuffers));
			     } else {
			    	 GraphManager.setContinuousRun(gm, new SplitterStage2(gm, rings[j++], splitsBuffers)); 
			     }
			 } else {			 
				 RingBuffer inputRing = rings[j++];
				RingBuffer outputRing1 = rings[j];
				GraphManager.setContinuousRun(gm, highLevelAPI ? new CopyStageHighLevel(gm, outputRing1, inputRing) : new CopyStageLowLevel(gm, outputRing1, inputRing));		
			 }
			 
		 }
		RingBuffer inputRing = rings[j];
		boolean useRoute = useTap&useRouter;
		 GraphManager.setContinuousRun(gm, highLevelAPI ? new DumpStageHighLevel(gm, inputRing, useRoute) : new DumpStageLowLevel(gm, inputRing, useRoute));
		 
		 System.out.println("########################################################## Testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTap? "using "+splits+(useRouter?" router ":" splitter "):"")+(monitor?"monitored":"")+" totalThreads:"+totalThreads);
		 
		 //start the timer		 
		 final long start = System.currentTimeMillis();
		    normalService.startup();
		 //blocks until all the submitted runnables have stopped
		
			 //this timeout is set very large to support slow machines that may also run this test.
			boolean cleanExit = normalService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (!cleanExit) {
				//dump the Queue data
				int k=0;
				while (k<rings.length){
					System.err.println(GraphManager.getRingProducer(gm, rings[k].ringId)+"  ->\n    "+rings[k].toString()+"  ->  "+GraphManager.getRingConsumer(gm, rings[k].ringId));										
					k++;
				}	
				System.err.println(GraphManager.getRingConsumer(gm, rings[k-1].ringId));
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
		 
	}

}
