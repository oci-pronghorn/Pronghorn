package com.ociweb.pronghorn.stage;

import static com.ociweb.pronghorn.ring.RingBuffer.addByteArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.readBytesAndreleaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorAdapter;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.monitor.MonitorFROM;
import com.ociweb.pronghorn.stage.monitor.RingBufferMonitorStage;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class RingBufferPipeline {
	
	private final class DumpMonitorStage extends PronghornStage {
		private final RingBuffer inputRing;
		private int monitorMessageSize;
		private long messageCount = 0;
		
		private DumpMonitorStage(GraphManager gm,RingBuffer inputRing) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.monitorMessageSize = RingBuffer.from(inputRing).fragDataSize[0];
		}

		@Override
		public void run() {  
			while (RingBuffer.contentToLowLevelRead(inputRing, monitorMessageSize)) {
	        
	            //read the message
	            int msgId = RingBuffer.takeMsgIdx(inputRing);//readValue(0, inputRing.buffer,inputRing.mask,inputRing.workingTailPos.value); 
	            
	            long time = RingBuffer.takeLong(inputRing);
	            long head = RingBuffer.takeLong(inputRing);
	            long tail = RingBuffer.takeLong(inputRing);
	            int tmpId = RingBuffer.takeValue(inputRing);
	            int bufSize = RingBuffer.takeValue(inputRing);
	            int bLen = RingBuffer.takeValue(inputRing);

	            RingBuffer.setWorkingTailPosition(inputRing, RingBuffer.getWorkingTailPosition(inputRing)+monitorMessageSize);
	                     
	            int queueDepth = (int)(head-tail);
                //show depth vs bufSize	            
				
	        	//doing nothing with the data
				RingBuffer.releaseReads(inputRing);

	        	
	        	messageCount++;
	        	RingBuffer.confirmLowLevelRead(inputRing, monitorMessageSize);

			}
		}
	}

	private final class DumpStageLowLevel extends PronghornStage {
		private final RingBuffer inputRing;
		private final boolean useRoute;
		private long total = 0;
		private int lastPos = -1;
		
		//only enter this block when we know there are records to read
		private long messageCount = 0;

		private DumpStageLowLevel(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
			RingBuffer.setReleaseBatchSize(inputRing, 8);
		}

		@Override
		public void run() {  
		        	
			while (RingBuffer.contentToLowLevelRead(inputRing, msgSize)) {
		        	
		            RingBuffer.takeMsgIdx(inputRing);
		            RingBuffer.confirmLowLevelRead(inputRing, msgSize);
		        	int meta = takeRingByteMetaData(inputRing);
		        	int len = takeRingByteLen(inputRing);
		        	assertEquals(testArray.length,len);

		        	int pos = bytePosition(meta, inputRing, len);
		        	
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
								fail("String does not match at index "+i+" of "+len+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(pos+i)+" masked "+((pos+i)&mask));
								
							}
						}
					}
					
					RingBuffer.releaseReads(inputRing);
		            	
		        	messageCount++;
		        	
		        	total += len;

			}      	
			
		}
		
		@Override
		public void shutdown() {
			//assertEquals(testMessages,useRoute? messageCount*splits: messageCount);	            			            	
        	RingBuffer.releaseAll(inputRing);	
			
		}
		
	}

	private final class DumpStageStreamingConsumer extends PronghornStage {

		private final boolean useRoute;
		private final StreamingReadVisitor visitor;
		private final StreamingVisitorReader reader;
		
		private DumpStageStreamingConsumer(GraphManager gm,RingBuffer inputRing, boolean useRoute) {
			super(gm, inputRing, NONE);
			this.useRoute = useRoute;			
			this.visitor =// new StreamingConsumerToJSON(System.out); 
					new StreamingReadVisitorAdapter() {
				@Override
				public void visitBytes(String name, long id, ByteBuffer value) {
					
					value.flip();
					if (0==value.remaining()) {
						return;//EOM??
					}
					assertEquals(testArray.length, value.remaining());
					value.get(tempArray);
			//	    System.err.println(new String(Arrays.copyOfRange(tempArray, 0 ,100)));
		///		    System.err.println(new String(Arrays.copyOfRange(testArray,0,100)));
				    //TODO: B, this test is not passing.
					
				//	assertTrue(Arrays.equals(testArray, tempArray));
		
				}
			};
			reader = new StreamingVisitorReader(inputRing, visitor );
		}

		@Override
		public void run() {
				reader.run();
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
			RingBuffer.setReleaseBatchSize(inputRing, 8);
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
							RingReader.releaseReadLock(inputRing);
						} else if (-1 == msgId) {
							RingReader.releaseReadLock(inputRing);
							requestShutdown();
							return;
						}
					} 
					return;
	
		}

		@Override
		public void shutdown() {
//			assertEquals(testMessages,useRoute? msgCount*splits: msgCount);	 //TODO: After confirm that flush works we must add this test back in
		}
		
		
		
	}

	private final class CopyStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private final RingBuffer inputRing;
		
		private int mask;

		private CopyStageLowLevel(GraphManager gm,RingBuffer outputRing, RingBuffer inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;

			RingBuffer.setReleaseBatchSize(inputRing, 8);
			RingBuffer.setPublishBatchSize(outputRing, 8);
			
			this.mask = byteMask(outputRing); // data often loops around end of array so this mask is required
		}

		@Override
		public void startup() {
		}
		
		@Override
		public void run() {
			
			while (RingBuffer.contentToLowLevelRead(inputRing, msgSize) && RingBuffer.roomToLowLevelWrite(outputRing, msgSize)) {			
			        
			        RingBuffer.confirmLowLevelRead(inputRing, msgSize);
			        RingBuffer.confirmLowLevelWrite(outputRing, msgSize);
	        					
					
					//read the message
		        	RingBuffer.takeMsgIdx(inputRing);
	  															
	            	int meta = takeRingByteMetaData(inputRing);
	            	int len = takeRingByteLen(inputRing);
	            	//is there room to write
	            	
	            	RingBuffer.addMsgIdx(outputRing, 0);
					RingBuffer.addByteArrayWithMask(outputRing, mask, len, byteBackingArray(meta, inputRing), bytePosition(meta, inputRing, len));	
							
					RingBuffer.publishWrites(outputRing);
					RingBuffer.releaseReads(inputRing);

			} 
				
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
							RingReader.releaseReadLock(inputRing);
							msgId = -2;
						} else {
							return;
						}
					} else if (-1==msgId) {
						
						RingWriter.publishEOF(outputRing);	 //TODO: AA, hidden blocking call		
						RingBuffer.setReleaseBatchSize(inputRing, 0);
						RingReader.releaseReadLock(inputRing);
						assert(RingBuffer.contentRemaining(inputRing)==0);
						requestShutdown();
						return;
					}
				} while (true);

		}
	}

	private final class ProductionStageLowLevel extends PronghornStage {
		private final RingBuffer outputRing;
		private long messageCount;                       
		
		private ProductionStageLowLevel(GraphManager gm, RingBuffer outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
			this.messageCount = testMessages;         
			RingBuffer.setPublishBatchSize(outputRing, 8);
			
		}

		
		public void startup() {
		}
		
		@Override
		public void run() {
			
			while (RingBuffer.roomToLowLevelWrite(outputRing, msgSize)) {
		        
		        if (--messageCount>=0) {
 		        	  RingBuffer.confirmLowLevelWrite(outputRing, msgSize);
			          //write the record
			    	  RingBuffer.addMsgIdx(outputRing, 0);
			    	  addByteArray(testArray, 0, testArray.length, outputRing);
					  publishWrites(outputRing);
		        } else {
				      requestShutdown();
				      return;
		        }		        
			}      
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
					 RingWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length, Integer.MAX_VALUE);							 
					 RingWriter.publishWrites(outputRing);
					 messageCount--;
				 } else {
					 return;
				 }
			 }
			 RingWriter.publishEOF(outputRing);	
			 requestShutdown();
 			 return;//do not come back			
		}
	}

	private static final int TIMEOUT_SECONDS = 10;
	private static final String testString1 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.-_+()*@@@@@@@@@@@@@@@@";
	private static final String testString = testString1+testString1+testString1+testString1+testString1+testString1+testString1+testString1;
	//using length of 61 because it is prime and will wrap at odd places
	private final byte[] testArray = testString.getBytes();//, this is a reasonable test message.".getBytes();
	private final byte[] tempArray = new byte[testArray.length];
	private final long testMessages = 100000; 
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
		
		
						
		 System.out.println();

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
		 	montorFROM = MonitorFROM.buildFROM();
		 }
		 
		 byte ex = (byte)(useRouter ? 0 : 1);
		 
		 while (--j>=0)  {
			 
			 if (stages-2==j) {
				 //need to make this ring bigger when the splitter is used
				 rings[j] = new RingBuffer(new RingBufferConfig((byte)(primaryBits+ex), (byte)(secondaryBits+ex), null,  FieldReferenceOffsetManager.RAW_BYTES));
			 }  else {
				 rings[j] = new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, null,  FieldReferenceOffsetManager.RAW_BYTES));
			 } 

			 
			 //test by starting at different location in the ring to force roll over.
			 rings[j].reset(rings[j].sizeOfStructuredLayoutRingBuffer-13,rings[j].sizeOfUntructuredLayoutRingBuffer-101);
	  		 
			 if (monitor) {
				 monitorRings[j] = new RingBuffer(new RingBufferConfig((byte)16, (byte)2, null, montorFROM));
				 final RingBuffer monRing = monitorRings[j];

				 monitorStages[j] = new RingBufferMonitorStage(gm, rings[j], monRing);	
				 
				 
				 
				 //this is a bit complex may be better to move this inside on thread?
				
				 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(4100000), monitorStages[j]);
				 
				 
				 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(4700000), new DumpMonitorStage(gm, monRing));
				 
			 }
		 }
		 		 

		 
		 //add all the stages start running
		 j = 0;
	 	 RingBuffer outputRing = rings[j];
	
		 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? new ProductionStageHighLevel(gm, outputRing) : new ProductionStageLowLevel(gm, outputRing));
				 
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
						 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? 
						 //new DumpStageStreamingConsumer(gm, inputRing, useRoute):
						 new DumpStageHighLevel(gm, inputRing, useRoute) :
						 new DumpStageLowLevel(gm, inputRing, useRoute));
					 }
				 } 
				 
				 
			     if (useRouter) {
			    	 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), new RoundRobinRouteStage(gm, rings[j++], splitsBuffers));
			     } else {
			    	 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), new SplitterStage(gm, rings[j++], splitsBuffers)); 
			     }
			 } else {			 
				 RingBuffer inputRing = rings[j++];
				RingBuffer outputRing1 = rings[j];
				GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? new CopyStageHighLevel(gm, outputRing1, inputRing) : new CopyStageLowLevel(gm, outputRing1, inputRing));		
			 }
			 
		 }
		 
	  	 RingBuffer inputRing = rings[j];
		 boolean useRoute = useTap&useRouter;
		 GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? 
			//	new DumpStageStreamingConsumer(gm, inputRing, useRoute):
			      new DumpStageHighLevel(gm, inputRing, useRoute) :
			      new DumpStageLowLevel(gm, inputRing, useRoute));
		 
		 System.out.println("########################################################## Testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTap? "using "+splits+(useRouter?" router ":" splitter "):"")+(monitor?"monitored":"")+" totalThreads:"+totalThreads);
		 
		 //start the timer		 
		 final long start = System.currentTimeMillis();
		 
		 GraphManager.enableBatching(gm);
		 StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
		 scheduler.startup();
		 
		 
		 //blocks until all the submitted runnables have stopped
		
			 //this timeout is set very large to support slow machines that may also run this test.
			boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
			
			long duration = System.currentTimeMillis()-start;
			
			long bytes = testMessages * (long)testArray.length;
			long bpSec = 0==duration ? 0 :(1000l*bytes*8l)/duration;
			
			long msgPerMs = 0==duration ? 0 :testMessages/duration;
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
