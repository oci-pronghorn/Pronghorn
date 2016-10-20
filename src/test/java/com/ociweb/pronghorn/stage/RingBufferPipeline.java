package com.ociweb.pronghorn.stage;

import static com.ociweb.pronghorn.pipe.Pipe.addByteArray;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.publishWrites;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorAdapter;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.monitor.RingBufferMonitorStage;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class RingBufferPipeline {
	
	private final class DumpMonitorStage extends PronghornStage {
		private final Pipe inputRing;
		private int monitorMessageSize;
		private long messageCount = 0;
		
		private DumpMonitorStage(GraphManager gm,Pipe inputRing) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.monitorMessageSize = Pipe.from(inputRing).fragDataSize[0];
		}

		@Override
		public void run() {  
			while (Pipe.hasContentToRead(inputRing, monitorMessageSize)) {
	        
	            //read the message
	            int msgId = Pipe.takeMsgIdx(inputRing);//readValue(0, inputRing.buffer,inputRing.mask,inputRing.workingTailPos.value); 
	            
	            long time = Pipe.takeLong(inputRing);
	            long head = Pipe.takeLong(inputRing);
	            long tail = Pipe.takeLong(inputRing);
	            int tmpId = Pipe.takeValue(inputRing);
	            int bufSize = Pipe.takeValue(inputRing);
	            int bLen = Pipe.takeValue(inputRing);

	            Pipe.setWorkingTailPosition(inputRing, Pipe.getWorkingTailPosition(inputRing)+monitorMessageSize);
	                     
	            int queueDepth = (int)(head-tail);
                //show depth vs bufSize	            
				
	        	//doing nothing with the data
				Pipe.releaseReadLock(inputRing);

	        	
	        	messageCount++;
	        	Pipe.confirmLowLevelRead(inputRing, monitorMessageSize);

			}
		}
	}

	private final class DumpStageLowLevel extends PronghornStage {
		private final Pipe inputRing;
		private final boolean useRoute;
		private long total = 0;
		private int lastPos = -1;
		
		//only enter this block when we know there are records to read
		private long messageCount = 0;

		private DumpStageLowLevel(GraphManager gm,Pipe inputRing, boolean useRoute) {
			super(gm,inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
			Pipe.setReleaseBatchSize(inputRing, 8);
		}

		@Override
		public void run() {  
		        	
			while (Pipe.hasContentToRead(inputRing, msgSize)) {
		        	
		            Pipe.takeMsgIdx(inputRing);
		            Pipe.confirmLowLevelRead(inputRing, msgSize);
		        	int meta = takeRingByteMetaData(inputRing);
		        	int len = takeRingByteLen(inputRing);
		        	assertEquals(testArray.length,len);

		        	int pos = bytePosition(meta, inputRing, len);
		        	
					if (lastPos>=0) {
						assertEquals((lastPos+len)&inputRing.byteMask,pos&inputRing.byteMask);
					} 
					lastPos = pos;
											
					byte[] data = byteBackingArray(meta, inputRing);
					int mask = blobMask(inputRing);
				
					if (deepTest) {
						//This block causes a dramatic slow down of the work!!
						int i = testArray.length;
						while (--i>=0) {
							if (testArray[i]==data[(pos+i)&mask]) {		    									
							} else {
								fail("String does not match at index "+i+" of "+len+"   tailPos:"+Pipe.tailPosition(inputRing)+" byteFailurePos:"+(pos+i)+" masked "+((pos+i)&mask));
								
							}
						}
					}
					
					Pipe.releaseReadLock(inputRing);
		            	
		        	messageCount++;
		        	
		        	total += len;

			}      	
			
		}
		
		@Override
		public void shutdown() {
			//assertEquals(testMessages,useRoute? messageCount*splits: messageCount);	            			            	
        	Pipe.releaseAll(inputRing);	
			
		}
		
	}

	private final class DumpStageStreamingConsumer extends PronghornStage {

		private final boolean useRoute;
		private final StreamingReadVisitor visitor;
		private final StreamingVisitorReader reader;
		
		private DumpStageStreamingConsumer(GraphManager gm,Pipe inputRing, boolean useRoute) {
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
		private final Pipe inputRing;
		private final boolean useRoute;
		final int MSG_ID = RawDataSchema.MSG_CHUNKEDSTREAM_1;
		final int FIELD_ID = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
		int msgCount=0;

		private DumpStageHighLevel(GraphManager gm,Pipe inputRing, boolean useRoute) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
			this.useRoute = useRoute;
			Pipe.setReleaseBatchSize(inputRing, 8);
		}

		@Override
		public void run() {
				
		            int lastPos = -1;
		    		            
					//try also releases previously read fragments
					while (PipeReader.tryReadFragment(inputRing)) {												
	
						assert(PipeReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
						int msgId = PipeReader.getMsgIdx(inputRing);
						
						if (msgId>=0) {		
						    
							msgCount++;
							//check the data
							int len = PipeReader.readBytesLength(inputRing, FIELD_ID);
							assertEquals(testArray.length,len);
							
							int pos = PipeReader.readBytesPosition(inputRing, FIELD_ID);

							if (lastPos>=0) {
								assertEquals(msgCount+" Expected pos to jump by length:"+len+" for mask "+inputRing.byteMask,
								             (lastPos+len) & inputRing.byteMask, 
								             pos           & inputRing.byteMask);
							} 
							lastPos = pos;
							
							//This block causes a dramatic slow down of the work!!
							if (deepTest) {
								if (!PipeReader.eqASCII(inputRing, FIELD_ID, testString)) {
									fail("\n msgCount:"+msgCount+"\nexpected:\n"+testString+"\nfound:\n"+PipeReader.readASCII(inputRing, FIELD_ID, new StringBuilder()).toString() );
								}
							}
							PipeReader.releaseReadLock(inputRing);
						} else if (-1 == msgId) {
							PipeReader.releaseReadLock(inputRing);
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
		private final Pipe outputRing;
		private final Pipe inputRing;
		
		private int mask;

		private CopyStageLowLevel(GraphManager gm,Pipe outputRing, Pipe inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;

			Pipe.setReleaseBatchSize(inputRing, 8);
			Pipe.setPublishBatchSize(outputRing, 8);
			
			this.mask = blobMask(outputRing); // data often loops around end of array so this mask is required
		}

		@Override
		public void startup() {
		}
		
		@Override
		public void run() {
			
			while (Pipe.hasContentToRead(inputRing, msgSize) && Pipe.roomToLowLevelWrite(outputRing, msgSize)) {			
			        
			        Pipe.confirmLowLevelRead(inputRing, msgSize);
			        Pipe.confirmLowLevelWrite(outputRing, msgSize);
	        					
					
					//read the message
		        	Pipe.takeMsgIdx(inputRing);
	  															
	            	int meta = takeRingByteMetaData(inputRing);
	            	int len = takeRingByteLen(inputRing);
	            	//is there room to write
	            	
	            	Pipe.addMsgIdx(outputRing, 0);
					Pipe.addByteArrayWithMask(outputRing, mask, len, byteBackingArray(meta, inputRing), bytePosition(meta, inputRing, len));	
							
					Pipe.publishWrites(outputRing);
					Pipe.releaseReadLock(inputRing);

			} 
				
		}
		
	}

	private final class CopyStageHighLevel extends PronghornStage {
		private final Pipe outputRing;
		private final Pipe inputRing;
		final int MSG_ID = RawDataSchema.MSG_CHUNKEDSTREAM_1;
		final int FIELD_ID = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
		int msgId=-2;

		private CopyStageHighLevel(GraphManager gm, Pipe outputRing, Pipe inputRing) {
			super(gm,inputRing,outputRing);
			this.outputRing = outputRing;
			this.inputRing = inputRing;

		}

		@Override
		public void run() {
				do {
					if (msgId<0) {
				        if (PipeReader.tryReadFragment(inputRing)) { 
							assert(PipeReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
							msgId = PipeReader.getMsgIdx(inputRing);
						} else {
							return;
						}
					}
					//wait until the target ring has room for this message
					if (0==msgId) {
						if (PipeWriter.tryWriteFragment(outputRing,MSG_ID)) {
							//copy this message from one ring to the next
							//NOTE: in the normal world I would expect the data to be modified before getting moved.
							PipeReader.copyBytes(inputRing, outputRing, FIELD_ID, FIELD_ID);
							PipeWriter.publishWrites(outputRing);
							PipeReader.releaseReadLock(inputRing);
							msgId = -2;
						} else {
							return;
						}
					} else if (-1==msgId) {
						
						PipeWriter.publishEOF(outputRing);	 //TODO: AA, hidden blocking call		
						Pipe.setReleaseBatchSize(inputRing, 0);
						PipeReader.releaseReadLock(inputRing);
						assert(Pipe.contentRemaining(inputRing)==0);
						requestShutdown();
						return;
					}
				} while (true);

		}
	}

	private final class ProductionStageLowLevel extends PronghornStage {
		private final Pipe outputRing;
		private long messageCount;                       
		
		private ProductionStageLowLevel(GraphManager gm, Pipe outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
			this.messageCount = testMessages;         
			Pipe.setPublishBatchSize(outputRing, 8);
			
		}

		
		public void startup() {
		}
		
		@Override
		public void run() {
			
			while (Pipe.roomToLowLevelWrite(outputRing, msgSize)) {
		        
		        if (--messageCount>=0) {
 		        	  Pipe.confirmLowLevelWrite(outputRing, msgSize);
			          //write the record
			    	  Pipe.addMsgIdx(outputRing, 0);
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
		private final Pipe outputRing;
		private final int MESSAGE_LOC = RawDataSchema.MSG_CHUNKEDSTREAM_1;
		private final int FIELD_LOC = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
		private long messageCount = testMessages; 

		private ProductionStageHighLevel(GraphManager gm, Pipe outputRing) {
			super(gm,NONE,outputRing);
			this.outputRing = outputRing;
		}
		
		@Override
		public void startup() {
		    PipeWriter.setPublishBatchSize(outputRing, 8);
		    
		}

		@Override
		public void run() {

			 while (messageCount>0) {				
				 if (PipeWriter.tryWriteFragment(outputRing, MESSAGE_LOC)) {
					 PipeWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length, Integer.MAX_VALUE);							 
					 PipeWriter.publishWrites(outputRing);
					 messageCount--;
				 } else {
					 return;
				 }
			 }
			 PipeWriter.publishEOF(outputRing);	
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
    
	private final int msgSize = RawDataSchema.FROM.fragDataSize[0];

	
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
		 Pipe[] rings = new Pipe[j];
		 
		 PronghornStage[] monitorStages = null;

		 		 
		 Pipe[] monitorRings = null;
		 FieldReferenceOffsetManager montorFROM = null;
		 if (monitor) {
			monitorStages = new PronghornStage[j];
		 	monitorRings = new Pipe[j];
		 	montorFROM = PipeMonitorSchema.FROM;
		 }
		 
		 byte ex = (byte)(useRouter ? 0 : 1);
		 
		 while (--j>=0)  {
			 
			 if (stages-2==j) {
				 //need to make this ring bigger when the splitter is used
				 rings[j] = new Pipe(new PipeConfig((byte)(primaryBits+ex), (byte)(secondaryBits+ex), null,  RawDataSchema.instance));
			 }  else {
				 rings[j] = new Pipe(new PipeConfig(primaryBits, secondaryBits, null, RawDataSchema.instance));
			 } 
	  		 
			 if (monitor) {
				 monitorRings[j] = new Pipe(new PipeConfig((byte)16, (byte)2, null, PipeMonitorSchema.instance));
				 final Pipe monRing = monitorRings[j];

				 monitorStages[j] = new RingBufferMonitorStage(gm, rings[j], monRing);	
				 
				
				 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(4100000), monitorStages[j]);
				 
				 
				 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(4700000), new DumpMonitorStage(gm, monRing));
				 
			 }
		 }
		 		 

		 
		 //add all the stages start running
		 j = 0;
	 	 Pipe outputRing = rings[j];
	
		 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? new ProductionStageHighLevel(gm, outputRing) : new ProductionStageLowLevel(gm, outputRing));
				 
		 int i = stagesBetweenSourceAndSink;
		 while (--i>=0) {
			 if (useTap & 0==i) { //only do taps on first stage or this test could end up using many many threads.		 
				 
				 Pipe[] splitsBuffers = new Pipe[splits];
				 splitsBuffers[0] = rings[j+1];//must jump ahead because we are setting this early
				 assert(splitsBuffers[0].bitsOfSlabRing == ex+primaryBits);
				 if (splits>1) {
					 int k = splits;
					 while (--k>0) {
						 splitsBuffers[k] = new Pipe(new PipeConfig((byte)(primaryBits+ex), (byte)(secondaryBits+ex), null, RawDataSchema.instance));
						Pipe inputRing = splitsBuffers[k];
						boolean useRoute = useTap&useRouter;
						 ///
						 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? 
						 //new DumpStageStreamingConsumer(gm, inputRing, useRoute):
						 new DumpStageHighLevel(gm, inputRing, useRoute) :
						 new DumpStageLowLevel(gm, inputRing, useRoute));
					 }
				 } 
				 
				 
			     if (useRouter) {
			    	 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), new RoundRobinRouteStage(gm, rings[j++], splitsBuffers));
			     } else {
			    	 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), new ReplicatorStage(gm, rings[j++], splitsBuffers)); 
			     }
			 } else {			 
				 Pipe inputRing = rings[j++];
				Pipe outputRing1 = rings[j];
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? new CopyStageHighLevel(gm, outputRing1, inputRing) : new CopyStageLowLevel(gm, outputRing1, inputRing));		
			 }
			 
		 }
		 
	  	 Pipe inputRing = rings[j];
		 boolean useRoute = useTap&useRouter;
		 GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, Integer.valueOf(0), highLevelAPI ? 
			//	new DumpStageStreamingConsumer(gm, inputRing, useRoute):
			      new DumpStageHighLevel(gm, inputRing, useRoute) :
			      new DumpStageLowLevel(gm, inputRing, useRoute));
		 
		 System.out.println("########################################################## Testing "+ (highLevelAPI?"HIGH level ":"LOW level ")+(useTap? "using "+splits+(useRouter?" router ":" splitter "):"")+(monitor?"monitored":"")+" totalThreads:"+totalThreads);
		 
		 //start the timer		 
		 final long start = System.currentTimeMillis();
		 
		 GraphManager.enableBatching(gm);
		 ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 scheduler.playNice = false;
		 scheduler.startup();
		 
		 
		 //blocks until all the submitted runnables have stopped
		
			 //this timeout is set very large to support slow machines that may also run this test.
			boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (!cleanExit) {
				//dump the Queue data
				int k=0;
				while (k<rings.length){
					System.err.println(GraphManager.getRingProducer(gm, rings[k].id)+"  ->\n    "+rings[k].toString()+"  ->  "+GraphManager.getRingConsumer(gm, rings[k].id));										
					k++;
				}	
				System.err.println(GraphManager.getRingConsumer(gm, rings[k-1].id));
			}
			
			assertTrue("Test timed out, forced shut down of stages",cleanExit); //the tests are all getting cut here
			
			int t = rings.length;
			while (--t>=0) {
				assertFalse("Unexpected error in thread, see console output",Pipe.isShutdown(rings[t]));
			}
			if (monitor) {
				t = monitorRings.length;
				while (--t>=0) {
					assertFalse("Unexpected error in thread, see console output",Pipe.isShutdown(monitorRings[t]));
				}
			}
			
			long duration = System.currentTimeMillis()-start;
			
			long bytes = testMessages * (long)testArray.length;
			long bpSec = 0==duration ? 0 :(1000l*bytes*8l)/duration;
			
			long msgPerMs = 0==duration ? 0 :testMessages/duration;
			System.out.println("Bytes:"+bytes+"  Gbits/sec:"+(bpSec/1000000000f)+" stages:"+stages+" msg/ms:"+msgPerMs+" MsgSize:"+testArray.length);

				t = rings.length;
				while (--t>=0) {
					Pipe.shutdown(rings[t]);
				}
				if (monitor) {
					t = monitorRings.length;
					while (--t>=0) {
						Pipe.shutdown(monitorRings[t]);
					}
				}	 
		 
	}

}
