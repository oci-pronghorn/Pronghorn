package com.ociweb.pronghorn.stage.network;

import static com.ociweb.pronghorn.pipe.Pipe.*;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.code.GGSGenerator;
import com.ociweb.pronghorn.code.GVSValidator;
import com.ociweb.pronghorn.code.TestFailureDetails;
import com.ociweb.pronghorn.network.mqtt.IdGenStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class IdGenStageBehavior{

	private static final Logger log = LoggerFactory.getLogger(IdGenStageBehavior.class);

	
	public static GGSGenerator generator(final long testDuration) {
		return new GGSGenerator() {
			
			private static final int ABS_MAX_INFLIGHT_TESTED = 12;//1000;//TODO: B, MOVE UP TO 64k
			private final int length = 1<<16;
			private final int mask = length-1;
			
			private final int[] values = new int[length]; //maximum of 65536 id values
			private long head = 0;
			private long tail = 0;
			private long stopTime = System.currentTimeMillis()+testDuration;
			
			@Override
			public boolean generate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, Random random) {
				final int theOneMessage = 0;
				int sizeOfFragment = Pipe.from(inputs[0]).fragDataSize[theOneMessage];
				int maxReturnBlockSize = 1+random.nextInt(2*IdGenStage.MAX_BLOCK_SIZE);// 1 up to 2x the block size used for reserving ranges
				

				//mix up the ranges to simulate late returning values in fllight
				int maxMix = random.nextInt(ABS_MAX_INFLIGHT_TESTED);//max in flight
				int oneFailureIn = 10000; //NOTE: as this ratio gets worse the speed drops off
				//NOTE: this does not test any "lost" ids, TODO: C, this would be a good test to add later.
				
				long mixStart = Math.max(tail, head-maxMix);
				long k = head-mixStart;
				//do the shuffle.
				while (--k>1) {
					if (0==random.nextInt(oneFailureIn)) {
						assert(head-k>=tail);
						assert(head-(k-1)>=tail);
						assert(head-k<head);
						assert(head-(k-1)<head);
						//mix forward to values can move a long way
						int temp = values[mask&(int)(head-(k-1))];
						values[mask&(int)(head-(k-1))] = values[mask&(int)(head-k)];
						values[mask&(int)(head-k)] = temp;
						
					}					
				}
									
				assert(1==outputs.length) : "IdGen can only support a single queue of release ranges";
				
				
				Pipe outputRing = outputs[0];
				while ( tail<head && hasRoomForWrite( outputRing, sizeOfFragment)) {
				    
					addMsgIdx(outputRing, theOneMessage);	
					
					//assemble a single run
					long t = tail;
					{
						int last =  -1;
						int value = values[mask&(int)tail];
						int limit = maxReturnBlockSize;
						do {
							last = value;
							value = values[mask&(int)++t];
						} while (1+last==value && t<head && --limit>=0);

					}
					//publish tail to t exclusive	
					int value = values[mask&(int)tail];
					long run = t-tail;
					int rangeEnd = value+(int)run;
					
					int range = IdGenStage.buildRange(value,rangeEnd);
			
					log.debug("Generator produce range {}->{}",value,rangeEnd);
										
					tail = t;
					
					Pipe.addIntValue(range, outputRing);
					
					publishWrites(outputRing);
					Pipe.confirmLowLevelWrite(outputRing, sizeOfFragment);
				}
								
				
				//get new ranges
				int i = inputs.length;
				while (--i>=0) {
					Pipe inputRing = inputs[i];
					while (Pipe.hasContentToRead( inputRing, sizeOfFragment) && ((tail+65534)-head)>IdGenStage.MAX_BLOCK_SIZE ) {
						int msgIdx = Pipe.takeMsgIdx(inputRing);
						assert(theOneMessage == msgIdx);
						
						int range = Pipe.takeInt(inputRing);
						
						int idx = IdGenStage.rangeBegin(range);						
						int limit = IdGenStage.rangeEnd(range);
						
						log.debug("Generator consume range {}->{}",idx,limit);
						
						int count = limit-idx;
						if (count > IdGenStage.MAX_BLOCK_SIZE) {
							System.err.println("TOO many in one call "+count); //TODO: AAA,  move test to validation
							return false;
						}
												
						while(idx<limit) {//room is guaranteed by head tail check in while definition														
							values[mask & (int)head++] = idx++;
						}						
						
						Pipe.releaseReads(inputRing);
						Pipe.confirmLowLevelRead(inputRing, sizeOfFragment);
					}
				}
	
				
				return System.currentTimeMillis()<stopTime;
				
			}
		};
	}

	public static GVSValidator validator() {
			return new GVSValidator() {
	
				final byte[] testSpace = new byte[1 << 16];// 0 to 65535
				final int theOneMessage = 0;
				long total=0;
				final String[] label = new String[]{"Tested","Generator"};
				
				@Override
				public TestFailureDetails validate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs) {
					int sizeOfFragment = Pipe.from(inputs[0]).fragDataSize[theOneMessage];
					assert(inputs.length == outputs.length);
					assert(2 == outputs.length);
	
					byte j = 2; // 0 we expect to find 0 and change to 1, 1 we
								// expect to find 1 and change to zero.
					
					//TODO: remove loop
					while (--j >= 0) {
	
						
						final byte expected = j;
						final byte toggle = (byte) ((expected + 1) & 1);
						
						boolean contentToLowLevelRead = Pipe.hasContentToRead(inputs[expected], sizeOfFragment);
						boolean roomToLowLevelWrite = hasRoomForWrite(outputs[toggle], sizeOfFragment);
											
						if (contentToLowLevelRead && roomToLowLevelWrite) {
	
							int msgIdx = Pipe.takeMsgIdx(inputs[expected]);
							if (theOneMessage != msgIdx) {
								System.err.println(label[expected]+" bad message found "+msgIdx);
								return new TestFailureDetails("Corrupt Feed");
							};
	
							final int range = Pipe.takeInt(inputs[expected]);
							final int start = IdGenStage.rangeBegin(range);
							final int limit = IdGenStage.rangeEnd(range);
							
							log.debug("Validation "+label[expected]+" to "+label[toggle]+" range {}->{} ",start,limit);
							
							int count = limit-start;
							if (count<1) {
								System.err.println(label[expected]+" message must contain at least 1 value in the range, found "+count+" in value "+Integer.toHexString(range));
								
								return new TestFailureDetails("Missing Data");
							}
												
							total += (long)count;
							// test for the expected values
							int failureIdx = -1;
							boolean exit = false;
							int idx = start;
							while (idx < limit) {
								if (expected != testSpace[idx]) {
									if (failureIdx < 0) {
										failureIdx = idx;
										exit = true;
									}								
								} else {
									if (failureIdx>=0) {								
										System.err.println("Validator found "+label[expected]+" produced toggle error at "+failureIdx+" up to "+idx+" expected "+expected+" released up to "+limit);
										failureIdx = -1;
									}								
								}
								
								testSpace[idx++] = toggle;
							}
							if (failureIdx>=0) {								
								System.err.println("Validator found "+label[expected]+" produced toggle error at "+failureIdx+" up to "+limit+" expected "+expected);
								
							    exit = true;
							}
							if (exit) {
								
								return new TestFailureDetails("Bad Match on input range "+IdGenStage.rangeBegin(range)+"->"+IdGenStage.rangeEnd(range));
								
							}
							
	
							addMsgIdx(outputs[toggle], msgIdx);
							Pipe.addIntValue(range, outputs[toggle]);
							publishWrites(outputs[toggle]);
							Pipe.confirmLowLevelWrite(outputs[toggle], sizeOfFragment);
	
							Pipe.releaseReadLock(inputs[expected]);
							Pipe.confirmLowLevelRead(inputs[expected], sizeOfFragment);
						}
					}
					return null;
				}
	
				@Override
				public String status() {
					return "Total Values Checked:"+Long.toString(total>>1);
				}
			};
		}

}
