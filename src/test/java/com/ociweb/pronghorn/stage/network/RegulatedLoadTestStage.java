package com.ociweb.pronghorn.stage.network;

import java.util.Arrays;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class RegulatedLoadTestStage extends PronghornStage{

	private static final Logger logger = LoggerFactory.getLogger(RegulatedLoadTestStage.class);
	
	private Pipe<NetResponseSchema>[] inputs;
	private Pipe<NetRequestSchema>[] outputs;
	private int[]                    toSend;
	private int[]                    received;
	private final int                 count;
	private int                      shutdownCount;
	private long[][]                 times;
	private Histogram histRoundTrip;
	private Histogram histInFlight;
	
	long total = 0;
	long inFlight = 0;
	long start;
	
	final int stepSize = 1<<17;
	final int usersPerPipe = 1; //bumping this up creates more users while using the same pipe count //TODO: rethink this it does not work.

	
	protected RegulatedLoadTestStage(GraphManager graphManager, Pipe<NetResponseSchema>[] inputs, Pipe<NetRequestSchema>[] outputs, int testSize) {
		super(graphManager, inputs, outputs);
		
		assert (inputs.length==outputs.length);
		
		this.inputs = inputs;
		this.outputs = outputs;
		this.count = testSize/inputs.length;
		logger.info("Each pipe will be given a total of {} requests.",count);
		
		
		GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 200_000_000, this); //5x per second
	}
	
	@Override
	public void startup() {
		
		histRoundTrip = new Histogram(40_000_000_000L,0);
		histInFlight  = new Histogram(10_000_000,0);
		
	//	histInFlight.copyCorrectedForCoordinatedOmission(expectedIntervalBetweenValueSamples)
		
		times = new long[inputs.length][count];
		
		shutdownCount = inputs.length;
		toSend = new int[outputs.length];
		Arrays.fill(toSend, count*usersPerPipe);
		
		received = new int[inputs.length];
		Arrays.fill(received, count*usersPerPipe);
		
		start = System.currentTimeMillis();
	}
	
	@Override
	public void shutdown() {
		
		//long avg = total/ (count*inputs.length);
		//System.out.println("average ns "+avg);
		
		
		histRoundTrip.outputPercentileDistribution(System.out, 1_000.0); //showing microseconds.
		
		histInFlight.outputPercentileDistribution(System.out, 1d);
		
		
	}

	@Override
	public void run() {
		String testFile = "/SQRL.svg";
	//	String testFile = "/OCILogo.png";
		

		//while (true)
		{
			
			int i;
			
			boolean didWork;
			
			didWork = true;
			while (didWork) {
				didWork = false;
				
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = outputs.length;
					while (--i>=0) {
						
						if (Pipe.hasContentToRead(inputs[i])) {
							int msg = Pipe.takeMsgIdx(inputs[i]);
							
							switch (msg) {
								case NetResponseSchema.MSG_RESPONSE_101:
									Pipe.takeLong(inputs[i]);
									int meta = Pipe.takeRingByteMetaData(inputs[i]); //TODO: DANGER, write helper method that does this for low level users.
									int len = Pipe.takeRingByteLen(inputs[i]);
									Pipe.bytePosition(meta, inputs[i], len);
									
									break;
								case NetResponseSchema.MSG_CLOSED_10:
									
									int meta2 = Pipe.takeRingByteMetaData(inputs[i]);
									int len2 = Pipe.takeRingByteLen(inputs[i]);
									Pipe.bytePosition(meta2, inputs[i], len2);
									
									Pipe.takeInt(inputs[i]);
									break;
								case -1:
									//EOF
									break;
							}
							Pipe.confirmLowLevelRead(inputs[i], Pipe.sizeOf(inputs[i], msg));
							Pipe.releaseReadLock(inputs[i]);
			
							
							didWork = true;	
												
							int recIdx = --received[i];
							
							//	System.err.println("xxxxxxxx  "+recIdx);
								long duration = System.nanoTime() - times[i][recIdx];
								
								inFlight--;
								total+=duration;
								
								if (duration < 4_000_000_000L) {
									histRoundTrip.recordValue(duration);
								}
								
								histInFlight.recordValue(inFlight);
								
				
								if (recIdx == 0) {
									System.out.println("shutdown "+shutdownCount+" "+i);
									if (--shutdownCount == 0) {
										System.out.println("XXXXXXX full shutdown now "+shutdownCount);
										requestShutdown();
										return;
									}
								}
			
						}	
					}
				}
			}
			
			//TODO: note: red is not 80% on the charts it is probably more near 50% when the pipe is fully loaded we have more contention and throughput drops off.
			//     Keep in mind balance, we want short pipes however keeping pipes half full may be much more important.
			
			//do not overload the server.
			
			//////////////
			//Math
			//
			//  .031035 ms per call
			// gives us 32221.686 per second
			//          * 3.3K * 8 / 1024
			// gives us 830 mbps       7.5 ms latency
			//////////////
			
			//if we start with 900 mbps 
			//     send end 3.3K * 8 or 26.4K bits
			//   then we can only send 34908.16 files per second
			//   
			// maximum of 250 is a result of the max that can wait in the pipes

			
			didWork = true;
			while (inFlight<200 && didWork) {  //250 maxes out the network connection for 3.3K file
				didWork = false;
			
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = outputs.length;
					while (--i >= 0) {
	
						if (toSend[i]>0 && Pipe.hasRoomForWrite(outputs[i])) {	
									
							int size = Pipe.addMsgIdx(outputs[i], NetRequestSchema.MSG_HTTPGET_100);
							
							toSend[i]--;	
							inFlight++;
							didWork = true;
	
							Pipe.addIntValue(8443, outputs[i]);
							Pipe.addUTF8("127.0.0.1", outputs[i]);
							Pipe.addUTF8(testFile, outputs[i]);
							Pipe.addIntValue(i + (j * stepSize), outputs[i]);            //TODO: need to add additinal connections per round per connection.
	
							times[i][toSend[i]] = System.nanoTime();
							
							Pipe.confirmLowLevelWrite(outputs[i], size);
							Pipe.publishWrites(outputs[i]);
							
							if (0==toSend[i]) {
								System.out.println("finished requesting "+i);
							}
						}					
					}				
				}
			}
					
		}
			
	}

}
