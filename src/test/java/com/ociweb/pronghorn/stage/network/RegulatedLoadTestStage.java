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
	
	long totalMs = 0;
	long inFlight = 0;
	long totalReceived = 0;
	long totalExpected;
	
	long start;
	long lastTime = System.currentTimeMillis();

	private final int limit;
	private String testFile;
	
	private final int usersPerPipe;

	
	protected RegulatedLoadTestStage(GraphManager graphManager, Pipe<NetResponseSchema>[] inputs, Pipe<NetRequestSchema>[] outputs, 
			                          int testSize, int inFlightLimit, String fileRequest, int usersPerPipe) {
		super(graphManager, inputs, outputs);
		
		this.usersPerPipe = usersPerPipe;
		this.testFile = fileRequest;
		assert (inputs.length==outputs.length);
		this.limit = inFlightLimit;
		this.inputs = inputs;
		this.outputs = outputs;
		this.count = testSize/inputs.length;
		logger.info("Each pipe will be given a total of {} requests.",count);
		
		
	//	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 200_000_000, this); //5x per second
	}
	
	
	public long totalReceived() {
		return totalReceived;
	}
	
	@Override
	public void startup() {
		
		histRoundTrip = new Histogram(40_000_000_000L,0);
		histInFlight  = new Histogram(10_000_000,0);
		
	//	histInFlight.copyCorrectedForCoordinatedOmission(expectedIntervalBetweenValueSamples)
		
		times = new long[inputs.length][count*usersPerPipe];
		
		shutdownCount = inputs.length;
		toSend = new int[outputs.length];
		Arrays.fill(toSend, count*usersPerPipe);
		
		received = new int[inputs.length];
		Arrays.fill(received, count*usersPerPipe);
		
		totalExpected = inputs.length * ((long)count*(long)usersPerPipe);
		
		start = System.currentTimeMillis();
	}
	
	@Override
	public void shutdown() {
		
		//long avg = total/ (count*inputs.length);
		//System.out.println("average ns "+avg);
		
		
		histRoundTrip.outputPercentileDistribution(System.out, 1_000_000.0); //showing ms.
		
		histInFlight.outputPercentileDistribution(System.out, 1d);
		
		
	}

	long lastChecked = 0;
	
	@Override
	public void run() {
		
		long now = System.currentTimeMillis();
		
		if (now-lastTime > 90_000) {
			logger.error("ZZZZZZZZZZZZZZZZZZZZZZZ test is frozen, in flight {}",inFlight);
			
			int i = inputs.length;
			while (--i>=0) {
				System.err.println((inputs.length-i)+" "+received[i]+"  "+toSend[i]);
			}
			
			
			System.exit(-1);
		}
		
		//while (true)
		{
			
			int i;
			
			boolean didWork;
			
			didWork = true;
			while (didWork) {
				didWork = false;
				
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = inputs.length;
					while (--i>=0) {
						
						//System.out.println(inputs[i]);
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
							lastTime = now;
							
							inFlight--;
							totalReceived++;
							
							int recIdx = --received[i];

						  
								long duration = System.nanoTime() - times[i][recIdx];
								
								totalMs+=duration;
								
								if (duration < 4_000_000_000L) {
									histRoundTrip.recordValue(duration);
								}
						  
						    
								histInFlight.recordValue(inFlight);
								
				
								if (recIdx <=0 ) {
									System.out.println("shutdown "+shutdownCount+" "+i);
									if (--shutdownCount == 0) {
										logger.info("XXXXXXX full shutdown now "+shutdownCount);
										requestShutdown();
										return;
									}
								}
			
						}	
					}
				}
			}
			
			boolean debug = true;
			if (debug) {
								
				if (lastChecked!=totalReceived) {
					float pct = (100L*totalReceived)/(float)totalExpected;
					
					System.out.println("total load test received "+totalReceived+"  "+pct+"%"); //TODO: what is the total expected.
					lastChecked = totalReceived;
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
			while (inFlight<limit && didWork) {  //250 maxes out the network connection for 3.3K file
				didWork = false;
			
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = outputs.length;
					while (--i >= 0) {
	
						if (toSend[i]>0 && inFlight<limit && Pipe.hasRoomForWrite(outputs[i])) {	
									
							int size = Pipe.addMsgIdx(outputs[i], NetRequestSchema.MSG_HTTPGET_100);

							toSend[i]--;	
							inFlight++;
							didWork = true;
							lastTime = now;
	
							Pipe.addIntValue(8443, outputs[i]);
							Pipe.addUTF8("127.0.0.1", outputs[i]);
							Pipe.addUTF8(testFile, outputs[i]);
							
							Pipe.addIntValue(i + (j * outputs.length), outputs[i]);            //TODO: need to add additional connections per round per connection.
	
							times[i][toSend[i]] = System.nanoTime();
							
							Pipe.confirmLowLevelWrite(outputs[i], size);
							Pipe.publishWrites(outputs[i]);
							
							//if (0==toSend[i]) {
								
								//								System.out.println("finished requesting "+i);
							//}
							
						}					
					}
					
					
				}
			}
					
		}
			
	}

}
