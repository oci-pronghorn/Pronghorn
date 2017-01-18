package com.ociweb.pronghorn.stage.network;

import java.util.Arrays;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParserReader;


public class RegulatedLoadTestStage extends PronghornStage{

	private static final int HANG_TIMEOUT_MS = 20_000;

	private static final Logger logger = LoggerFactory.getLogger(RegulatedLoadTestStage.class);
	
	private Pipe<NetResponseSchema>[] inputs;
	private Pipe<ClientHTTPRequestSchema>[] outputs;
	private int[]                    toSend;
	private int[]                    received;
	private final int                 count;
	private int                      shutdownCount;
	private long[][]                 times;
	private Histogram histRoundTrip;
	private final ClientCoordinator clientCoord;
	
	long totalMs = 0;
	long inFlight = 0;
	long totalReceived = 0;
	long totalExpected;
	
	private long[] connectionIdCache; 
	
	int port;
	String host;
	
	long start;
	long lastTime = System.currentTimeMillis();

	private final int limit;
	private String testFile;
	
	private final String expected=null;//"{\"x\":9,\"y\":17,\"groovySum\":26}";
	
	private final int usersPerPipe;
	private final String label;
	
	
	protected RegulatedLoadTestStage(GraphManager graphManager, Pipe<NetResponseSchema>[] inputs, Pipe<ClientHTTPRequestSchema>[] outputs, 
			                          int testSize, int inFlightLimit, String fileRequest, int usersPerPipe, int port, String host, String label, ClientCoordinator clientCoord) {
		super(graphManager, inputs, outputs);
		
		this.clientCoord = clientCoord;
		this.usersPerPipe = usersPerPipe;
		assert(1 == usersPerPipe);
		this.testFile = fileRequest;
		assert (inputs.length==outputs.length);
		this.limit = inFlightLimit;
		this.inputs = inputs;
		this.outputs = outputs;
		this.count = testSize/inputs.length;
		logger.info("Each pipe will be given a total of {} requests.",count);
		
		this.label = label;
		this.port = port;
		this.host = host;
	//	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 200_000_000, this); //5x per second
	}
	
	
	public long totalReceived() {
		return totalReceived;
	}
	
	@Override
	public void startup() {
		
		connectionIdCache = new long[100];
		Arrays.fill(connectionIdCache, -1);
		
		histRoundTrip = new Histogram(40_000_000_000L,0);
		
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
		
		
		histRoundTrip.outputPercentileDistribution(System.out, 1_000.0); //showing micro seconds.
		
	}

	long lastChecked = 0;
	StringBuilder workspaceSB = new StringBuilder();
	
	byte[] buff = new byte[64];
	byte[] workspace = new byte[256];
	TrieParserReader hostTrieReader = new TrieParserReader();
	
	@Override
	public void run() {
		
		long now = System.currentTimeMillis();
		
		if (now-lastTime > HANG_TIMEOUT_MS) {
			logger.error("ZZZZZZZZZZZZZZZZZZZZZZZ test is frozen, in flight {}",inFlight);
			
			int i = inputs.length;
			while (--i>=0) {
				System.err.println((inputs.length-i)+" Pending Rec:"+received[i]+" Pending Send:"+toSend[i]);
			}
			
			
			System.exit(-1);
		}
		
	//	int x = 3;
		
	//	while (--x>=0) 
		{
			
			int i;
			
			boolean didWork;
						
			didWork = true;
			while (didWork) {
				didWork=false;
				
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = inputs.length;
					while (--i>=0) {
						
						//System.out.println(inputs[i]);
						if (Pipe.hasContentToRead(inputs[i])) {
							didWork=true;
							
							final int msg = Pipe.takeMsgIdx(inputs[i]);
							
							switch (msg) {
								case NetResponseSchema.MSG_RESPONSE_101:
									Pipe.takeLong(inputs[i]);
									int meta = Pipe.takeRingByteMetaData(inputs[i]); //TODO: DANGER, write helper method that does this for low level users.
									int len = Pipe.takeRingByteLen(inputs[i]);
									int pos = Pipe.bytePosition(meta, inputs[i], len);
									
									
									
									if (null!=expected) {
										workspaceSB.setLength(0);
										int headerSkip = 8;
										Appendables.appendUTF8(workspaceSB, inputs[i].blobRing, pos+headerSkip, len-headerSkip, inputs[i].blobMask);								
										String tested = workspace.toString().trim();
										if (!expected.equals(tested)) {
											System.err.println("A error no match "+expected);
											System.err.println("B error no match "+tested);											
										}
									}
									
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
			
							
							lastTime = now;
							
							inFlight--;
							totalReceived++;
							
							int recIdx = --received[i];

							if (recIdx > 0 ) {
								long duration = System.nanoTime() - times[i][recIdx];								
								totalMs+=duration;
								if (duration < 4_000_000_000L) {
									histRoundTrip.recordValue(duration);
								}								
							} else {
								if (0!=toSend[i]) {
									throw new RuntimeException("received more responses than sent requests");
								}
								
//								if (Pipe.contentRemaining(inputs[i])==0) {
//									throw new RuntimeException("expected pipe to be empty upon last response but found "+inputs[i]);
//								}
								
								
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
								
				int pct = (int)((100L*totalReceived)/(float)totalExpected);
				if (lastChecked!=pct) {
				
					if (pct>96 || pct>=(lastChecked+5)) {
					
						long perMS = totalReceived/(now-start);					
						
						System.out.print(label);
						Appendables.appendValue(System.out, " test completed ", pct, "% ");
						Appendables.appendValue(System.out, " rpms ", perMS, "\n");
										
						lastChecked = pct;
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
			while (inFlight<limit && didWork) {  //250 maxes out the network connection for 3.3K file
				didWork = false;
			
				int j = usersPerPipe;
				while (--j >= 0) {
				
					i = outputs.length;
					while (--i >= 0) {
	
						if (toSend[i]>0 && inFlight<limit && Pipe.hasRoomForWrite(outputs[i])) {	
							didWork = true;

							int userId = i + (j * outputs.length); 
							int msdIdx;
														
							
							long connectionId = connectionIdCache[userId];
														
							
							if (connectionId == -1) {
								byte[] hByte = host.getBytes();
								System.arraycopy(hByte, 0, buff, 0, hByte.length);
								
								try {
									connectionId = clientCoord.lookup(buff, 0, hByte.length, 6, port, userId, workspace, hostTrieReader);
									assert(connectionId>0);
									connectionIdCache[userId] = connectionId;	
								} catch (Throwable t) {
									//ignore and try again later, for now let connectionId remain as -1
								}
							}							
							
							if (connectionId>=0) {
								msdIdx = ClientHTTPRequestSchema.MSG_FASTHTTPGET_200;
							} else {
								msdIdx = ClientHTTPRequestSchema.MSG_HTTPGET_100;
							}
							
							
							int size = Pipe.addMsgIdx(outputs[i], msdIdx);
														   

							
							toSend[i]--;	
							inFlight++;
							lastTime = now;
	
							Pipe.addIntValue(userId, outputs[i]);  
							Pipe.addIntValue(port, outputs[i]);
							Pipe.addUTF8(host, outputs[i]);

							if (connectionId>=0) {
								Pipe.addLongValue(connectionId, outputs[i]);
							}
							
							Pipe.addUTF8(testFile, outputs[i]);						
						
							
							times[i][toSend[i]] = System.nanoTime();
							
							Pipe.confirmLowLevelWrite(outputs[i], size);
							Pipe.publishWrites(outputs[i]);

				//			System.err.println("write "+msdIdx+" for size "+size+"   "+outputs[i]);
							
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
