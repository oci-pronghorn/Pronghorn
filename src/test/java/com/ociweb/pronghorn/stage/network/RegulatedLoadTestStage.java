package com.ociweb.pronghorn.stage.network;

import java.util.Arrays;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.SSLConnection;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParserReader;


public class RegulatedLoadTestStage extends PronghornStage{

	private static final int HANG_TIMEOUT_MS = 120_000;//Integer.MAX_VALUE;//10_000;

	private static final Logger logger = LoggerFactory.getLogger(RegulatedLoadTestStage.class);
	
	private Pipe<NetResponseSchema>[] inputs;
	private Pipe<ClientHTTPRequestSchema>[] outputs;

	private final int                 count;
	private int                      shutdownCount;
	private long[][]                 times;

	private final ClientCoordinator clientCoord;
	
	long totalMs = 0;
	long totalReceived = 0;
	long totalExpected;
	
	private long[] connectionIdCache; 
	private long[] toSend;
	private long[] toRecieve;
	private int[]  userIdFromConnectionId;
	
	int port;
	final String host;
	private byte[] hostBytes;
	
	long start;
	long lastTime = System.currentTimeMillis();

	private final String testFile;
	private byte[] testFileBytes;
	
	private final String expected=null;//"{\"x\":9,\"y\":17,\"groovySum\":26}";
	
	private final int usersPerPipe;
	private final String label;
	private final GraphManager graphManager;
	
	
	protected RegulatedLoadTestStage(GraphManager graphManager, Pipe<NetResponseSchema>[] inputs, Pipe<ClientHTTPRequestSchema>[] outputs, 
			                          int testSize, String fileRequest, int usersPerPipe, int port, String host, String label, ClientCoordinator clientCoord) {
		super(graphManager, inputs, outputs);
		
		this.clientCoord = clientCoord;
		this.usersPerPipe = usersPerPipe;
		this.graphManager = graphManager;

		this.testFile = fileRequest;
		this.testFileBytes = fileRequest.getBytes();
		assert (inputs.length==outputs.length);
		
		this.inputs = inputs;
		this.outputs = outputs;
		this.count = testSize/inputs.length;
		logger.info("Each pipe will be given a total of {} requests.",count);
		
		this.label = label;
		this.port = port;
		this.host = host;
		this.hostBytes = host.getBytes();

		supportsBatchedPublish = false;
		supportsBatchedRelease = false;
		
	}
	
	
	public long totalReceived() {
		return totalReceived;
	}
	
	@Override
	public void startup() {
		
		int conCount = outputs.length*usersPerPipe;
		
		connectionIdCache = new long[conCount];
		Arrays.fill(connectionIdCache, -1);
		
		toSend = new long[conCount];
		Arrays.fill(toSend, count);
		
		toRecieve = new long[conCount];
		Arrays.fill(toRecieve, count);
		
		userIdFromConnectionId = new int[conCount*4];

		
	//	histInFlight.copyCorrectedForCoordinatedOmission(expectedIntervalBetweenValueSamples)
		
		times = new long[inputs.length][count*usersPerPipe];
		
		shutdownCount = inputs.length;
		
		totalExpected = inputs.length * ((long)count*(long)usersPerPipe);
		
		start = System.currentTimeMillis();
		
		
	}
	
	@Override
	public void shutdown() {
		
		//long avg = total/ (count*inputs.length);
		//System.out.println("average ns "+avg);

		
//		int c = connectionIdCache.length;
//		while (--c>=0) {
//			if (-1 != connectionIdCache[c]) {
//				ClientConnection cc = (ClientConnection)clientCoord.get(connectionIdCache[c], 0);
//				if (null != cc) {				
//					logger.info("totals from {} sent {}  rec {}  inFlight {} toSend {} expectedToRec {} ",cc.getId(), cc.reqCount(), cc.respCount(), cc.inFlightCount(), toSend[c], toRecieve[c]);				
//				}
//			}
//		}			
				
				
				
	}

	long lastChecked = 0;
	StringBuilder workspaceSB = new StringBuilder();
	
	byte[] buff = new byte[64];
	byte[] workspace = new byte[256];
	TrieParserReader hostTrieReader = new TrieParserReader();
	
	@Override
	public void run() {
		
		long firstTime = System.currentTimeMillis();
		long now = firstTime;
		
		if (now-lastTime > HANG_TIMEOUT_MS) {
		
			logger.info("CRITICAL ERROR: Test is frozen");
			 int r = toRecieve.length;
			 while (--r>=0) {
				 if (toRecieve[r]>0) {
					 logger.info("still need {} for user {} ",toRecieve[r],r);
					 for(Pipe p : GraphManager.allPipes(graphManager)) {
						 if (Pipe.contentRemaining(p)>0) {
							 logger.info("found pipe with data {} ",p);
						 }
						 
					 };
					 
					 
				 }
			 }		
			 requestShutdown();
			 return;
		}

		assert(inputs.length == outputs.length);
						
		int i;
		boolean foundWork = consumeAllResults(now);		
		do {			
			printProgress(now);
			sendPendingRequests(now);				
			foundWork = consumeAllResults(now);
			now = System.currentTimeMillis();
			
		} while (foundWork);
	}


	private void printProgress(long now) {
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
	}

	
	int outputIdx = -1;
	int usr = -1;

	private void sendPendingRequests(long now) {
				
		boolean didWork;
		didWork = true;
		while (didWork) {  //250 maxes out the network connection for 3.3K file
			didWork = false;
		
			if (usr<0) {
				usr=usersPerPipe;
			}
			while (--usr >= 0) {
			
				if (outputIdx<0) {
					outputIdx = outputs.length;
				}
				
				while (--outputIdx >= 0) {
					
					final int userId = outputIdx + (usr * outputs.length); 
					if (Pipe.hasRoomForWrite(outputs[outputIdx]) ) {	
						if (toSend[userId]>0) {
							int msdIdx;													
							
							long connectionId = connectionIdCache[userId];
									
							{
								ClientConnection cc = (ClientConnection)clientCoord.get(connectionId);
				
								if (null==cc || (toRecieve[userId]-toSend[userId]) < cc.maxInFlight ) {  //limiting in flight
									didWork = true;
										
									boolean useSlow = (connectionId == -1); 
									if (useSlow) {
										System.arraycopy(hostBytes, 0, buff, 0, hostBytes.length);
										
										connectionId = clientCoord.lookup(buff, 0, hostBytes.length, 6, port, userId, workspace, hostTrieReader);
										
										if (-1!=connectionIdCache[userId]) {
											throw new UnsupportedOperationException("already set ");
										}
										connectionIdCache[userId] = connectionId;	
										assert(connectionId<Integer.MAX_VALUE);
										if (-1 != connectionId) {
											userIdFromConnectionId[(int)connectionId] = userId;
										}
										msdIdx = ClientHTTPRequestSchema.MSG_HTTPGET_100;
									} else {
										msdIdx = ClientHTTPRequestSchema.MSG_FASTHTTPGET_200;										
									}
									
									
									
									int size = Pipe.addMsgIdx(outputs[outputIdx], msdIdx);
																   
	
									lastTime = now;
			
									Pipe.addIntValue(userId, outputs[outputIdx]);  
									Pipe.addIntValue(port, outputs[outputIdx]);
									Pipe.addByteArray(hostBytes, 0, hostBytes.length, outputs[outputIdx]); // old	Pipe.addUTF8(host, outputs[i]);
	
									if (!useSlow) {
										Pipe.addLongValue(connectionId, outputs[outputIdx]);
									}
									
									Pipe.addByteArray(testFileBytes, 0, testFileBytes.length, outputs[outputIdx]); //Pipe.addUTF8(testFile, outputs[i]);						
								
									Pipe.confirmLowLevelWrite(outputs[outputIdx], size);
									Pipe.publishWrites(outputs[outputIdx]);
									toSend[userId]--;
								}
							}
						}
					} 

				}
				
			}
		}
	}


	private boolean consumeAllResults(long now) {
		boolean didWork = false;
		int i;
		int usr = usersPerPipe;
		while (--usr >= 0) {
		
			i = inputs.length;
			while (--i>=0) {

				Pipe<NetResponseSchema> pipe = inputs[i];
				
				while (Pipe.hasContentToRead(pipe)) {
					
					didWork=true;
									
					final int msg = Pipe.takeMsgIdx(pipe);
					
					switch (msg) {
						case NetResponseSchema.MSG_RESPONSE_101:
							long conId = Pipe.takeLong(pipe);
			
							//TODO: this is a serious issue we request on 1 pipe but they come back on another....
							int userIdx = userIdFromConnectionId[(int)conId];
							
							int meta = Pipe.takeRingByteMetaData(inputs[i]); //TODO: DANGER, write helper method that does this for low level users.
							int len = Pipe.takeRingByteLen(pipe);
							int pos = Pipe.bytePosition(meta, pipe, len);
							
							totalReceived++;
							if (--toRecieve[userIdx]==0) {
								if (--shutdownCount == 0) {
									//logger.info("XXXXXXX full shutdown now "+shutdownCount);
									requestShutdown();
									break;
								}
								
								//System.out.println("shutodown remaning "+shutdownCount+" for user "+userId);
							}
							
							
							if (false) {									
								testExpectedValues(i, len, pos);
							}
							
							break;
						case NetResponseSchema.MSG_CLOSED_10:
							
							int meta2 = Pipe.takeRingByteMetaData(pipe);
							int len2 = Pipe.takeRingByteLen(pipe);
							Pipe.bytePosition(meta2, pipe, len2);
							
							Pipe.takeInt(pipe);
							break;
						case -1:
							//EOF
							break;
						default:
							throw new UnsupportedOperationException(msg+"  "+pipe.toString());
					}
					Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetResponseSchema.instance, msg));
					Pipe.releaseReadLock(pipe);
								
					lastTime = now;			

				}	
				
				
				
			}
		}
		return didWork;
	}


	private void testExpectedValues(int i, int len, int pos) {
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
	}

}
