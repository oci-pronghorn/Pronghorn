package com.ociweb.pronghorn.stage.network;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.BasicClientConnectionFactory;
import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerFactory;
import com.ociweb.pronghorn.network.ServerPipesConfig;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfigManager;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.struct.StructRegistry;

public class SocketIOStageTest {
	
	int clientWriters = 2;				
	int responseUnwrapCount = 1;
	int clientWrapperCount = 1;
	int responseQueue = 10;
	int releaseCount = 10;
	int netResponseCount = 10;
			
	int payloadSize = 16;//140; //SAME RESULTS EVEN WITH SMALL PAYLOAD
	int connectionsInBits = 9;
	int fromSocketBlocks = 128;
	int fromSocketBuffer = 1<<17;
	int maxRequestSize = payloadSize;
	int maxResponseSize = payloadSize;
	
	int totalSessions = 512;//256;
	int testSize = 20;//_000_000;		
	int port = (int) (2000 + (System.nanoTime()%12000));
	
	//TODO: why is return pipe 100% and red on graph
	//TODO: why is server pipe into echo always full?
	
	String host = "127.0.0.1";

	TLSCertificates tlsCertificates = null;
	
	ClientCoordinator ccm = new ClientCoordinator(connectionsInBits, totalSessions, tlsCertificates, new StructRegistry());

	
	public class EchoTest extends PronghornStage {

		private final Pipe<NetPayloadSchema>[] inputs;
		private final Pipe<NetPayloadSchema>[] outputs;
		private final int step;
		private int openCount;
		public EchoTest(GraphManager gm, boolean isClient, Pipe<NetPayloadSchema>[] inputs, Pipe<NetPayloadSchema>[] outputs, Pipe<ReleaseSchema>... release) {
			super(gm, inputs, join(outputs,release));
			this.inputs = inputs;
			this.outputs = outputs;
			this.openCount = inputs.length;
			this.step = isClient ? 1 : 0;
			assert(inputs.length == outputs.length) : "inputs: "+inputs.length+" vs outputs: "+outputs.length;
		}

		@Override
		public void startup() {
			
			if (0 != step) {
				/////////////////////
				//staring data
				////////////////////
				
				int hostId = ClientCoordinator.lookupHostId(host.getBytes());
				
				for(int i = 0; i<totalSessions; i++) {
					int sessionId = i+1;
					int responsePipeIdx = i;
					
					
					long liveConnectionId = ClientCoordinator.lookup(hostId, port, sessionId);
					
					
					
					ClientConnection con = ClientCoordinator.openConnection(ccm, hostId, port, 
											           sessionId, 
											           responsePipeIdx, 
											           outputs, 
											           liveConnectionId, 
											           BasicClientConnectionFactory.instance);
					liveConnectionId = con.id;
					
					Pipe.addMsgIdx(outputs[i], NetPayloadSchema.MSG_BEGIN_208);
					Pipe.addIntValue(0, outputs[i]);
					Pipe.confirmLowLevelWrite(outputs[i]);
					Pipe.publishWrites(outputs[i]);
					
					Pipe.addMsgIdx(outputs[i], NetPayloadSchema.MSG_PLAIN_210);
					Pipe.addLongValue(liveConnectionId, outputs[i]);
					Pipe.addLongValue(System.nanoTime(), outputs[i]);
					Pipe.addLongValue(0, outputs[i]);
					DataOutputBlobWriter<NetPayloadSchema> dataOut = Pipe.openOutputStream(outputs[i]);
					
					dataOut.writeInt(testSize);
					byte[] temp = new byte[payloadSize]; 
					Arrays.fill(temp, (byte)-8);			
					dataOut.write(temp);
					dataOut.closeLowLevelField();
				
					Pipe.confirmLowLevelWrite(outputs[i]);
					Pipe.publishWrites(outputs[i]);
	
				}
			}
		}
		
		
		@Override
		public void run() {

			
			boolean didWork;
			//do {
				didWork = false;
				int i = inputs.length;
				while (--i>=0) {
					Pipe<NetPayloadSchema> pipeIn = inputs[i];
					Pipe<NetPayloadSchema> pipeOut = outputs[i];
					didWork = consumePipe(didWork, pipeIn, pipeOut);	
				}
				
			//} while (didWork);
			
			
		}

		private final int readSizeOf = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
		
		private boolean consumePipe(boolean didWork, Pipe<NetPayloadSchema> pipeIn, Pipe<NetPayloadSchema> pipeOut) {
			if (Pipe.hasContentToRead(pipeIn) && Pipe.hasRoomForWrite(pipeOut)) {
				int msgIdx = Pipe.takeMsgIdx(pipeIn);
				if (msgIdx == NetPayloadSchema.MSG_PLAIN_210) {
					long con = Pipe.takeLong(pipeIn);						
					long arr = Pipe.takeLong(pipeIn);
					long pos = Pipe.takeLong(pipeIn);
					DataInputBlobReader<NetPayloadSchema> sourceData = Pipe.openInputStream(pipeIn);
					int iter = sourceData.readInt()-step;
					
					if (iter > 0) {
					
						didWork = true;
						publishCopy(pipeOut, con, sourceData, iter);
					
					} else {
						
						if (--openCount == 0) {								
							//if all are done....
							Pipe.publishEOF(outputs);
							requestShutdown();
						}
					}
					
					Pipe.confirmLowLevelRead(pipeIn, readSizeOf);
					Pipe.releaseReadLock(pipeIn);
					
					
				} else {
					
					Pipe.skipNextFragment(pipeIn, msgIdx);
					
				}
			}
			return didWork;
		}

		private void publishCopy(Pipe<NetPayloadSchema> pipeOut, long con,
				DataInputBlobReader<NetPayloadSchema> sourceData, int iter) {
			int size = Pipe.addMsgIdx(pipeOut, NetPayloadSchema.MSG_PLAIN_210);
			Pipe.addLongValue(con, pipeOut);
			Pipe.addLongValue(System.nanoTime(), pipeOut);
			Pipe.addLongValue(Pipe.workingHeadPosition(pipeOut), pipeOut);
			DataOutputBlobWriter<NetPayloadSchema> targetData = Pipe.openOutputStream(pipeOut);
			targetData.writeInt(iter);
			sourceData.readInto(targetData, sourceData.available()); //copy the payload back
			targetData.closeLowLevelField();
			Pipe.confirmLowLevelWrite(pipeOut, size);
			Pipe.publishWrites(pipeOut);
		}

	}


	
	private PronghornStage watch = null;

	//send 1 at a time to measure max throughput of the Socket reader/writers alone.
	@Test @Ignore
	public void testRoundTrip() {
		
		//disable groups so we can test single stages
		NetGraphBuilder.supportReaderGroups = false;
		
		GraphManager.showThreadIdOnTelemetry = true;
		
		
		ClientCoordinator.registerDomain(host);
		
		GraphManager gm = new GraphManager();

		//this rate is  10K per second, with 512 is 5120K per second but we only get 300K or so...
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 100_000);
		
				//TODO: Can we do all this a yaml and system props? - would be much better for dev ops and late changes..
				//      each server would have a prefix to read so they can all change independent.
				//      or point to yaml file to load from resources?
				final ServerPipesConfig serverConfig = new ServerPipesConfig(
						null,//we do not need to log the telemetry traffic, so null...
						null != tlsCertificates,
						connectionsInBits,
						totalSessions, //tracks 
						1,
						1,
						1,
						1,				
						//one message might be broken into this many parts
						fromSocketBlocks, 
						fromSocketBuffer,
						maxRequestSize,
						maxResponseSize,
						2, //requests in Queue, keep small
						2, //responses, keep small.
						new PipeConfigManager(),
						new PipeConfigManager());

		ServerConnectionStruct scs = new ServerConnectionStruct(gm.recordTypeData);
		ServerCoordinator coordinator = new ServerCoordinator(tlsCertificates,
				        host, port, scs,								
				        false, "RoundTrip Server","", serverConfig);
		
		NetGraphBuilder.buildServerGraph(gm, coordinator, new ServerFactory() {

			@Override
			public void buildServer(GraphManager graphManager, ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse,
					Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {
			
				
				new EchoTest(graphManager, false, receivedFromNet, sendingToNet, releaseAfterParse);
				
				
			}
			
		});


		Pipe<NetPayloadSchema>[] toServer = Pipe.buildPipes(totalSessions, NetPayloadSchema.instance.newPipeConfig(responseQueue, payloadSize+64));

		
		NetGraphBuilder.buildClientGraph(gm, ccm, responseQueue, toServer, responseUnwrapCount,
				         clientWrapperCount, clientWriters,
				         releaseCount, netResponseCount, new ClientResponseParserFactory() {

							@Override
							public void buildParser(GraphManager gm, ClientCoordinator ccm,
													Pipe<NetPayloadSchema>[] clearResponse,
													Pipe<ReleaseSchema>[] ackReleaseForResponseParser) {
								
								watch = new EchoTest(gm, true, clearResponse, toServer, ackReleaseForResponseParser);
												
							}
							
						});
	
	
		
		/////////////////
		//run
		////////////////
		gm.enableTelemetry(8098);
		StageScheduler sched = StageScheduler.defaultScheduler(gm);
		sched.startup();
		GraphManager.blockUntilStageTerminated(gm, watch);
		sched.shutdown();
	}


	

    
    
    
}
