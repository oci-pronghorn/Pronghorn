package com.ociweb.pronghorn.stage.network;

import java.util.Random;

import com.ociweb.pronghorn.network.BasicClientConnectionFactory;
import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParserReader;

/**
 * _no-docs_
 */
public class SocketTestGenStage extends PronghornStage {

	private final TrieParserReader READER = new TrieParserReader(true);
	private final boolean useTLS=false;
	private final String host = "127.0.0.1";
	
	private final Pipe<NetPayloadSchema>[] pipe;
	private final int testUsers;
	private final int[] testSeeds;
	private final int[] testSizes;
	private final ClientCoordinator clientCoordinator;
	
	private int sessionId;
	private int testIdx;

	private final int port;
	
	public SocketTestGenStage(GraphManager gm, Pipe<NetPayloadSchema>[] pipe, int testUsers, int[] testSeeds, int[] testSizes, ClientCoordinator clientCoordinator, int port) {
		super(gm,NONE,pipe);
		
		this.pipe = pipe;
		this.testUsers = testUsers;
		this.testSeeds = testSeeds;
		this.testSizes = testSizes;
		this.port      = port;
		
		this.sessionId = testUsers-1;
		this.testIdx = testSeeds.length-1;

		
		this.clientCoordinator = clientCoordinator;
	}
	

	@Override
	public void run() {
		
		byte[] hostBytes = host.getBytes();
		while (true) {
			
			//this.payloadToken = schema.growStruct(structId, BStructTypes.Blob, 0, "payload".getBytes());
			//only add header support for http calls..
			//int structureId = 
			
			
			//////////
			//crazy connection open logic  (need to simplify)
			/////////
			int hostId = ClientCoordinator.lookupHostId(hostBytes);
			long lookup = clientCoordinator.lookup(hostId, port, sessionId);
			
			int responsePipeIdx = 0;//
			
			
			ClientConnection connection = ClientCoordinator.openConnection(
					clientCoordinator, hostId, port, sessionId, responsePipeIdx, pipe,	
			        lookup, BasicClientConnectionFactory.instance);
			
			if (null==connection) { //returns non null if this connection is open and ready for use.
				return;//try again later
			}
			
			/////////////
			//send test data
			////////////
			int useUsersPipe = connection.requestPipeLineIdx(); //this lets us return to the same pipe we registered with	
			long connectionId = connection.getId();
			if (!sendSingleMessage(pipe[useUsersPipe], connectionId, testSeeds[testIdx], testSizes[testIdx])){
				return;//try again later
			}			
			
			///////////////
			//inc to next test datum
			///////////////
			
			if (--sessionId<0) {
				sessionId = testUsers-1;					
				if (--testIdx<0) {
					//System.out.println("finished sending all data");
					//all done
					requestShutdown();
					return;
				}
			}			
		}
		
	}

	private boolean sendSingleMessage(Pipe<NetPayloadSchema> pipe, long connectionId, int testSeed, int testSize) {
		
		Random r = new Random(testSeed);
		//System.out.println("sending "+connectionId+" "+testSize);
		
		if (Pipe.hasRoomForWrite(pipe)) {
			
			int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
			Pipe.addLongValue(connectionId, pipe);
			Pipe.addLongValue(System.currentTimeMillis(), pipe);
			Pipe.addLongValue(0, pipe);
			
			byte[] payload = new byte[testSize];
			r.nextBytes(payload);
			
			Pipe.addByteArray(payload, 0, testSize, pipe);
			
			Pipe.confirmLowLevelWrite(pipe, size);
			Pipe.publishWrites(pipe);
			
			return true;
		} else {
			return false;
		}
		
	}

	
	
	
	
}
