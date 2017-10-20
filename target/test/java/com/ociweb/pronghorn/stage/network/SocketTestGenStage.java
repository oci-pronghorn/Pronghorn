package com.ociweb.pronghorn.stage.network;

import java.io.IOException;
import java.util.Random;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SocketTestGenStage extends PronghornStage {

	private final boolean useTLS=false;
	private final String host = "127.0.0.1";
	
	private final Pipe<NetPayloadSchema>[] pipe;
	private final int testUsers;
	private final int[] testSeeds;
	private final int[] testSizes;
	private final ClientCoordinator clientCoordinator;
	
	private int userIdx;
	private int testIdx;

	private final int port;
	
	public SocketTestGenStage(GraphManager gm, Pipe<NetPayloadSchema>[] pipe, int testUsers, int[] testSeeds, int[] testSizes, ClientCoordinator clientCoordinator, int port) {
		super(gm,NONE,pipe);
		
		this.pipe = pipe;
		this.testUsers = testUsers;
		this.testSeeds = testSeeds;
		this.testSizes = testSizes;
		this.port      = port;
		
		this.userIdx = testUsers-1;
		this.testIdx = testSeeds.length-1;

		
		this.clientCoordinator = clientCoordinator;
	}
	

	@Override
	public void run() {
		
		byte[] hostBytes = host.getBytes();
		while (true) {
			
			
			//////////
			//crazy connection open logic  (need to simplify)
			/////////
			int hostLen = hostBytes.length;
			int hostMask = Integer.MAX_VALUE;
			ClientConnection connection = ClientCoordinator.openConnection(
					clientCoordinator, host, port, userIdx, pipe,	
			        clientCoordinator.lookup(host, port, userIdx));			
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
			
			if (--userIdx<0) {
				userIdx = testUsers-1;					
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
