package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RoundRobinRouteStage extends PronghornStage {

	RingBuffer inputRing;
	RingBuffer[] outputRings;
	int targetRing;
	int targetRingInit;
	int msgId = -2;
	
	public RoundRobinRouteStage(GraphManager gm, RingBuffer inputRing, RingBuffer ... outputRings) {
		super(gm,inputRing,outputRings);
		this.inputRing = inputRing;
		this.outputRings = outputRings;
		this.targetRingInit = outputRings.length-1;
		this.targetRing = targetRingInit;
		
		this.supportsBatchedPublish = false;
		this.supportsBatchedRelease = false;
		
		//TODO:AAAA must confirm that the output rings are just as big or bigger than inputs and that both have the same schema!!
		
		
	}

	@Override
	public void run() {		
		processAvailData(this);
	}

	//TODO: AA, May need NEW round robin stage that is low level and only works with messages
	//TODO: AA, May also need to add cursor to head of all fragments.
	
	private static void processAvailData(RoundRobinRouteStage stage) {
		
		do {
		
			if (-2==stage.msgId) {
				if (RingReader.tryReadFragment(stage.inputRing)) {
					if ((stage.msgId = RingReader.getMsgIdx(stage.inputRing))<0) {
						oldShutdown(stage);
						return;
					}		
				} else {
					return;
				}
			}
	
			if (RingReader.tryMoveSingleMessage(stage.inputRing, stage.outputRings[stage.targetRing])) {
				RingReader.releaseReadLock(stage.inputRing);
				if (--stage.targetRing<0) {
					stage.targetRing = stage.targetRingInit;
				}				
				stage.msgId = -2;
			} else {
				return;
			}

		} while(true);	

	}

	@Deprecated
	private static void oldShutdown(RoundRobinRouteStage stage) {
		//new Exception("warning old shutdown is used").printStackTrace();;
		
		//send the EOF message to all of the targets.
		int i = stage.outputRings.length;
		while (--i>=0) {
			RingBuffer.publishAllBatchedWrites(stage.outputRings[i]);
		}
		RingReader.releaseReadLock(stage.inputRing);
		stage.msgId = -2;
		stage.requestShutdown();
	}
	
	
}
