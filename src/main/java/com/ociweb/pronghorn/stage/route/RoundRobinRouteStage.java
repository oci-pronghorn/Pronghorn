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
	}

	@Override
	public void run() {		
		processAvailData(this);
	}

	//TODO: AA, May need NEW round robin stage that is low level and only works with messages
	//TODO: AA, May also need to add cursor to head of all fragments.
	
	private static void processAvailData(RoundRobinRouteStage stage) {
		
		if (-2==stage.msgId && RingReader.tryReadFragment(stage.inputRing)) {
			stage.msgId = RingReader.getMsgIdx(stage.inputRing);
			if (stage.msgId<0) {
				oldShutdown(stage);
				return;
			}		
		}	
			
		if (stage.msgId>=0) {
			if (RingReader.tryMoveSingleMessage(stage.inputRing, stage.outputRings[stage.targetRing])) {
				if (--stage.targetRing<0) {
					stage.targetRing = stage.targetRingInit;
				}
				stage.msgId = -2;
				return;
			}
		}
		return;
	}

	private static void oldShutdown(RoundRobinRouteStage stage) {
		//send the EOF message to all of the targets.
		int i = stage.outputRings.length;
		while (--i>=0) {
			RingBuffer.publishAllWrites(stage.outputRings[i]);
		}
		RingReader.releaseReadLock(stage.inputRing);
		stage.msgId = -2;
	}
	
	
}
