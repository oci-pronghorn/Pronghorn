package com.ociweb.pronghorn.ring.route;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

public class RoundRobinRouteStage2 extends PronghornStage {

	RingBuffer inputRing;
	RingBuffer[] outputRings;
	int targetRing;
	int targetRingInit;
	
	public RoundRobinRouteStage2(GraphManager gm, RingBuffer inputRing, RingBuffer ... outputRings) {
		super(gm,inputRing,outputRings);
		this.inputRing = inputRing;
		this.outputRings = outputRings;
		this.targetRingInit = outputRings.length-1;
		this.targetRing = targetRingInit;
	}

	@Override
	public boolean exhaustedPoll() {		
		return processAvailData(this);
	}

	private static boolean processAvailData(RoundRobinRouteStage2 stage) {
		
		if (RingReader.tryReadFragment(stage.inputRing)) {
			
			if (RingReader.getMsgIdx(stage.inputRing)<0) {
				return false;//exit with EOF
			}			
			RingBuffer ring = stage.outputRings[stage.targetRing];
			while (!RingReader.tryMoveSingleMessage(stage.inputRing, ring)) {
			}
			if (--stage.targetRing<0) {
				stage.targetRing = stage.targetRingInit;
			}
		}	
		return true;
	}
	
	
}
