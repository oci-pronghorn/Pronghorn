package com.ociweb.pronghorn.ring.route;

import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

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
	public void run() {		
		processAvailData(this);
	}

	private static boolean processAvailData(RoundRobinRouteStage2 stage) {
		
		if (RingReader.tryReadFragment(stage.inputRing)) {
			
			if (RingReader.getMsgIdx(stage.inputRing)<0) {
				RingReader.releaseReadLock(stage.inputRing);
				//send the EOF message to all of the targets.
				int i = stage.outputRings.length;
				while (--i>=0) {
					RingBuffer ring = stage.outputRings[i];
					RingBuffer.spinBlockOnTail(tailPosition(ring), headPosition(ring) - (ring.maxSize-RingBuffer.EOF_SIZE), ring);
					RingBuffer.publishEOF(ring);
				}
				return false;//exit with EOF
			}			
			RingBuffer ring = stage.outputRings[stage.targetRing];
			while (!RingReader.tryMoveSingleMessage(stage.inputRing, ring)) { //TODO: B, rewite so it does not block on write
			}
			if (--stage.targetRing<0) {
				stage.targetRing = stage.targetRingInit;
			}
		}	
		return true;
	}
	
	
}
