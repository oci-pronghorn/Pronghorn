package com.ociweb.pronghorn.ring.route;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;

public class RoundRobinRouteStage implements Runnable {

	RingBuffer inputRing;
	RingBuffer[] outputRings;
	int targetRing;
	int targetRingInit;
	
	public RoundRobinRouteStage(RingBuffer inputRing, RingBuffer ... outputRings) {
		this.inputRing = inputRing;
		this.outputRings = outputRings;
		this.targetRingInit = outputRings.length-1;
		this.targetRing = targetRingInit;
	}

	@Override
	public void run() {
		
		assert(Thread.currentThread().isDaemon()) : "This stage can only be run with daemon threads";
		if (!Thread.currentThread().isDaemon()) {
			throw new UnsupportedOperationException("This stage can only be run with daemon threads");
		}

		try{			
			while (processAvailData(this)) {
				Thread.yield();
			}
		} catch (Throwable t) {
			RingBuffer.shutdown(inputRing);
			int i = outputRings.length;
			while(--i>=0) {
				RingBuffer.shutdown(outputRings[i]);
			}
			
		}
	}

	private static boolean processAvailData(RoundRobinRouteStage stage) {
		
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
