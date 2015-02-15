package com.ociweb.pronghorn.ring.route;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWalker;

public class RoundRobinRouteStage implements Runnable {

	RingBuffer inputRing;
	RingBuffer[] outputRings;
	int targetRing;
	int totalRings;
	
	public RoundRobinRouteStage(RingBuffer inputRing, RingBuffer ... outputRings) {
		this.inputRing = inputRing;
		this.outputRings = outputRings;
		this.totalRings = outputRings.length;
		this.targetRing = totalRings;
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
		
		if (RingWalker.tryReadFragment(stage.inputRing)) {
			
			int msgId = RingWalker.messageIdx(stage.inputRing);
			
			//if the messageId is EOF then do we need to send it to both sides?
			
			
			RingBuffer outputRing = stage.outputRings[--stage.targetRing];
			if (0==stage.targetRing) {
				stage.targetRing = stage.totalRings;
			}
			
			while (!RingWalker.tryMoveSingleMessage(stage.inputRing, outputRing)) {
				Thread.yield();
			}
		}	
		return true;
	}
	
	
}
