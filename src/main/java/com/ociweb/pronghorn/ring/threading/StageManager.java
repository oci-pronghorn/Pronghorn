package com.ociweb.pronghorn.ring.threading;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

public abstract class StageManager {

	private static final Logger log = LoggerFactory.getLogger(StageManager.class);
	protected GraphManager graphManager;
	
	//these fields are not used at production run time but are used by the asserts to validate:
	//   1. No stage is ever running on two threads at the same time.
	//   2. Clean shutdown and reporting of those stages that have hung
	private Object assertLock = new Object();
	private long[] runCounters = new long[0]; //only grows when asserts are on
	
	public StageManager(GraphManager graphManager) {
		this.graphManager = graphManager;
	}
	
	//called by assert
	protected boolean confirmRunStart(PronghornStage stage) {	
		Thread.currentThread().setName("Stage:"+stage.toString());
		synchronized (assertLock) {
			runCounters = incValue(runCounters, stage.stageId);
		}
		//confirm that count is odd, because we now added added 1 to start the stage
		if (0 == (runCounters[stage.stageId]&1)) {
			log.error("Expected stage {} to be starting but it appears to already be running.", stage);
			return false;
		}
		return true;
	}
	
	//called by assert
	protected boolean confirmRunStop(PronghornStage stage) {		
		synchronized (assertLock) {
			runCounters = incValue(runCounters, stage.stageId);
		}
		//confirm that count is even, because we added 1 to start the stage and now 1 to stop the stage
		if (0 != (runCounters[stage.stageId]&1)) {
			log.error("Expected stage {} to be stopping but it appears to be running.", stage);
			return false;
		}			
		return true;
	}
	
	private static long[] incValue(long[] target, int idx) {		
		long[] result = idx<target.length ? target :  Arrays.copyOf(target, (1+idx)*2); //double the array
		//very large count that is not expected to roll-over
		result[idx]++;
		return result;
	}
	
	//called by assert
	protected boolean validShutdownState() {
		synchronized (assertLock) {
			int i = runCounters.length;
			boolean result = true;
			while (--i>=0) {
				//confirm that all the counts are even, because we added 1 to start the stage and now 1 to stop the stage
				if (0!=(runCounters[i]&1)) {
					log.error("Expected stage {} to be stopped but it appears to be running.", GraphManager.getStage(graphManager,i));
					result = false;
				}				
			}
			return result;
		}
		
	}

	
	
	public abstract void startup();
	public abstract void shutdown();
	public abstract boolean awaitTermination(long timeout, TimeUnit unit);
	public abstract boolean TerminateNow();
	
	
	
}
