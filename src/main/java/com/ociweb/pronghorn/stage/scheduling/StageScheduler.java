package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.ThreadBasedCallerLookup;
import com.ociweb.pronghorn.stage.PronghornStage;

public abstract class StageScheduler {

	static final Logger logger = LoggerFactory.getLogger(StageScheduler.class);
	protected GraphManager graphManager;
	
	private ThreadLocal<Integer> callerId = new ThreadLocal<Integer>();
	
	public StageScheduler(GraphManager graphManager) {
		GraphManager.disableMutation(graphManager);
		this.graphManager = graphManager;		
		assert(initThreadChecking(graphManager));
	}

	private boolean initThreadChecking(final GraphManager graphManager) {
		Pipe.setThreadCallerLookup(new ThreadBasedCallerLookup(){

			@Override
			public int getCallerId() {
				Integer id = callerId.get();
				return null==id ? -1 : id.intValue();
			}

			@Override
			public int getProducerId(int pipeId) {				
				return GraphManager.getRingProducerId(graphManager, pipeId);
			}

			@Override
			public int getConsumerId(int pipeId) {
				return GraphManager.getRingConsumerId(graphManager, pipeId);
			}});
		
		return true;
	}

	protected void setCallerId(Integer caller) {
		assert(setC(caller)); //only do with assertions on
		
	}
	
	public boolean checkForException() {
		return true;//for specific implementations can throw exception if one was captured.
	}
	

	private final boolean setC(Integer caller) {
		callerId.set(caller);
		return true;
	}
	
	protected void clearCallerId() {
		assert(setC(null)); //only do with assertions on
	}
	
	protected boolean validShutdownState() {
		return GraphManager.validShutdown(graphManager);	
	}

	public abstract void startup();
	public abstract void shutdown();
	public abstract boolean awaitTermination(long timeout, TimeUnit unit);
	public abstract void awaitTermination(long timeout, TimeUnit unit, Runnable clean, Runnable dirty);
	public abstract boolean TerminateNow();

	

	private static int idealThreadCount() {
		return Runtime.getRuntime().availableProcessors();
	}
	
	public static StageScheduler defaultScheduler(GraphManager gm) {
		
		final boolean threadLimitHard = true;//must make this a hard limit or we can saturate the system easily.
		return defaultSchedulerImpl(gm, threadLimitHard, idealThreadCount());
	}
	
	public static StageScheduler defaultScheduler(GraphManager gm, int maxThreads, boolean threadLimitHard) {
		return defaultSchedulerImpl(gm, threadLimitHard, maxThreads);
	}

	private static StageScheduler defaultSchedulerImpl(GraphManager gm, final boolean threadLimitHard, final int targetThreadCountLimit) {
		assert(targetThreadCountLimit>0);
		final int countStages = GraphManager.countStages(gm);

		//disabled until we find a large machine for testing
		if (targetThreadCountLimit>countStages+
				                   (GraphManager.isTelemetryEnabled(gm)?20:0)) { 
				  //NOTE: this case will be rarely used, the other schedules are
			      //      more efficient however this scheduler is much simpler.
				  logger.info("Threads in use {}, one per stage.", countStages);
		          return new ThreadPerStageScheduler(gm);
		} else {
				  logger.info("Threads in use {}, fixed limit with fixed script.", targetThreadCountLimit);
				  return new ScriptedFixedThreadsScheduler(gm, targetThreadCountLimit, threadLimitHard);
		}
	}

	public static StageScheduler threadPerStage(GraphManager gm) {
		return new ThreadPerStageScheduler(gm);
	}
	
	public static StageScheduler fixedThreads(GraphManager gm, int threadCountLimit, boolean isHardLimit) {
		return new FixedThreadsScheduler(gm, threadCountLimit, isHardLimit);
	}
	
}
