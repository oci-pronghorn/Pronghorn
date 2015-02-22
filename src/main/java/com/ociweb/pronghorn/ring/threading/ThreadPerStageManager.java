package com.ociweb.pronghorn.ring.threading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

public class ThreadPerStageManager extends StageManager {
	private static final Logger log = LoggerFactory.getLogger(ThreadPerStageManager.class);
	
	private ExecutorService executorService; 
	private volatile boolean isShutDownNow = false;
	private volatile boolean isShuttingDown = false;
	
	
	public ThreadPerStageManager(GraphManager graphManager) {
		super(graphManager);		
		
		this.executorService = Executors.newCachedThreadPool();
	}



	/**
	 * Normal shutdown request, blocks until all the stages have finished by seeing the poison pill.
	 * 
	 * @param timeout
	 * @param unit
	 * @return
	 */
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		isShuttingDown = true;
		executorService.shutdown();
		try {
			boolean cleanExit = executorService.awaitTermination(timeout, unit);
			assert(validShutdownState());
			return cleanExit;
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		}
		return true;
	}
	
	
	/**
	 * Do not call this method except when the system has become hung.
	 * Work in the flow may be lost as a result.
	 */
	public boolean TerminateNow() {
		isShuttingDown = true;
		isShutDownNow = true;
		try {
			//give the stages 1 full second to shut down cleanly
			return executorService.awaitTermination(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		}		
		return true;
	}	
	
	public void submit(PronghornStage stage) {		
		executorService.execute(buildRunnable(stage));		
	}
	
	public void submitAll(PronghornStage ... stages) {
		int i = stages.length;
		while (--i>=0) {
			executorService.execute(buildRunnable(stages[i]));
		}
	}

	protected Runnable buildRunnable(final PronghornStage stage) {

		return new Runnable() {
			//once we get a thread we never give it back
			//because this is true we can name the thread as the name of the stage

			@Override
			public String toString() {
				//must pass stage name so thread knows name
				return stage.toString();
			}
			
			@Override
			public void run() {
				try {	
					boolean hasMoreWork = true;
					do {
						assert(confirmRunStart(stage));
						hasMoreWork = stage.exhaustedPoll();
						assert(confirmRunStop(stage));
						Thread.yield();
					} while (!isShutDownNow && hasMoreWork);	
				//	} while (!isShutDownNow && (hasMoreWork || (stage.stateless && (!isShuttingDown || GraphManager.hasUpstreamData(graphManager, stage.stageId) )) /*|| GraphManager.hasUpstreamData(graphManager, stage.stageId)*/  ));
									
				} catch (Throwable t) {
					log.error("Unexpected error in stage {}", stage);
					log.error("Stacktrace",t);
					GraphManager.shutdownNeighbors(graphManager, stage);
				}
			}
			
		};
	}
	
	
}
