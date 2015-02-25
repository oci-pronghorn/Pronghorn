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
	
	
	public void startup() {
		
		int i = PronghornStage.totalStages();
		while (--i>=0) {
			PronghornStage stage = GraphManager.getStage(graphManager, i);
			if (null!=stage) {
				stage.startup();
				int rate = GraphManager.getScheduleRate(graphManager, stage.stageId);
				if (0==rate) {
					executorService.execute(buildRunnable(stage));	
				} else {
					executorService.execute(buildRunnable(rate, stage));
				}
			}
		}
	}
	
	public void shutdown(){		
		GraphManager.terminateInputStages(graphManager);
	}
	
	/**
	 * Normal shutdown request, blocks until all the stages have finished by seeing the poison pill.
	 * 
	 * @param timeout
	 * @param unit
	 * @return
	 */
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		
		
		///TOOD: for simplicity this needs to terminate the inputs AND the inputs can block if needed.??
		
		isShuttingDown = true;
		executorService.shutdown();
		try {
			boolean cleanExit = executorService.awaitTermination(timeout, unit);
			assert(validShutdownState());
			return cleanExit;
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		} catch (Throwable e) {
			log.trace("awaitTermination", e);
			return false;
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
	

	
	
	
	
	protected Runnable buildRunnable(final PronghornStage stage) {

		return new Runnable() {
			//once we get a thread we never give it back
			//because this is true we can name the thread as the name of the stage

			@Override
			public String toString() {
				//must pass stage name so thread knows name
				return stage.toString();
			}
			
			///TODO: call terminate from the source stage and set isShutting down.
			///TODO: convert all stages to use the stateless shutdown
			///TDOO: may have upstream must return true for source till it calls shutdown.
			
			
			@Override
			public void run() {
				try {	
					do {
						assert(confirmRunStart(stage));
						stage.run();
						assert(confirmRunStop(stage));
						
					} while (!isShutDownNow && (!isShuttingDown || GraphManager.mayHaveUpstreamData(graphManager, stage.stageId) ));	
			
					stage.shutdown();
					
								
				} catch (Throwable t) {
					log.error("Unexpected error in stage {}", stage);
					log.error("Stacktrace",t);
					GraphManager.shutdownNeighborRings(graphManager, stage);
					assert(confirmRunStop(stage));
				}
			}			
		};
	}
	
	protected Runnable buildRunnable(final int nsScheduleRate, final PronghornStage stage) {

		return new Runnable() {
			//once we get a thread we never give it back
			//because this is true we can name the thread as the name of the stage

			@Override
			public String toString() {
				//must pass stage name so thread knows name
				return stage.toString();
			}
			
			/**
			 * Run the stage such that the leading edge of each run is nsScheduledRate apart.
			 * If the runtime of one pass is longer than the rate the runs will happen sequentially with no delay.
			 * 
			 * stops calling when terminate is started
			 */
			@Override
			public void run() {
				try {	
					do {
						long start = System.nanoTime();
						assert(confirmRunStart(stage));
						stage.run();
						
						int sleepFor = nsScheduleRate - (int)(System.nanoTime()-start);
						if (sleepFor>0) {
							int sleepMs = sleepFor/1000000;
							int sleepNs = sleepFor%1000000;
							Thread.sleep(sleepMs, sleepNs);
						};
						assert(confirmRunStop(stage));
												
					} while (!isShutDownNow && !isShuttingDown);	
					
					stage.shutdown();
							
				} catch (Throwable t) {
					log.error("Unexpected error in stage {}", stage);
					log.error("Stacktrace",t);
					GraphManager.shutdownNeighborRings(graphManager, stage);
					Thread.currentThread().interrupt();
					assert(confirmRunStop(stage));
				}
			}			
		};
	}
}
