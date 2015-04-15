package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;

public class ThreadPerStageScheduler extends StageScheduler {
	private static final Logger log = LoggerFactory.getLogger(ThreadPerStageScheduler.class);
	
	private ExecutorService executorService; 
	private volatile boolean isShuttingDown = false;
	
	
	public ThreadPerStageScheduler(GraphManager graphManager) {
		super(graphManager);		
	}
	
	public void startup() {
				
		int i = PronghornStage.totalStages();
		this.executorService = Executors.newFixedThreadPool(i);
		while (--i>=0) {
			PronghornStage stage = GraphManager.getStage(graphManager, i);
			if (null != stage) {
				int rate = (Integer)GraphManager.getAnnotation(graphManager, stage, GraphManager.SCHEDULE_RATE, Integer.valueOf(0));
				
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
		isShuttingDown = true;
				
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
			validShutdownState();		
			return cleanExit;
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
			return true;			
		} catch (Throwable e) {
			log.error("awaitTermination", e);
			return false;
		}
	}
	
	
	/**
	 * Do not call this method except when the system has become hung.
	 * Work in the flow may be lost as a result.
	 */
	public boolean TerminateNow() {
				
		shutdown();
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
			
			@Override
			public void run() {
				try {
					
					//TODO: need to record state so we know the failure point
					log.trace("block on initRings:"+stage.getClass().getSimpleName());					
					GraphManager.initInputRings(graphManager, stage.stageId);					
					log.trace("finished on initRings:"+stage.getClass().getSimpleName());
					
					stage.startup();
					
					runLoop(stage);	
			
					stage.shutdown();	
					GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
								
				} catch (Throwable t) {
				    log.error("Stacktrace",t);
					log.warn("Unexpected error in stage "+stage.stageId+" "+stage.getClass().getSimpleName());
					GraphManager.shutdownNeighborRings(graphManager, stage);
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
					
					log.trace("block on initRings:{}",stage.getClass().getSimpleName());
					GraphManager.initInputRings(graphManager, stage.stageId);
					log.trace("finished on initRings:{}",stage.getClass().getSimpleName());
					
					stage.startup();
					
					runPeriodicLoop(nsScheduleRate, stage);	
			
					stage.shutdown();
					GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
							
				} catch (Throwable t) {
					log.error("Unexpected error in stage {}", stage);
					log.error("Stacktrace",t);
					GraphManager.shutdownNeighborRings(graphManager, stage);
					Thread.currentThread().interrupt();
				}
			}			
		};
	}

	private final void runLoop(final PronghornStage stage) {
		int i = 0;

		do {
			stage.run();			
						
			//one out of every 128 passes we will yield to play nice since we may end up with a lot of threads
			if (0==(0x7F&i++)){
				//before doing yield must push any batched up writes.
				GraphManager.publishAllWrites(graphManager, stage);
				Thread.yield();
			}
		} while ( continueRunning(this, stage));
	}

	private static boolean continueRunning(ThreadPerStageScheduler tpss, final PronghornStage stage) {
		return (!tpss.isShuttingDown && !GraphManager.isStageShuttingDown(tpss.graphManager, stage.stageId)) || GraphManager.mayHaveUpstreamData(tpss.graphManager, stage.stageId);
	}

	private void runPeriodicLoop(final int nsScheduleRate, final PronghornStage stage) {
		do {
			long start = System.nanoTime();
			stage.run();
			
			int sleepFor = nsScheduleRate - (int)(System.nanoTime()-start);
			if (sleepFor>0) {
				int sleepMs = sleepFor/1000000;
				int sleepNs = sleepFor%1000000;
				try {
					Thread.sleep(sleepMs, sleepNs);
				} catch (InterruptedException e) {
					break;
				}
			};
									
		} while (!isShuttingDown);
	}
}
