package com.ociweb.pronghorn.stage.scheduling;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;

// The Color- Scheduling algorithm
// is based on the Color Scheduling algorithm.
// Where Color uses a central concurrent
// queue threads block on 
//
// Color has the advantage of automatic 
// thread idling when the system is idle,
// Color- does not.  
// 
// Color- has the advantage of not 
// requiring a central concurrent queue.
public class ColorMinusScheduler extends StageScheduler {

    private static final Logger log = LoggerFactory.getLogger(ColorMinusScheduler.class);

    private ExecutorService executorService; 
    private volatile boolean isShutDownNow = false;
    private volatile boolean isShuttingDown = false;

    private GraphManager graphManager;
    private PronghornStage[] stages;

    // where each thread is assigned,
    // no locking required, each thread 
    // updates their own.
    private int[] threadAssignment;

    // how many threads are currently visiting 
    // a stage. 
    private AtomicInteger[] threadCount;

    // how many stages are allowed at any stage
    // at any given time.
    private int[] maximumThreadsPerStage;
    
    // the number of stages we're working with.
    private int numberOfStages;
    private int numberOfThreads;


    // @stages the array of stages to schedule 
    // @maximumThreadsPerStage is the number of threads
    // allowed to visit a stage at any given time.
    public ColorMinusScheduler(GraphManager graphManager) {

        super(graphManager);
        this.graphManager = graphManager;

        this.executorService = Executors.newCachedThreadPool();
    }


    @Override 
    public void startup() {

        this.stages = GraphManager.getStages(graphManager);
        this.numberOfStages = stages.length;
        
        this.maximumThreadsPerStage = new int[numberOfStages];
        Arrays.fill(maximumThreadsPerStage, 1);

        int numberOfCores = Runtime.getRuntime().availableProcessors();
        this.numberOfThreads = Math.min(this.numberOfStages, numberOfCores);
        this.numberOfThreads = Math.max(this.numberOfThreads, 1);   // just in case numberOfCores is -1

        // allocate the arrays to be the appropriate size
        this.threadAssignment = new int[numberOfThreads];
        Arrays.fill(this.threadAssignment, -1);

        this.threadCount = new AtomicInteger[numberOfStages];
        for(AtomicInteger v : this.threadCount) {
            v = new AtomicInteger();
        }

        // initialization. This destroys NUMA
        // however, the multiphase startup 
        // makes creating proper threads for 
        // scheduling untenable. in the name 
        // of progress, I defer NUMA concerns
        // until they *actually* matter, and
        // the responsibilities of the scheduler
        // are better adhered to.
        // 
        // proper initialization should actually 
        // be happening outside the scheduler.
        // the scheduler's responsibility is 
        // owning the processing threads 
        // and directing them to where the 
        // work is. :/
        for(PronghornStage stage : stages) {
            executorService.execute(initStage(stage));
        }
        
        // start processing threads.
        for(int threadId = 0; threadId < numberOfStages; ++threadId) {
            executorService.execute(buildRunnable(threadId));
        }
    }

    public void shutdown(){
        try {
         GraphManager.terminateInputStages(graphManager);
        } catch (Throwable t) {
            log.error("Stacktrace",t);
        }
    }

	@Override
	public void awaitTermination(long timeout, TimeUnit unit, Runnable clean, Runnable dirty) {
		if (awaitTermination(timeout, unit)) {
			clean.run();
		} else {
			dirty.run();
		}
	}
	
    /**
     * Normal shutdown request, blocks until all the stages have finished by seeing the poison pill.
     * 
     * @param timeout
     * @param unit
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
    public boolean terminateNow() {

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
    
    // This responsibility does NOT belong on a 
    // scheduler. 
    private Runnable initStage(final PronghornStage stage) {

        return new Runnable() {

            @Override 
            public void run() {
                stage.startup();
            }
        };
    }



    // This is where the magic happens... 
    // This is the main loop of the 
    // scheduling logic.
    //
    // @return the next stage to visit
    public final PronghornStage nextStage(int threadId) {

        removeCurrentAssignment(threadId);
        int nextStageId = -1;

        do {
            nextStageId = dequeNextStage();
            tryAssign(threadId, nextStageId);
        } while(keepPolling(threadId));

        return this.stages[nextStageId];
    } 

    // @return -1 or # of stages if no stage is available, 
    // otherwise returns the offset into the stage array
    // for the stage assigned.
    private final int dequeNextStage() {
        
        int nextStageId = -1;
        long highWater = 0; 

        for(int spot = 0; spot < this.numberOfStages; ++spot) {

            AtomicInteger currentVisitors = this.threadCount[spot];

            int maxThreads = this.maximumThreadsPerStage[spot];
            int numThreadsVisiting = currentVisitors.get();

            long workAtSpot = amountOfWork(spot);

            if((numThreadsVisiting < maxThreads) &&
                (workAtSpot > highWater)) {
                    nextStageId = spot;
                    highWater = workAtSpot;
            }
        }

        return nextStageId;
    }

    // remove the current thread from the assignment 
    // matrix.
    private final void removeCurrentAssignment(int threadId) {

        int assignment = this.threadAssignment[threadId];

        // if we are assigned somewhere...
        if(assignment >= 0) {

            AtomicInteger threadCount = this.threadCount[assignment];

            boolean updated = false;
            while(!updated ) {    
                int currentCount = threadCount.get();
                updated = threadCount.compareAndSet(currentCount, currentCount - 1);
            }

            // mark thread as unassigned.
            this.threadAssignment[threadId] = -1;            
        }
    }

    // try to assign the threadId to stageId,
    // fail if the stage is oversubscribed. 
    // 
    private final void tryAssign(int threadId, int stageId) {

        if((stageId >= 0) && (stageId < this.stages.length)) {
        
            AtomicInteger currentVisitors = this.threadCount[stageId];

            int maxThreads = this.maximumThreadsPerStage[stageId];
            int numThreadsVisiting = currentVisitors.get();

            if(numThreadsVisiting < maxThreads) {

                // if there is room, we visit, otherwise drop out
                // and look for a different stage to visit. 
                if(currentVisitors.compareAndSet(numThreadsVisiting, numThreadsVisiting + 1)) {
                    this.threadAssignment[threadId] = stageId;    
                }
            }
        }        
    }

    // @return true if we should continue polling, false 
    // if a thread was successfully assigned to a stage.
    private final boolean keepPolling(int threadId) {
        return this.threadAssignment[threadId] == -1;
    }

    // Technically, Color doesn't care about the amount
    // of work, just that there is work at a stage. 
    // 
    // However, Color- uses the amount of work 
    // to prioritize visits. Note, the amount 
    // of work is similar but different than MG1's
    //  use of theoretic load for scheduling. 
    // 
    // @Coveat: this function has a side effect, 
    // it updates the previousWorkCount.
    private final long amountOfWork(int stageId) {

        int[] rings = GraphManager.getInputRingIdsForStage(graphManager, stageId);

        int index = rings.length;
        int totalWork = 0; 

        while((index > 0) && (rings[index] >= 0)) {
            Pipe ring = GraphManager.getPipe(graphManager, rings[index]);
            totalWork += Pipe.contentRemaining(ring);

            --index;
        }

        return totalWork;
    }

    // create the processing threads.
    protected Runnable buildRunnable(final int threadId) {

        return new Runnable() {

            private final int id = threadId;

            @Override
            public String toString() {
                return Integer.valueOf(threadId).toString();
            }
            
            @Override
            public void run() {

                while(!done()) {

                    try {
                        PronghornStage stage = nextStage(id);

                        while(!done()) {

                            // Our PronghornStage.run() 
                            // assumes exhaustive polling. 
                            // this is also myopic. :/
                            // 
                            // the gating policy really 
                            // should be separated out from
                            // Pronghorn.run(), that's 
                            // the concern of the scheduler
                            // not the stage. the stage
                            // really should poll its queue
                            // for a single message at a time
                            // then interact with the scheduler
                            // to determine what to do.
                            // 
                            // so we run it, then request 
                            // the nextStage().
                        	setCallerId(stage.boxedStageId);
                            stage.run();
                            clearCallerId();
                            stage = nextStage(id);
                        }

                    } catch (Throwable t) {
                        log.error("Unexpected error in thread {}", id);
                        log.error("Stacktrace",t);
                    }
                }
            }

            private boolean done() {
                return isShuttingDown || isShutDownNow;
            }   
        };
    }

}