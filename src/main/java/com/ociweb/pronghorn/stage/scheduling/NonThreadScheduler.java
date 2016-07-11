package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.Blocker;

public class NonThreadScheduler extends StageScheduler implements Runnable {

    private final GraphManager graphManager;
    private AtomicBoolean shutdownRequested;
    private ReentrantLock terminatedLock;
    private long[] rates;
    private long[] lastRun;
    
    
    public NonThreadScheduler(GraphManager graphManager) {
        super(graphManager);
        this.graphManager = graphManager;
    }
    
    @Override
    public void startup() {
        shutdownRequested = new AtomicBoolean(false);
        terminatedLock = new ReentrantLock();
        
        final int stageCount = GraphManager.countStages(graphManager);
        
        rates = new long[stageCount+1];
        lastRun = new long[stageCount+1];
        
        startupAllStages(stageCount);
        System.gc();
        
        terminatedLock.lock();
    }

    /**
     * Stages have unknown dependencies based on their own internal locks and the pipe usages.  As a result we do not know
     * the right order for starting them. Every stage is scheduled in a fixed thread pool to ensure every stage has the 
     * opportunity to be first, finish and wait on the others.
     * 
     * @param stageCount
     */
    private void startupAllStages(final int stageCount) {
        
        //NOTE: possible new design.
        //      Ask GraphManager for ordered list of stages starting with those which are outputs working back up to the inputs.
        //      TODO: this would provide deterministic startup and would not require as many threads.   
        
        
        ExecutorService executorService = Executors.newFixedThreadPool(stageCount); //must be as big as the count of stages
        int stageId = stageCount+1;
        while (--stageId >= 0) {               
             final PronghornStage stage = GraphManager.getStage(graphManager, stageId);
             executorService.submit(new Runnable() {
                 public void run() {
                     //this may block as it must wait for the output pipes to be init by a different stage down stream
                     GraphManager.initInputRings(graphManager, stage.stageId);   //any output pipe that does not have a consuming stage is a configuration error.   
                     //everything down stream has been init so we are free to call the client work in startup();
                     stage.startup();
                     //client work is complete so move stage of stage to started.
                     GraphManager.setStateToStarted(graphManager, stage.stageId);
                 };
             });
             //determine the scheduling rules
             
             if (null == GraphManager.getNota(graphManager, stage, GraphManager.UNSCHEDULED, null)) {                   
                 Object value = GraphManager.getNota(graphManager, stage, GraphManager.SCHEDULE_RATE, Long.valueOf(0));              
                 long rate = value instanceof Number ? ((Number)value).longValue() : null==value ? 0 : Long.parseLong(value.toString());
                 
                 if (0==rate) {
                     //DEFAULT, RUN IN TIGHT LOOP
                     rates[stageId] = 0;
                     lastRun[stageId] = 0;
                 } else {
                     //SCHEDULE_RAGE, RUN EVERY rate ns
                     rates[stageId] = rate;
                     lastRun[stageId] = 0;                     
                 }
             } else {
                 //UNSCHEDULED, NEVER RUN
                 rates[stageId] = -1;
                 lastRun[stageId] = 0;
             }
             
        }
        executorService.shutdown();

        try {
            executorService.awaitTermination(stageCount, TimeUnit.SECONDS); //provides 1 second per stage to get started up
        } catch (InterruptedException e) {
           throw new RuntimeException(e);
        }
    }
    
    @Override
    public void run() {
 
        while (!shutdownRequested.get() && !GraphManager.isAllPipesEmpty(graphManager)) {
            
            int s = GraphManager.countStages(graphManager)+1;
            while (--s>=0) {
                
                long rate = rates[s];
                
                if (rate==0) {
                    GraphManager.getStage(graphManager, s).run();
                    lastRun[s] = System.nanoTime();
                } else if (rate>0) {
                    //check time and only run if valid
                    long now = System.nanoTime();
                    if (lastRun[s]+rate <= now) {
                        GraphManager.getStage(graphManager, s).run();
                        lastRun[s] = System.nanoTime();
                    }
                } else {
                    //never run -1
                }
            }
            
        }
        
        if (shutdownRequested.get()) {
            
            int s = GraphManager.countStages(graphManager)+1;
            while (--s>=0) {
                GraphManager.getStage(graphManager, s).shutdown();
            }
            terminatedLock.unlock();
        }
        
    }

    @Override
    public void shutdown() {
        shutdownRequested.set(true);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        try {
            if (terminatedLock.tryLock(timeout, unit)) {
                terminatedLock.unlock();
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean TerminateNow() {
        shutdown();
        return true;
    }

}
