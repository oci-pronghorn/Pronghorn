package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.Blocker;

public class NonThreadScheduler extends StageScheduler implements Runnable {

    private final GraphManager graphManager;
    private AtomicBoolean shutdownRequested;
    private ReentrantLock runLock;
    private long[] rates;
    private long[] lastRun;
    private long maxRate;
    
    private boolean isSingleStepMode = false;

    private long minimumDuration = 0;
    
    
    //Time based events will poll at least this many times over the period.
    // + ensures that the time trigger happens "near" the edge
    // + ensures that this non-thread scheduler in unit tests can capture the time delayed events.
    public static final int granularityMultiplier = 4;
    private static final long MS_TO_NS = 1_000_000;
    
    
    public NonThreadScheduler(GraphManager graphManager) {
        super(graphManager);
        this.graphManager = graphManager;
    }
    
    public void setSingleStepMode(boolean isSingleStepMode) {
        this.isSingleStepMode = isSingleStepMode;    
    }
    
    @Override
    public void startup() {
        shutdownRequested = new AtomicBoolean(false);
        runLock = new ReentrantLock();
        
        final int stageCount = GraphManager.countStages(graphManager);
        
        rates = new long[stageCount+1];
        lastRun = new long[stageCount+1];
        
        startupAllStages(stageCount);
        System.gc();
        
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
                     //SCHEDULE_rate, RUN EVERY rate ns
                     rates[stageId] = rate;
                     if (rate>maxRate) {
                         maxRate = rate;
                     }
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
    	
        runLock.lock();      

        long runUntil = System.nanoTime()+minimumRunDuration();

       try {

          //we always run at least once.
          do {
                
                int s = GraphManager.countStages(graphManager)+1;
                while (--s>=0) {
                    
                    long rate = rates[s];
                    
                    PronghornStage stage = GraphManager.getStage(graphManager, s);
                    
                    //TODO: time each run and keep a total of which stages take the most time to return, this is a nice debug feature.

                    if (null != stage) {
                    	if (rate>0) {
                    		//check time and only run if valid
                    		long now = System.nanoTime();
                    		if (lastRun[s]+rate <= now) {
                    			stage.run();
                    			lastRun[s] = now;
                    		}
                    	} else if (rate==0) {
                            stage.run();                            
                        } else {
                            //never run -1
                        }
                    }
 
                }
                
                //stop if shutdown is requested
                //continue until all the pipes are empty and not in singleStepMode
                //continue until enough time as passed for the largest rate to be called at least once
            } while (( System.nanoTime()<runUntil) && (!shutdownRequested.get()) && (((!GraphManager.isAllPipesEmpty(graphManager)) && (!isSingleStepMode)))  );

        } finally {
            runLock.unlock();
        }
        
    }

    public void setMinimumStepDurationMS(long duration) {
        this.minimumDuration = duration*MS_TO_NS;
    }
    
    private long minimumRunDuration() {
        return Math.max(minimumDuration, maxRate);
    }

    @Override
    public void shutdown() {
        shutdownRequested.set(true);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) { 
        try {
            //wait for run to stop...
            if (runLock.tryLock(timeout, unit)) {
                
                int s = GraphManager.countStages(graphManager)+1;
                while (--s>=0) {
                    PronghornStage stage = GraphManager.getStage(graphManager, s);
                    if (null != stage) {
                        stage.shutdown();
                    }
                }
                
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
