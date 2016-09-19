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

    private AtomicBoolean shutdownRequested;
    private ReentrantLock runTerminationLock;
    private long[] rates;
    private long[] lastRun;
    private long maxRate;
    private final PronghornStage[] stages;
    
	private volatile Throwable firstException;//will remain null if nothing is wrong
    
    //Time based events will poll at least this many times over the period.
    // + ensures that the time trigger happens "near" the edge
    // + ensures that this non-thread scheduler in unit tests can capture the time delayed events.
    public static final int granularityMultiplier = 4;
    private static final long MS_TO_NS = 1_000_000;
    
    
    public NonThreadScheduler(GraphManager graphManager) {
        super(graphManager);
        
        this.stages = GraphManager.allStages(graphManager);
        
    }
    
    public NonThreadScheduler(GraphManager graphManager, PronghornStage[] stages) {
        super(graphManager);
        this.stages = stages;       
    }    
    
    @Override
    public void startup() {
        shutdownRequested = new AtomicBoolean(false);
        runTerminationLock = new ReentrantLock();
        
        final int stageCount = stages.length;
        
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
        int idx = stageCount;
        while (--idx >= 0) {               
             final PronghornStage stage = stages[idx];
             executorService.submit(new Runnable() {
                 public void run() {
                     //this may block as it must wait for the output pipes to be init by a different stage down stream
                     GraphManager.initInputRings(graphManager, stage.stageId);   //any output pipe that does not have a consuming stage is a configuration error.   
                     //everything down stream has been init so we are free to call the client work in startup();
                 
                     try {
                    	 stage.startup();
                    	 //client work is complete so move stage of stage to started.
                    	 GraphManager.setStateToStarted(graphManager, stage.stageId);
	                 } catch (Throwable t) {				    
	 	                recordTheException(stage, t);
	 	                try {
	 	                	if (null!=stage) {
	 	                		stage.shutdown();	
	 	                	}
	 	                } catch(Throwable tx) {
	 	                	recordTheException(stage, tx);
	 	                } finally {
	 	                	if (null!=stage) {
	 	                		GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
	 	                	}
	 	                }
	 				 }                 
                 };
             });
             //determine the scheduling rules
             
             if (null == GraphManager.getNota(graphManager, stage, GraphManager.UNSCHEDULED, null)) {                   

            	 Object value = GraphManager.getNota(graphManager, stage, GraphManager.SCHEDULE_RATE, Long.valueOf(0));       
                 long rate = value instanceof Number ? ((Number)value).longValue() : null==value ? 0 : Long.parseLong(value.toString());
                 
                 //System.out.println("NTS schedule rate for "+stage+" is "+value);
                 
                 
                 if (0==rate) {
                     //DEFAULT, RUN IN TIGHT LOOP
                     rates[idx] = 0;
                     lastRun[idx] = 0;
                 } else {
                     //SCHEDULE_rate, RUN EVERY rate ns
                     rates[idx] = rate;
                     if (rate>maxRate) {
                         maxRate = rate;
                     }
                     lastRun[idx] = 0;                     
                 }
             } else {
                 //UNSCHEDULED, NEVER RUN
                 rates[idx] = -1;
                 lastRun[idx] = 0;
             }

             
        }
        executorService.shutdown();

        try {
            executorService.awaitTermination(stageCount, TimeUnit.SECONDS); //provides 1 second per stage to get started up
        } catch (InterruptedException e) {
           throw new RuntimeException(e);
        }
 
        
    }

    private long nextRun = 0; //keeps times of the last pass so we need not check again
        
    @Override
    public void run() {   
    	
    	if (nextRun>0) {
    		//we have called run but we know that it can do anything until this time so we must wait
    		long nanoDelay = nextRun-System.nanoTime();
    	
    		try {
				Thread.sleep(nanoDelay/1_000_000,(int) (nanoDelay%1_000_000));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
		    }
    	}
    	
    	if (!runTerminationLock.tryLock()) {
    		return;
    	}
    	
    	if (isShutdownRequested(this)) {
    		return;
    	}

       try {
    	        long minDelay = Long.MAX_VALUE;
    	        int s = stages.length;
                while (--s>=0 && !shutdownRequested.get()) {
                    
                    long rate = rates[s];
                    
                    PronghornStage stage = stages[s];
                    
                    //TODO: time each run and keep a total of which stages take the most time to return, this is a nice debug feature.

                    if (rate>=0) {
                    	
                    	if (GraphManager.isRateLimited(graphManager,  stage.stageId)) {                    	
                    		long nsDelay =  GraphManager.delayRequiredNS(graphManager,stage.stageId);
                    		if (nsDelay>0) {   
                    			minDelay = Math.min(minDelay, nsDelay);
                    			continue;//must try again later
                    		}
                    	}
                    	
                    	if (rate>0) {
                    		//check time and only run if valid
                    		long now = System.nanoTime();
                    		                    		
                    		long nsDelay = (lastRun[s]+rate) - now;
                    		if (nsDelay<=0) {
                    			run(stage);
                    			lastRun[s] = System.nanoTime();
                    		} else {                    			
                    			minDelay = Math.min(minDelay, nsDelay);
                    		}
                    	} else {
                    		assert(0==rate);
                    		minDelay = 0;
                    		run(stage);                            
                    	}
                    } else {
                    	//never run -1
                    }    
                }
                //if we are spinning we need to sleep if non of the runs will need to be called for a while.
                if (minDelay>300) { //300 ns
                	nextRun = System.nanoTime()+minDelay;                	
                } else {
                	nextRun = 0;
                }
                
        } finally {
            runTerminationLock.unlock();
        }
        
    }

	private void run(PronghornStage stage) {
		try {
			stage.run();
		} catch (Throwable t) {				    
            recordTheException(stage, t);
		} 
		
	}

    @Override
    public void shutdown() {
    	shutdownRequested.set(true);
    	GraphManager.terminateInputStages(graphManager);		
    }

    public static boolean isShutdownRequested(NonThreadScheduler nts) {
    	return nts.shutdownRequested.get();
    }
    
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) { 
        try {
        	if (!isShutdownRequested(this)) {
        		throw new UnsupportedOperationException("Shutdown() must be called before awaitTerminiation()");
        	}
            //wait for run to stop...
            if (runTerminationLock.tryLock(timeout, unit)) {
                
                int s = stages.length;
                while (--s>=0) {
                    PronghornStage stage = stages[s];
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

    private void recordTheException(final PronghornStage stage, Throwable t) {
        synchronized(this) {
            if (null==firstException) {                     
                firstException = t;
            }
        }            
        
        GraphManager.reportError(graphManager, stage, t, log);
    }
}
