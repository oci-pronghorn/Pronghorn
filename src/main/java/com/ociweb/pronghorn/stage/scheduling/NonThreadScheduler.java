package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.ma.RunningStdDev;

public class NonThreadScheduler extends StageScheduler implements Runnable {

    private AtomicBoolean shutdownRequested;
    private long[] rates;
    private long[] lastRun;
    
    
    private long maxRate;
    private final PronghornStage[] stages;
    private static final Logger logger = LoggerFactory.getLogger(NonThreadScheduler.class);

    private long nextRun = 0; //keeps times of the last pass so we need not check again
        
	private volatile Throwable firstException;//will remain null if nothing is wrong
    
    //Time based events will poll at least this many times over the period.
    // + ensures that the time trigger happens "near" the edge
    // + ensures that this non-thread scheduler in unit tests can capture the time delayed events.
    public static final int granularityMultiplier = 4;
    private static final long MS_TO_NS = 1_000_000;
    
    private int[] producersIdx;
    
    private Pipe[] producerInputPipes;
    private long[] producerInputPipeHeads;
    
    private Pipe[] inputPipes;
    private long[] inputPipeHeads;
    
    private Pipe[] internalPipes;
    private boolean isInternalEmpty = false; //always starts as no empty to be safe.
    private boolean someAreRateLimited = false;
    private AtomicInteger isRunning = new AtomicInteger(0);
    private final GraphManager graphManager;
    private String name = "";
    private PronghornStage lastRunStage = null;
    private final boolean inLargerScheduler;
	private static final boolean debugNonReturningStages = false;
	private final boolean recordTime;
	private DidWorkMonitor didWorkMonitor;

    public NonThreadScheduler(GraphManager graphManager) {
        super(graphManager);        
        this.stages = GraphManager.allStages(graphManager);
        this.graphManager = graphManager;
        this.inLargerScheduler = false;
    	recordTime = GraphManager.isTelemetryEnabled(graphManager);
    	
    	if (recordTime) {
    		didWorkMonitor = new DidWorkMonitor();
    	}
    }
    
    public NonThreadScheduler(GraphManager graphManager, PronghornStage[] stages, String name) {
        super(graphManager);
        this.stages = stages;       
        this.graphManager = graphManager;
        this.name = name;
        this.inLargerScheduler = false;
    	recordTime = GraphManager.isTelemetryEnabled(graphManager);
    	
    	if (recordTime) {
    		didWorkMonitor = new DidWorkMonitor();
    	}
    }    
    
    public NonThreadScheduler(GraphManager graphManager, PronghornStage[] stages, String name, boolean isInLargerScheduler) {
        super(graphManager);
        this.stages = stages;       
        this.graphManager = graphManager;
        this.name = name;
        this.inLargerScheduler = isInLargerScheduler;
    	recordTime = GraphManager.isTelemetryEnabled(graphManager);
    	
    	if (recordTime) {
    		didWorkMonitor = new DidWorkMonitor();
    	}
    }  
    
    RunningStdDev stdDevRate = null;
    
    public RunningStdDev stdDevRate() {
    	if (null==stdDevRate) {
    		stdDevRate = new RunningStdDev();
    		int i = stages.length;
	    	while (--i>=0) {	    		
	    		Number n = (Number)GraphManager.getNota(graphManager, stages[i].stageId, GraphManager.SCHEDULE_RATE, 1_200);
	    		RunningStdDev.sample(stdDevRate, n.doubleValue());
	    	}
	    	
    	}
    	return stdDevRate;
    }
    
    public boolean checkForException() {
    	if (firstException!=null) {
    		if (firstException instanceof AssertionError) {
    			throw (AssertionError)firstException;
    		} else {
    			throw new RuntimeException(firstException);
    		}
    	}
    	return true;
    }
    
    public String name() {
    	return name;
    }
    
    @Override
    public void startup() {
        shutdownRequested = new AtomicBoolean(false);
        
        final int stageCount = stages.length;
        
        
        //TODO: we need to re-order the stages to ensure we run these in order?  This will be important.

        //System.err.println("beging stage startup "+this.hashCode());
        startupAllStages(stageCount);
        //System.err.println("done stage startup "+this.hashCode());
        
        int i;
        producersIdx = buildProducersList(0, 0, graphManager, stages);        
        producerInputPipes = buildProducersPipes(0, 0, 1, producersIdx, stages, graphManager);
        producerInputPipeHeads = new long[producerInputPipes.length];
        
        inputPipes = buildInputPipes(0, 0, 1, stages, graphManager);
        inputPipeHeads = new long[inputPipes.length];

        syncInputHeadValues(producerInputPipes, producerInputPipeHeads);
        syncInputHeadValues(inputPipes, inputPipeHeads);
        
        internalPipes = buildInternalPipes(0, 0, 1, stages, graphManager);
    }

	private static void syncInputHeadValues(Pipe[] pipes, long[] heads) {
		int i = pipes.length;
        while (--i>=0) {//keep these so we know that it has changed and there is new content
        	heads[i]=Pipe.headPosition(pipes[i]);        	
        }
	}
	
	private static boolean isSyncInputHeadValues(Pipe[] pipes, long[] heads) {
		int i = pipes.length;
        while (--i>=0) {//keep these so we know that it has changed and there is new content
        	if (heads[i]!=Pipe.headPosition(pipes[i])) {
        		return false;
        	}    	
        }
        return true;
	}
    
    private static int[] buildProducersList(int count, int idx, final GraphManager graphManager, PronghornStage[] stages) {
    	
    		//skip over the non producers
    		while (idx<stages.length) {
    			
    			if (null!=GraphManager.getNota(graphManager, stages[idx].stageId, GraphManager.PRODUCER, null) || 
    				(0==GraphManager.getInputPipeCount(graphManager, stages[idx])) ) {
    				int[] result = buildProducersList(count+1, idx+1, graphManager, stages);    		
    				result[count] = idx;    		
    				return result;
    			}
    			
    			idx++;
    		}
    		
    		return new int[count];
    		
    }
    
    private static Pipe[] buildProducersPipes(int count, int indexesIdx, int outputIdx, final int[] indexes, final PronghornStage[] stages, final GraphManager graphManager) {
    	
    	while(indexesIdx<indexes.length) {
    		
    		int outputCount = GraphManager.getOutputPipeCount(graphManager, stages[indexes[indexesIdx]].stageId);
    		while(outputIdx<=outputCount) {
    		
    			Pipe pipe = GraphManager.getOutputPipe(graphManager, stages[indexes[indexesIdx]], outputIdx);  
    			
        		//is the consumer of this pipe inside the graph?
        		int consumerId = GraphManager.getRingConsumerId(graphManager, pipe.id);
        		
        		int k = stages.length;
        		while (--k>=0) {
        			if (stages[k].stageId==consumerId) {
        				
        				Pipe[] result = buildProducersPipes(count+1, indexesIdx, outputIdx+1, indexes, stages, graphManager);
        				result[count] = pipe;
        				return result;

        			}
        		}    			
    			outputIdx++;
    		}
    		outputIdx=1;
    		indexesIdx++;
    	}    	
    	return new Pipe[count]; 
    	
    }
    
    private static Pipe[] buildInputPipes(int count, int stageIdx, int inputIdx, final PronghornStage[] stages, final GraphManager graphManager) {
    	
    	while(stageIdx<stages.length) {
    		
    		int inputCount = GraphManager.getInputPipeCount(graphManager, stages[stageIdx]);
    		while(inputIdx<=inputCount) {
    		
    			Pipe pipe = GraphManager.getInputPipe(graphManager, stages[stageIdx], inputIdx);  
    			
        		int producerId = GraphManager.getRingProducerId(graphManager, pipe.id);
        		
        		boolean isFromOutside = true;
        		int k = stages.length;
        		while (--k>=0) {
        			if (stages[k].stageId==producerId) {
        				isFromOutside = false;
        				break;
        			}
        		}    			
        		if (isFromOutside) {
        			Pipe[] result = buildInputPipes(count+1, stageIdx, inputIdx+1, stages, graphManager);
        			result[count] = pipe;
        			return result;
        			
        		}
        		
    			inputIdx++;
    		}
    		inputIdx=1;
    		stageIdx++;
    	}    	
    	return new Pipe[count]; 
    	
    }
    
    private static Pipe[] buildInternalPipes(int count, int stageIdx, int inputIdx, final PronghornStage[] stages, final GraphManager graphManager) {
    	
    	while(stageIdx<stages.length) {
    		
    		int inputCount = GraphManager.getInputPipeCount(graphManager, stages[stageIdx]);
    		while(inputIdx<=inputCount) {
    		
    			Pipe pipe = GraphManager.getInputPipe(graphManager, stages[stageIdx], inputIdx);  
    			
        		int producerId = GraphManager.getRingProducerId(graphManager, pipe.id);
        		
        		int k = stages.length;
        		while (--k>=0) {
        			if (stages[k].stageId==producerId) {
        				Pipe[] result = buildInternalPipes(count+1, stageIdx, inputIdx+1, stages, graphManager);
        				result[count] = pipe;
        				return result;
        			}
        		} 
    			inputIdx++;
    		}
    		inputIdx=1;
    		stageIdx++;
    	}    	
    	return new Pipe[count]; 
    	
    }

    /**
     * Stages have unknown dependencies based on their own internal locks and the pipe usages.  As a result we do not know
     * the right order for starting them. Every stage is scheduled in a fixed thread pool to ensure every stage has the 
     * opportunity to be first, finish and wait on the others.
     * 
     * @param stageCount
     */
    private void startupAllStages(final int stageCount) {

    	int j;
    	boolean isAnyRateLimited = false;
    	//to avoid hang we must init all the inputs first
    	j = stageCount;
    	while (--j >= 0) {               
    		//this is a half init which is required when loops in the graph are discovered and we need to initialized cross dependent stages.
    		if (null!=stages[j]) {
    			GraphManager.initInputPipesAsNeeded(graphManager,stages[j].stageId);
    			isAnyRateLimited |= GraphManager.isRateLimited(graphManager,  stages[j].stageId);
    		}
    	}
    	someAreRateLimited = isAnyRateLimited;
    	
    	int unInitCount = stageCount;
 
    	while (unInitCount>0) {
    		    		
    		j = stageCount;
	        while (--j >= 0) {               
	        	final PronghornStage stage = stages[j];
	        	
	        	if (null!=stage && !GraphManager.isStageStarted(graphManager, stage.stageId)) {
	        				        		
		        		GraphManager.initAllPipes(graphManager, stage.stageId);
		        		
		        		 try {
		                	 logger.debug("begin startup of    {}",stage);
		                	 
//		                	 Thread thread = Thread.currentThread();
//		                	 new ThreadLocal<Integer>();
		                	 
		        		    	long start = 0;
		        		    	if (recordTime) {
		        		    		start = System.nanoTime();	
		        		    	}
		        		    	
		                        setCallerId(stage.boxedStageId);
		                        stage.startup();
		                        clearCallerId();
		                        
		        				if (recordTime) {
		        					final long now = System.nanoTime();		        
		        		        	long duration = now-start;
		        		 			GraphManager.accumRunTimeNS(graphManager, stage.stageId, duration, now);
		        				}
		        			 
		        			 logger.debug("finished startup of {}",stage);
		        			 
		                	 //client work is complete so move stage of stage to started.
		                	 GraphManager.setStateToStarted(graphManager, stage.stageId);
		                	 unInitCount--;
		                 } catch (Throwable t) {				    
		 	                recordTheException(stage, t, this);
		 	                try {
		 	                	if (null!=stage) {
		 	                		setCallerId(stage.boxedStageId);
		 	                		GraphManager.shutdownStage(graphManager, stage);
		 	                		clearCallerId();
		 	                	}
		 	                } catch(Throwable tx) {
		 	                	recordTheException(stage, tx, this);
		 	                } finally {
		 	                	if (null!=stage) {
		 	                		GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
		 	                	}
		 	                }
		 	                return;
		 				 }		        		
	        	}
	        }
    	}

        
        rates = new long[stageCount+1];
        lastRun = new long[stageCount+1];
       
        
        int idx = stageCount;
        while (--idx >= 0) {               
             final PronghornStage stage = stages[idx];
                     
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
        
    }

    //TODO: we want to monitor pipe content and flip execution order if they are full.

    public long nextRun() {
    	return nextRun;
    }
    
    @Override
    public void run() {   
    	
    	if (null == shutdownRequested) {
    		throw new UnsupportedOperationException("startup() must be called before run.");
    	}
        	
    	if (nextRun > 0) {
    		//we have called run but we know that it can do anything until this time so we must wait
    		if (0!=nextRun) {
	    		long nanoDelay = nextRun-System.nanoTime();
	    		
	    		if (nanoDelay>10) {

	    			//if we are in the larger scheduler do sleep now
	    			if (!inLargerScheduler && nanoDelay > 2_000_000) {
	    				logger.info("return early since delay is long and we are testing");
	    				return;//too long to wait so return
	    			}
			    	
		    		try {
						Thread.sleep(nanoDelay/1_000_000,(int) (nanoDelay%1_000_000));
						long dif;
						while ((dif = (nextRun-System.nanoTime()))>0) {
							if (dif>100) {
								Thread.yield();
							}
						}	
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						shutdown();
						return;
				    }
		    		
		    		
	    		}
    		}
    		
    		nextRun = 0;
    	}


    	if (!isRunning.compareAndSet(0, 1)){
    		return;
    	}
   	        	
	
    	//confirm that we have work to do before
   
        	
    	long nearestNextRun = Long.MAX_VALUE;
        int s = stages.length;
        boolean continueRun = false;
        while (--s>=0 && !shutdownRequested.get() && rates!=null) {

                    nearestNextRun = runStage(graphManager, 
                    		                  someAreRateLimited, 
                    		                  nearestNextRun, 
                    		                  s, 
                    		                  rates[s], 
                    		                  lastRunStage = stages[s], this); 
                    lastRunStage = null;
  
                    //if one is not shutting down then keep going
                continueRun |= !GraphManager.isStageShuttingDown(graphManager, stages[s].stageId);
                Thread.yield();
                
         }
         if (!continueRun || shutdownRequested.get()) {            	 
        	shutdown();
         }
         assert(Long.MAX_VALUE!=nearestNextRun) : "Internal error, some stage should have been scheduled ";
         
         nextRun = nearestNextRun;
         
         if (! isRunning.compareAndSet(1, 0) ) {
         }
          
                    
    }

	private static long runStage(GraphManager graphManager, boolean someAreRateLimited, long nearestNextRun, int s, long rate, PronghornStage stage, NonThreadScheduler that) {

		if (rate>=0) {			
			if (!someAreRateLimited) {
			} else {
				long nsDelay =  GraphManager.delayRequiredNS(that.graphManager,stage.stageId);
				if (nsDelay>0) {   
					nearestNextRun = Math.min(nearestNextRun, nsDelay+System.nanoTime());
					rate = -1;//must try again later
				}
			}
			
			if (rate>0) {
				nearestNextRun = runStageWithRate(graphManager, nearestNextRun, s, rate, stage, that);
			} else {
				if (0==rate) {
					
					nearestNextRun = 0;
					
			    	long start = 0;
			    	if (that.recordTime) {
			    		start = System.nanoTime();
			    		DidWorkMonitor.begin(that.didWorkMonitor,start);	
			    	}
					
					run(that.graphManager, stage, that);   
					
					if (that.recordTime && DidWorkMonitor.didWork(that.didWorkMonitor)) {
						long now = System.nanoTime();
						GraphManager.accumRunTimeNS(graphManager, stage.stageId, now-start, now);
					}
					
				}
			}
		} else {
			//never run -1
		}
		return nearestNextRun;
	}

	private static long runStageWithRate(GraphManager graphManager, long nearestNextRun, int s, long rate, PronghornStage stage, NonThreadScheduler that) {
		//check time and only run if valid
		long start = System.nanoTime();
		                    		
		long nextRun = that.lastRun[s]+rate;
		long nsDelay = nextRun - start;
		if (nsDelay<=0) {
			//logger.info("running stage {}",stage);
			run(that.graphManager, stage, that);
			that.lastRun[s] = start;
			
			long now = System.nanoTime();
			GraphManager.accumRunTimeNS(graphManager, stage.stageId, now-start, now);
						
			nearestNextRun = Math.min(nearestNextRun, start+rate);
		} else {    
			//logger.info("skipped stage {}",stage);
			nearestNextRun = Math.min(nearestNextRun, nextRun);
		}
		return nearestNextRun;
	}

	private static void run(GraphManager graphManager, PronghornStage stage, NonThreadScheduler that) {
		try {
			if (!GraphManager.isStageShuttingDown(graphManager, stage.stageId)) {
				
				
				
				if (debugNonReturningStages) {
					logger.info("begin run {}",stage);///for debug of hang
				}
				that.setCallerId(stage.boxedStageId);
				//long start = System.nanoTime();
				stage.run();
				//long duration = System.nanoTime()-start;
				//if (duration>1_000_000_000) {
				//	new Exception("too long "+stage).printStackTrace();
				//}
				
				that.clearCallerId();
				
				if (debugNonReturningStages) {
					logger.info("end run {}",stage);
				}
				
				
			} else {
				if (!GraphManager.isStageTerminated(graphManager, stage.stageId)) {
					GraphManager.shutdownStage(graphManager, stage);
                     GraphManager.setStateToShutdown(graphManager, stage.stageId);  
				}
			}
		} catch (AssertionError ae) {
			recordTheException(stage, ae, that);
			System.exit(-1); //hard stop due to assertion failure
		} catch (Throwable t) {				    
            recordTheException(stage, t, that);
            System.exit(-1); //hard stop due to suprise
        } 
		
	}

    @Override
    public void shutdown() {
    	
    	if (shutdownRequested.compareAndSet(false, true)) {
	    	
		    int s = stages.length;
	        while (--s>=0) {
	        		//ensure every non terminated stage gets shutdown called.
	        		if (null!=stages[s] && !GraphManager.isStageTerminated(graphManager, stages[s].stageId)) {        			
	        			GraphManager.shutdownStage(graphManager, stages[s]);
	        			GraphManager.setStateToShutdown(graphManager, stages[s].stageId); 
	        			//System.err.println("terminated "+stages[s]+"  "+GraphManager.isStageTerminated(graphManager, stages[s].stageId));
	        		} 
	         }
	              
	        PronghornStage temp = lastRunStage;
	        if (null!=temp) {        	
	        	logger.info("ERROR: this stage was called but never returned {}",temp);        	
	        }     
    	}
    	
    }

    public static boolean isShutdownRequested(NonThreadScheduler nts) {
    	return nts.shutdownRequested.get();
    }
    
	@Override
	public void awaitTermination(long timeout, TimeUnit unit, Runnable clean, Runnable dirty) {
		if (awaitTermination(timeout, unit)) {
			clean.run();
		} else {
			dirty.run();
		}
	}
	
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) { 

    		if (!shutdownRequested.get()) {
    			throw new UnsupportedOperationException("call shutdown before awaitTerminination");
    		}
        	long limit = System.nanoTime()+unit.toNanos(timeout);

        	if (isRunning.get()!=2) {
        		//wait until we get shutdown or timeout.
	        	while (!isRunning.compareAndSet(0, 2)) {	        		
	        		Thread.yield();
	        		if (System.nanoTime()>limit) {
	        			return false;
	        		}
	        	}
        	}
    
            int s = stages.length;
            while (--s>=0) {
                PronghornStage stage = stages[s];
                
                if (null != stage && !GraphManager.isStageTerminated(graphManager, stage.stageId)) {
                	GraphManager.shutdownStage(graphManager, stage);
                    GraphManager.setStateToShutdown(graphManager, stage.stageId);                        
                }
            }
            
            return true;

    }

    @Override
    public boolean TerminateNow() {
        shutdown();
        return true;
    }

    private static void recordTheException(final PronghornStage stage, Throwable t, NonThreadScheduler that) {
        synchronized(that) {
            if (null==that.firstException) {                     
            	that.firstException = t;
            }
        }            
        
        GraphManager.reportError(that.graphManager, stage, t, logger);
    }
}
