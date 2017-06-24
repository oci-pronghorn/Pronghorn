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

	private static final boolean debugNonReturningStages = false;
	

    public NonThreadScheduler(GraphManager graphManager) {
        super(graphManager);        
        this.stages = GraphManager.allStages(graphManager);
        this.graphManager = graphManager;
    }
    
    public NonThreadScheduler(GraphManager graphManager, PronghornStage[] stages, String name) {
        super(graphManager);
        this.stages = stages;       
        this.graphManager = graphManager;
        this.name = name;
    }    
    
    public void checkForException() {
    	if (firstException!=null) {
    		throw new RuntimeException(firstException);
    	}
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
		                	 
		                	 Thread thread = Thread.currentThread();
		                	 new ThreadLocal<Integer>();
		                	 
		                	 setCallerId(stage.boxedStageId);
		        			 stage.startup();
		        			 clearCallerId();
		        			 
		        			 logger.debug("finished startup of {}",stage);
		        			 
		                	 //client work is complete so move stage of stage to started.
		                	 GraphManager.setStateToStarted(graphManager, stage.stageId);
		                	 unInitCount--;
		                 } catch (Throwable t) {				    
		 	                recordTheException(stage, t, this);
		 	                try {
		 	                	if (null!=stage) {
		 	                		setCallerId(stage.boxedStageId);
		 	                		stage.shutdown();
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

    @Override
    public void run() {   
    	
    	//TODO: we want to monitor pipe content and flip execution order if they are full.
    	
    	
    	if (nextRun>0) {
    		
//    		//TODO: this new feature is causing a hang in some cases, Must debug.
//    		if (/*false &&*/ !isInternalEmpty) {
//    			
//    			//before we set or scan for empty record the head positions, if they still match later then we will know its empty.
//    	        syncInputHeadValues(producerInputPipes, producerInputPipeHeads);
//    	        syncInputHeadValues(inputPipes, inputPipeHeads);
//    			
//    			// check that all pipes have no content, we have time if not set here it is never set true.
//    			boolean tmp = true;
//    			int i = internalPipes.length;
//    			while (--i>=0) {
//    				if (Pipe.contentRemaining(internalPipes[i])>0) {  
//    					tmp = false;
//    					break;//exit quick we found something.
//    				}
//    			}
//    			//must also scan input pipes
//    			i = inputPipes.length;
//    			while (--i>=0) {
//    				if (Pipe.contentRemaining(inputPipes[i])>0) {  
//    					tmp = false;
//    					break;//exit quick we found something.
//    				}
//    			}
//    			isInternalEmpty = tmp;   
//    			    			
//    		}
    		
    		//we have called run but we know that it can do anything until this time so we must wait
    		if (0!=nextRun) {
	    		long nanoDelay = nextRun-System.nanoTime();
	    		if (nanoDelay>0) {

	    			if (nanoDelay > 1_000_000) {
	    				return;//too long to wait so return
	    			}
			    	//System.out.println("delay "+nanoDelay);
		    		try {
						Thread.sleep(nanoDelay/1_000_000,(int) (nanoDelay%1_000_000));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
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
        	
        	
//        	if (isInternalEmpty && inputPipes.length>0 ) { // if all pipes were emptied on previous run.
//        	
//        		long spinDelay = 1000;		
//        		//sleep thread until something appears on one of these inputs
//    	        while (isSyncInputHeadValues(producerInputPipes, producerInputPipeHeads) && isSyncInputHeadValues(inputPipes, inputPipeHeads) && !isShutdownRequested(this)) {
//        	        	
//    	        	try {
//						Thread.sleep(spinDelay/1000000,(int)spinDelay%1000000); //TODO: review all the stages to pick an appropriate large value.
//					} catch (InterruptedException e) {
//						Thread.currentThread().interrupt();
//						break;
//					}
//    	        	
//    	        	int p = producersIdx.length;
//    	        	while (--p>=0) {
//    	        		int idx = producersIdx[p];
//    	        		
//    	        		spinDelay = runStage(graphManager, someAreRateLimited, spinDelay, idx, rates[idx], lastRunStage = stages[idx], this);
//    	        		lastRunStage = null;
//    	        	}
//    	        }
//    	        isInternalEmpty = false;
//        	}
        	
        	
        	long nearestNextRun = Long.MAX_VALUE;
   	        int s = stages.length;
   	        boolean continueRun = false;
            while (--s>=0 && !shutdownRequested.get() && rates!=null) {
            	
                    nearestNextRun = runStage(graphManager, someAreRateLimited, nearestNextRun, s, rates[s], lastRunStage = stages[s], this); 
                    lastRunStage = null;
                    
                    //if one is not shutting down then keep going
                    continueRun |= !GraphManager.isStageShuttingDown(graphManager, stages[s].stageId);
                    Thread.yield();
             }
             if (!continueRun || shutdownRequested.get()) {
            	 
            	shutdown();
             }
             nextRun = Long.MAX_VALUE==nearestNextRun ? 0 : nearestNextRun;
                
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
					long now = System.nanoTime();
					
					run(that.graphManager, stage, that);   
					
					long duration = System.nanoTime()-now;
					
					if (duration>0) {
						GraphManager.accumRunTimeNS(graphManager, stage.stageId, duration);
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
		long now = System.nanoTime();
		                    		
		long nsDelay = (that.lastRun[s]+rate) - now;
		if (nsDelay<=0) {
			run(that.graphManager, stage, that);
			that.lastRun[s] = now;
			
			long duration = System.nanoTime()-now;

			if (duration>0) {
				GraphManager.accumRunTimeNS(graphManager, stage.stageId, duration);
			}
			
		} else {                    			
			nearestNextRun = Math.min(nearestNextRun, nsDelay+now);
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
				stage.run();
				that.clearCallerId();
				
				if (debugNonReturningStages) {
					logger.info("end run {}",stage);
				}
				
				
			} else {
				if (!GraphManager.isStageTerminated(graphManager, stage.stageId)) {
					 stage.shutdown();
                     GraphManager.setStateToShutdown(graphManager, stage.stageId);  
				}
			}
		} catch (AssertionError ae) {
			recordTheException(stage, ae, that);
			System.exit(-1); //hard stop due to assertion failure
		} catch (Throwable t) {				    
            recordTheException(stage, t, that);
		} 
		
	}

    @Override
    public void shutdown() {
    	
    	if (shutdownRequested.compareAndSet(false, true)) {
	    	
		    int s = stages.length;
	        while (--s>=0) {
	        		//ensure every non terminated stage gets shutdown called.
	        		if (null!=stages[s] && !GraphManager.isStageTerminated(graphManager, stages[s].stageId)) {        			
	        			stages[s].shutdown();
	        			GraphManager.setStateToShutdown(graphManager, stages[s].stageId); 
	        			//System.err.println("terminated "+stages[s]+"  "+GraphManager.isStageTerminated(graphManager, stages[s].stageId));
	        		} 
	         }
	              
	        if (null!=lastRunStage) {        	
	        	logger.info("ERROR: this stage was called but never returned {}",lastRunStage.getClass().getSimpleName());        	
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

        	long limit = System.nanoTime()+unit.toNanos(timeout);
        	
        	while (!isRunning.compareAndSet(0, 2)) {
        		
        		Thread.yield();
        		if (System.nanoTime()>limit) {
        			return false;
        		}
        	}

            int s = stages.length;
            while (--s>=0) {
                PronghornStage stage = stages[s];
                
                if (null != stage && !GraphManager.isStageTerminated(graphManager, stage.stageId)) {
                    stage.shutdown();
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
        
        GraphManager.reportError(that.graphManager, stage, t, log);
    }
}
