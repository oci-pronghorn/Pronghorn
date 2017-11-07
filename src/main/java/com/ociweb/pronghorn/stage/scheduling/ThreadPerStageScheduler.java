package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;

public class ThreadPerStageScheduler extends StageScheduler {
	private static final Logger logger = LoggerFactory.getLogger(ThreadPerStageScheduler.class);
	
	private ExecutorService executorService; 
	private volatile boolean isShuttingDown = false;
    private volatile Throwable firstException;//will remain null if nothing is wrong
	public boolean playNice = true;
	
	private ReentrantLock unscheduledLock = new ReentrantLock();
	private CyclicBarrier allStagesLatch;
	
	//TODO: add low priority to the periodic threads? 
	//TODO: check for Thread.yedld() want to phase that out and use parkNano
	//TODO: Generative testing, end to end match and each stage confirmed against schema, bounds, behavior, relationship
	
	public ThreadPerStageScheduler(GraphManager graphManager) {
		super(graphManager);		
	}
	
	public void startup() {
	    
	    unscheduledLock.lock();//stop any non-runnable stages from running until shutdown is started.
				
		int realStageCount = GraphManager.countStages(graphManager);
		if (realStageCount<=0) {
			System.out.println("Success!, You have a new empty project.");
			//do not startup threads since we have nothing to be done
			return;
		}		
		
		this.executorService = Executors.newFixedThreadPool(realStageCount);

		allStagesLatch = new CyclicBarrier(realStageCount+1);
		
		
		for(int i=1;i<=realStageCount;i++) {
			PronghornStage stage = GraphManager.getStage(graphManager, i);
			if (null != stage) {
			    
			    if (null == GraphManager.getNota(graphManager, stage, GraphManager.UNSCHEDULED, null)) {			        
    			    Object value = GraphManager.getNota(graphManager, stage, GraphManager.SCHEDULE_RATE, Long.valueOf(0));			    
    				long rate = value instanceof Number ? ((Number)value).longValue() : null==value ? 0 : Long.parseLong(value.toString());
    				
    				//log.info("thread per stage rates "+stage+" rate "+rate);
    				
    				if (0==rate) {
    			        if (null != GraphManager.getNota(graphManager, stage, GraphManager.MONITOR, null)) {
    			        	throw new UnsupportedOperationException("Monitors can not be run in tight loops");
    			        }
    			        if (stage instanceof MonitorConsoleStage) {
    			        	throw new UnsupportedOperationException("Monitors can not be run in tight loops");
    			        }
    					executorService.execute(buildRunnable(allStagesLatch, stage)); 	
    				} else {
    					executorService.execute(buildRunnable(allStagesLatch, rate, stage));
    				}
			    } else {
			        if (null != GraphManager.getNota(graphManager, stage, GraphManager.MONITOR, null)) {
			        	throw new UnsupportedOperationException("Monitors can not be run in tight loops");
			        }
			        if (stage instanceof MonitorConsoleStage) {
			        	throw new UnsupportedOperationException("Monitors can not be run in tight loops");
			        }
			        executorService.execute(buildNonRunnable(allStagesLatch,stage));
			        
			    }
			}
		}		
		
		//force wait for all stages to complete startup before this method returns.
		try {
		    allStagesLatch.await();
        } catch (InterruptedException e) {
        	Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
        }
		
		
	}
	
	public void shutdown(){	
		if (isShuttingDown) {
			return;
		}
		//This thread stage per schedule must ensure the 
		//shutdown logic was never called on any of the
		//active shutdown threads this shutdown is also
		//async since we are only making requests so adding
		//a thread does not modify the expected behavior
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					unscheduledLock.unlock();
				} catch (Throwable t) {
				}
				
				GraphManager.terminateInputStages(graphManager);
				isShuttingDown = true;
			}
			
		}).start();		
				
	}
	

	@Override
	public void awaitTermination(final long timeout, final TimeUnit unit, final Runnable clean, final Runnable dirty) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				if (awaitTermination(timeout, unit)) {
					clean.run();
				} else {
					dirty.run();
				}
				
			}
			
		}).start();
	}
	
	/**
	 * Normal shutdown request, blocks until all the stages have finished by seeing the poison pill.
	 * 
	 * @param timeout
	 * @param unit
	 */
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		//returns true if it was never started, eg is null
		if (null == executorService || executorService.isShutdown()){
			return true;
		}
	    try {
	        unscheduledLock.unlock();
	    } catch (Throwable t) {
	    }
		isShuttingDown = true;
		executorService.shutdown();
		
		//
		if (null!=firstException) {
		    throw new RuntimeException(firstException);
		}
		
		try {
			boolean cleanExit = executorService.awaitTermination(timeout, unit);			
			validShutdownState();			
			return cleanExit;
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
			return true;			
		} catch (Throwable e) {
		    if (null==firstException) {
                throw new RuntimeException(e);
            }
			logger.error("awaitTermination", e);
			return false;
		} finally {
		    if (null!=firstException) {
	            throw new RuntimeException(firstException);
	        }
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

	
    protected Runnable buildNonRunnable(final CyclicBarrier allstages, final PronghornStage stage) {

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
                    logger.trace("block on initRings:"+stage.getClass().getSimpleName());                  
                    GraphManager.initAllPipes(graphManager, stage.stageId);                   
                    logger.trace("finished on initRings:"+stage.getClass().getSimpleName());
                    
                    Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);                    
                    setCallerId(stage.boxedStageId);
                    stage.startup();
                    clearCallerId();
                    GraphManager.setStateToStarted(graphManager, stage.stageId);
                    
                    try {
                        allStagesLatch.await();
                    } catch (InterruptedException e) {
                    	Thread.currentThread().interrupt();
                    } catch (BrokenBarrierException e) {
                    }
                    
                    //block here until shutdown is started
                    unscheduledLock.lock();
                    unscheduledLock.unlock();
                    
                    
                } catch (Throwable t) {                 
                    recordTheException(stage, t);
                } finally {
                    //shutdown will always be called no matter how the stage was exited.
                    try {
                        if (null!=stage) {
                        	//logger.info("called shutdown on stage {} ",stage);
                        	setCallerId(stage.boxedStageId);
                        	GraphManager.shutdownStage(graphManager, stage);   
                            clearCallerId();
                        }
                    } catch(Throwable t) {
                        recordTheException(stage, t);
                    } finally {
                        if (null!=stage) {
                            GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
                        }
                    }
                }
            }

            private void recordTheException(final PronghornStage stage, Throwable t) {
                synchronized(this) {
                    if (null==firstException) {                     
                        firstException = t;
                    }
                }            
                
                GraphManager.reportError(graphManager, stage, t, logger);
            }
            
        };
    }
	
	
	protected Runnable buildRunnable(final CyclicBarrier allstages, final PronghornStage stage) {

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
					logger.trace("block on initRings:"+stage.getClass().getSimpleName());					
					GraphManager.initAllPipes(graphManager, stage.stageId);					
					logger.trace("finished on initRings:"+stage.getClass().getSimpleName());
					
					Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);
					setCallerId(stage.boxedStageId);
					stage.startup();
					clearCallerId();
					GraphManager.setStateToStarted(graphManager, stage.stageId);
					
				       try {
				            allStagesLatch.await();
				        } catch (InterruptedException e) {
				        	Thread.currentThread().interrupt();
				        } catch (BrokenBarrierException e) {
				        }
					runLoop(stage);	
			
				} catch (Throwable t) {				    
	                recordTheException(stage, t);
				} finally {
					//shutdown will always be called no matter how the stage was exited.
					try {
					    if (null!=stage) {
					    	//logger.info("called shutdown on stage {} ",stage);
					    	setCallerId(stage.boxedStageId);
					    	GraphManager.shutdownStage(graphManager, stage);	
					        clearCallerId();
					    }
					} catch(Throwable t) {
						recordTheException(stage, t);
					} finally {
					    if (null!=stage) {
					        GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
					    }
					}
				}
			}

			private void recordTheException(final PronghornStage stage, Throwable t) {
				synchronized(this) {
				    if (null==firstException) {				        
				        firstException = t;
				    }
				}   	                
                logger.error("Stacktrace",t);
                
                if (null==stage) {
                    logger.error("Stage was never initialized");
                } else {
                
    				int inputcount = GraphManager.getInputPipeCount(graphManager, stage);
    				logger.error("Unexpected error in stage "+stage.stageId+" "+stage.getClass().getSimpleName()+" inputs:"+inputcount);
    				
    				int i = inputcount;
    				while (--i>=0) {
    				    
    				    logger.error("left input pipe in state:"+ GraphManager.getInputPipe(graphManager, stage, i+1));
    				    
    				}
    				
    				GraphManager.shutdownNeighborRings(graphManager, stage);
                }
			}			
		};
	}
	
	protected Runnable buildRunnable(final CyclicBarrier allstages, final long nsScheduleRate, final PronghornStage stage) {

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
					
					logger.trace("block on initRings:{}",stage.getClass().getSimpleName());
					GraphManager.initAllPipes(graphManager, stage.stageId);
					logger.trace("finished on initRings:{}",stage.getClass().getSimpleName());
					
					Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);				
					setCallerId(stage.boxedStageId);
					stage.startup();
					clearCallerId();
					
					GraphManager.setStateToStarted(graphManager, stage.stageId);					
				       
					try {
				            allStagesLatch.await();
				        } catch (InterruptedException e) {
				        	Thread.currentThread().interrupt();
				        } catch (BrokenBarrierException e) {
				        }
					
					runPeriodicLoop(nsScheduleRate/1_000_000l, (int)(nsScheduleRate%1_000_000l), stage);
					
					//logger.info("called shutdown on stage {} ",stage);
					setCallerId(stage.boxedStageId);
					GraphManager.shutdownStage(graphManager, stage);
					clearCallerId();
					GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
							
				} catch (Throwable t) {
				    
				    synchronized(this) {
    				    if (null==firstException) {
    				        firstException = t;
    				    }
				    }				    
				    
				    GraphManager.reportError(graphManager, stage, t, logger);
				    
					GraphManager.shutdownNeighborRings(graphManager, stage);
					Thread.currentThread().interrupt();
					shutdown();
				}
			}			
		};
	}

	private final void runLoopNotNice(final PronghornStage stage) {
	    assert(!playNice);
	    assert(!GraphManager.isRateLimited(graphManager,  stage.stageId));
	    setCallerId(stage.boxedStageId);
        do {
            stage.run();
        } while (continueRunning(this, stage));
        clearCallerId();
        GraphManager.accumRunTimeAll(graphManager, stage.stageId);
	}
	
	private final void runLoop(final PronghornStage stage) {
	    if (!playNice && !GraphManager.isRateLimited(graphManager,  stage.stageId) ) {
	        runLoopNotNice(stage);
	    } else {
	    	if (!GraphManager.isRateLimited(graphManager,  stage.stageId)) {
	    		int i = 0;
	    		setCallerId(stage.boxedStageId);
				do {
				   if (playNice && 0==(0x3&i++)){
				            //one out of every 8 passes we will yield to play nice since we may end up with a lot of threads
				            //before doing yield must push any batched up writes & reads
				            Thread.yield(); 
				    }
				   
				    long start = System.nanoTime();
					stage.run();
					
					long now = System.nanoTime();
					GraphManager.accumRunTimeNS(graphManager, stage.stageId, now-start, now);
					
				} while (continueRunning(this, stage));
				clearCallerId();
	    		
	    	} else {
	    		runLoopRateLimited(stage);	
	    	}
	    }
	}

	private void runLoopRateLimited(final PronghornStage stage) {
		int i = 0;
		setCallerId(stage.boxedStageId);
		do {
		    long nsDelay =  GraphManager.delayRequiredNS(graphManager,stage.stageId);
		    if (nsDelay>0) {
		        try {
		        	long limit = System.nanoTime()+nsDelay;
		        	//some slow platforms may not wait long enough so we spin below
		        	Thread.sleep(nsDelay/1_000_000,(int)(nsDelay%1_000_000));		                                    
		        	long dif;
		        	while ((dif = (limit-System.nanoTime()))>0) {
		        		if (dif>100) {
		        			Thread.yield();
		        		}
		        	}	
		        } catch (InterruptedException e) {
		        	Thread.currentThread().interrupt();
		            break;
		        }
		        
		    } else if (playNice && 0==(0x3&i++)){
		            //one out of every 8 passes we will yield to play nice since we may end up with a lot of threads
		            //before doing yield must push any batched up writes & reads
		            Thread.yield();
		    }
		    
		    long start = System.nanoTime();
			stage.run();
			long now = System.nanoTime();
			GraphManager.accumRunTimeNS(graphManager, stage.stageId, now-start, now);
			 
			
		} while (continueRunning(this, stage));
		clearCallerId();
	}

	private static boolean continueRunning(ThreadPerStageScheduler tpss, final PronghornStage stage) {
		return ( (!GraphManager.isStageShuttingDown(tpss.graphManager, stage.stageId))) 
				&&
				(!tpss.isShuttingDown || GraphManager.mayHaveUpstreamData(tpss.graphManager, stage.stageId) );
	}

	private void runPeriodicLoop(final long msSleep, final int nsSleep, final PronghornStage stage) {
		assert(nsSleep<=1_000_000);
		int stageId = stage.stageId;
		GraphManager localGM = graphManager;
		
		int iterCount = 0;
		setCallerId(stage.boxedStageId);
		do {
			if (msSleep>0) {
	      	    try {
			        Thread.sleep(msSleep);
			    } catch (InterruptedException e) {
				    Thread.currentThread().interrupt();
				    return;
			    }
			}
			if (nsSleep>0) {
				long limit = nsSleep + System.nanoTime();
				if (nsSleep>900) {					
		      	    try {
				        Thread.sleep(0, nsSleep-300);
				    } catch (InterruptedException e) {
					    Thread.currentThread().interrupt();
					    clearCallerId();
					    return;
				    }
					
				}
				//if sleep(long, int) is implemented on this platform then  will not spin longer than 800 ns in the worst case
				//we will not watch for interupt while in his short loop.
				long dif;
				while ((dif = (limit-System.nanoTime()))>0) {
					if (dif>100) {
						Thread.yield();
					}
				}	
				if (Thread.interrupted()) {
					Thread.currentThread().interrupt();
					clearCallerId();
					return;
				}
			}
			long start = System.nanoTime();
			stage.run();
			long now = System.nanoTime();
			GraphManager.accumRunTimeNS(graphManager, stage.stageId, now-start, now);
					
			//because continueRunning can be expensive we will only check it once every 4 passes.
		} while (((++iterCount & 0x3)!=0) || continueRunning(this, stage));
		clearCallerId();
		//Still testing removal of this which seemed incorrect,  } while (!isShuttingDown && !GraphManager.isStageShuttingDown(localGM, stageId));		
	}


}
