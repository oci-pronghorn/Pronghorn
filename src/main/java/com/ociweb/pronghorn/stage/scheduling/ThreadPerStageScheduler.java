package com.ociweb.pronghorn.stage.scheduling;

import java.util.concurrent.BrokenBarrierException;
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

public class ThreadPerStageScheduler extends StageScheduler {
	private static final Logger log = LoggerFactory.getLogger(ThreadPerStageScheduler.class);
	
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
				
		int i = GraphManager.countStages(graphManager);
		
		this.executorService = Executors.newFixedThreadPool(i);
		
		int realStageCount = i;

		allStagesLatch = new CyclicBarrier(realStageCount+1);
		
		
		while (--i>=0) {
			PronghornStage stage = GraphManager.getStage(graphManager, i);
			if (null != stage) {
			    
			    if (null == GraphManager.getNota(graphManager, stage, GraphManager.UNSCHEDULED, null)) {			        
    			    Object value = GraphManager.getNota(graphManager, stage, GraphManager.SCHEDULE_RATE, Long.valueOf(0));			    
    				long rate = value instanceof Number ? ((Number)value).longValue() : null==value ? 0 : Long.parseLong(value.toString());
    				
    				if (0==rate) {
    					executorService.execute(buildRunnable(allStagesLatch, stage)); 	
    				} else {
    					executorService.execute(buildRunnable(allStagesLatch, rate, stage));
    				}
			    } else {
			        
			        executorService.execute(buildNonRunnable(allStagesLatch,stage));
			        
			    }
			}
		}		
		
		//force wait for all stages to complete startup before this method returns.
		try {
		    allStagesLatch.await();
        } catch (InterruptedException e) {
        } catch (BrokenBarrierException e) {
        }
		
		
	}
	
	public void shutdown(){	
		try {
		    unscheduledLock.unlock();
		} catch (Throwable t) {
		}
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
			log.error("awaitTermination", e);
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
                    log.trace("block on initRings:"+stage.getClass().getSimpleName());                  
                    GraphManager.initInputRings(graphManager, stage.stageId);                   
                    log.trace("finished on initRings:"+stage.getClass().getSimpleName());
                    
                    Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);
                    stage.startup();
                    GraphManager.setStateToStarted(graphManager, stage.stageId);
                    
                    try {
                        allStagesLatch.await();
                    } catch (InterruptedException e) {
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
                            stage.shutdown();   
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
                
                GraphManager.reportError(graphManager, stage, t, log);
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
					log.trace("block on initRings:"+stage.getClass().getSimpleName());					
					GraphManager.initInputRings(graphManager, stage.stageId);					
					log.trace("finished on initRings:"+stage.getClass().getSimpleName());
					
					Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);
					stage.startup();
					GraphManager.setStateToStarted(graphManager, stage.stageId);
					
				       try {
				            allStagesLatch.await();
				        } catch (InterruptedException e) {
				        } catch (BrokenBarrierException e) {
				        }
					runLoop(stage);	
			
				} catch (Throwable t) {				    
	                recordTheException(stage, t);
				} finally {
					//shutdown will always be called no matter how the stage was exited.
					try {
					    if (null!=stage) {
					        stage.shutdown();	
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
                log.error("Stacktrace",t);
                
                if (null==stage) {
                    log.error("Stage was never initialized");
                } else {
                
    				int inputcount = GraphManager.getInputPipeCount(graphManager, stage);
    				log.error("Unexpected error in stage "+stage.stageId+" "+stage.getClass().getSimpleName()+" inputs:"+inputcount);
    				
    				int i = inputcount;
    				while (--i>=0) {
    				    
    				    log.error("left input pipe in state:"+ GraphManager.getInputPipe(graphManager, stage, i+1));
    				    
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
					
					log.trace("block on initRings:{}",stage.getClass().getSimpleName());
					GraphManager.initInputRings(graphManager, stage.stageId);
					log.trace("finished on initRings:{}",stage.getClass().getSimpleName());
					
					Thread.currentThread().setName(stage.getClass().getSimpleName()+" id:"+stage.stageId);
					stage.startup();
					GraphManager.setStateToStarted(graphManager, stage.stageId);					
				       
					try {
				            allStagesLatch.await();
				        } catch (InterruptedException e) {
				        } catch (BrokenBarrierException e) {
				        }
					
					runPeriodicLoop(nsScheduleRate/1_000_000l, (int)(nsScheduleRate%1_000_000l), stage);	
			
					stage.shutdown();
					GraphManager.setStateToShutdown(graphManager, stage.stageId); //Must ensure marked as terminated
							
				} catch (Throwable t) {
				    
				    synchronized(this) {
    				    if (null==firstException) {
    				        firstException = t;
    				    }
				    }				    
				    
				    GraphManager.reportError(graphManager, stage, t, log);
				    
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
        do {
            stage.run();
        } while (continueRunning(this, stage));
	    
	}
	
	private final void runLoop(final PronghornStage stage) {
	    if (!playNice && !GraphManager.isRateLimited(graphManager,  stage.stageId) ) {
	        runLoopNotNice(stage);
	    } else {
    		int i = 0;
    		do {
    		    long nsDelay =  GraphManager.delayRequiredNS(graphManager,stage.stageId);
    		   
    		    if (nsDelay>0) {
                    try {
                        GraphManager.publishAllWrites(graphManager, stage);
                        
                        Thread.sleep(nsDelay/1_000_000,(int)(nsDelay%1_000_000));
                                                
                    } catch (InterruptedException e) {
                        break;
                    }
    		    } else if (playNice && 0==(0x3&i++)){
    		            //one out of every 8 passes we will yield to play nice since we may end up with a lot of threads
    		            //before doing yield must push any batched up writes & reads
    		            GraphManager.publishAllWrites(graphManager, stage);
    		            Thread.yield(); //TODO: is there a race condition causing JUnit failures that happens when this is used?
    		            //nsSleep(1);
    		            //LockSupport.parkNanos(1); //may be longer than 1ns
    		            //GraphManager.releaseAllReads(graphManager, stage);
    		    }
    			stage.run();			
    			
    			
    		} while (continueRunning(this, stage));	
	    }
	}

	private static boolean continueRunning(ThreadPerStageScheduler tpss, final PronghornStage stage) {
		return (!tpss.isShuttingDown && !GraphManager.isStageShuttingDown(tpss.graphManager, stage.stageId)) 
				|| 
				GraphManager.mayHaveUpstreamData(tpss.graphManager, stage.stageId);
	}

	private void runPeriodicLoop(final long msSleep, final int nsSleep, final PronghornStage stage) {
		
	    if (0==msSleep) {
	           do {
	                //NOTE: This implementation is depended upon to run no faster than the requested rate. (eg i2c stage and others)
	                //      Regardless of how long or short is spend inside run the same delay between calls is always enforced.

	                nsSleep(nsSleep);
	                	                
	                stage.run();
	                
	            } while (!isShuttingDown && !GraphManager.isStageShuttingDown(graphManager, stage.stageId));
	        
	    } else {
	    
	    
    	    do {
    		    //NOTE: This implementation is depended upon to run no faster than the requested rate. (eg i2c stage and others)
    		    //      Regardless of how long or short is spend inside run the same delay between calls is always enforced.
	    
        	    try {
        		        Thread.sleep(msSleep, nsSleep);
        	    } catch (InterruptedException e) {
        		       Thread.currentThread().interrupt();
        		       return;
        	    }

    		    
    		    stage.run();
    			
    		} while (!isShuttingDown && !GraphManager.isStageShuttingDown(graphManager, stage.stageId));
	    }
		
		
	}

    private void nsSleep(final int nsSleep) {
        long next = System.nanoTime()+nsSleep;
        long hardStop = System.currentTimeMillis()+2;
        while (System.nanoTime()<next) {
           Thread.yield();
           if (System.currentTimeMillis()>=hardStop) {
               break;
           }
        }
    }
}
