package com.ociweb.pronghorn.stage.scheduling;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorStage;
import com.ociweb.pronghorn.util.ma.RunningStdDev;

public class FixedThreadsScheduler extends StageScheduler {

	public class JoinFirstComparator implements Comparator<Pipe> {

		private final GraphManager graphManager;
		private final int[] weights;
		
		public JoinFirstComparator(GraphManager graphManager) {
			this.graphManager = graphManager;
			this.weights = new int[Pipe.totalPipes()];
		}

		@Override
		public int compare(Pipe p1, Pipe p2) {
			return Integer.compare(weight(p1),weight(p2));
		}
		
		public int weight(Pipe p) {
			
			if (weights[p.id]==0) {
			
				int result = (int)p.config().slabBits();
				
				//returns the max pipe length from this pipe or any of the pipes that feed its producer.
				//if this value turns out to be large then we should probably not join these two stages.
				
				int producerId = GraphManager.getRingProducerId(graphManager, p.id);		
				if (producerId>=0) {
					PronghornStage producer = GraphManager.getStage(graphManager, producerId);			
					int count = GraphManager.getInputPipeCount(graphManager, producer);
					while (--count>=0) {
						Pipe inputPipe = GraphManager.getInputPipe(graphManager, producer, count);				
						result = Math.max(result, inputPipe.config().slabBits());
					}
				} else {
					//no producer found, an external thread must be pushing data into this, there is nothing to combine it with				
				}
				weights[p.id] = result;
			} 
			return weights[p.id];
			
		}
		
	}

	private int threadCount;
	private final IntHashTable rootsTable;
	private ExecutorService executorService;
	private volatile Throwable firstException;//will remain null if nothing is wrong
	private static final Logger logger = LoggerFactory.getLogger(FixedThreadsScheduler.class);
	private NonThreadScheduler[] ntsArray;
	
	public FixedThreadsScheduler(GraphManager graphManager) {
		//this is often very optimal since we have enough granularity to swap work but we do not
		//have so many threads that it overwhelms the operating system context switching
		this(graphManager, Runtime.getRuntime().availableProcessors()*2);
	}
	
	public FixedThreadsScheduler(GraphManager graphManager, int targetThreadCount) {
		this(graphManager, targetThreadCount, true);
	}
	
	public FixedThreadsScheduler(GraphManager graphManager, int targetThreadCount, boolean enforceLimit) {
		super(graphManager);

		int logLimit=4000;//how many stages that we we can schedule without significant delay.
		
		int countStages = GraphManager.countStages(graphManager);
		if (countStages>logLimit) {
			logger.info("beginning to schedule work if {} stages into thread count of {}, this may take some time.",
					countStages,
					targetThreadCount);
		}
	    final Comparator<? super Pipe> joinFirstComparator = new JoinFirstComparator(graphManager);
	    //created sorted list of pipes by those that should have there stages combined first.
	    Pipe[] pipes = GraphManager.allPipes(graphManager);	  
	    Arrays.sort(pipes, joinFirstComparator);
        if (countStages>logLimit) {
        	logger.info("finished inital sorting");
        }
	    int totalThreads = GraphManager.countStages(graphManager);  
	  
	    //must add 1 for the tree of roots also adding 1 more to make hash more efficient.
	    int bits = 1 + (int)Math.ceil(Math.log(totalThreads)/Math.log(2));
	   
		rootsTable = new IntHashTable(bits);
	    
	    //counter for root ids, must not collide with stageIds so we start above that point.	
	    //this is where we decide which stages will be in which thread groups.
	    final int rootCounter = hierarchicalClassifier(graphManager, targetThreadCount, pipes, totalThreads, enforceLimit);    
	    if (countStages>logLimit) {
	        	logger.info("finished hierarchical classifications");
	    }
	    PronghornStage[][] stageArrays = buildOrderedArraysOfStages(graphManager, rootCounter);
	  

	    int total = 0;
	    int i = stageArrays.length;
	    while (--i>=0) {
	    	if (null!=stageArrays[i]) {
	    		int j = stageArrays[i].length;
	    		total += j;
	    	}
	    	
	    }
	    if (countStages>logLimit) {
	    	logger.info("finished scheduling");
	    }
		///////////////////    
		//  test code for a single thread	
		//	threadCount = 1;
		//  PronghornStage[][] stageArrays = new PronghornStage[][]{GraphManager.allStages(graphManager)};
	    //////////////////	    	    
	    
	    createSchedulers(graphManager, stageArrays);
	    
	    
	    //clean up now before we begin.
	    stageArrays=null;
		System.gc();
		
	}

	private int hierarchicalClassifier(GraphManager graphManager, int targetThreadCount, Pipe[] pipes, int totalThreads, boolean enforceLimit) {
				
		int rootCounter =  totalThreads+1;
		
		//////////////////////////////////////////////////
		//before we begin combine all monitior stages on a single thread to minimize disruption across the graph.
		///////////////////////////////////////////////////
		//NOTE May test removing this later
		
		PronghornStage[] monitors = GraphManager.allStagesByType(graphManager, PipeMonitorStage.class);
		int m = monitors.length;
		while (--m >= 0) {
			PipeMonitorStage rbms = (PipeMonitorStage)monitors[m];
			
			int outId = GraphManager.getOutputPipe(graphManager, rbms.stageId).id;
		//	int outId = rbms.getObservedPipeId();//this idea did not work out.
									
			int consumerId = GraphManager.getRingConsumer(graphManager, outId).stageId;
    		int consRoot = rootId(consumerId, rootsTable);
    		int prodRoot = rootId(rbms.stageId , rootsTable);
    		if (consRoot!=prodRoot) {
				rootCounter = combineToSameRoot(rootCounter, consRoot, prodRoot);							
				totalThreads--;//removed one thread		
    		}
		}
		///////////////////////////////////////////////
		//done combining the monitor stages.
		///////////////////////////////////////////////
			
		
		//loop over pipes once and stop early if total threads is smaller than or matches the target thread count goal
	    int i = pipes.length;
	    while (totalThreads>targetThreadCount && --i>=0) {	    	
	    	int ringId = pipes[i].id;
	    	
	    	int consumerId = GraphManager.getRingConsumerId(graphManager, ringId);
	    	int producerId = GraphManager.getRingProducerId(graphManager, ringId);
	    	//only join stages if they both exist
	    	if (consumerId>0 && producerId>0) {	  
	    			    		
	    		if (isValidToCombine(ringId, consumerId, producerId, graphManager)) {
	    		
	    		
		    		//determine if they are already merged in the same tree?
		    		int consRoot = rootId(consumerId, rootsTable);
		    		int prodRoot = rootId(producerId, rootsTable);
		    		if (consRoot!=prodRoot) {		    			
		    			rootCounter = combineToSameRoot(rootCounter, consRoot, prodRoot);
		    			totalThreads--;//removed one thread
		    		}
		    		//else nothing already combined
		    				    				    		
	    		}
	    	}
	    }
	    
	    //////////////////////////
	    //if needed we do another pass to combine unconnected groups
	    //we combine the 2 smallest then we repeat
	    //////////////////////	    
	    
	    if (enforceLimit) {
	   
		    while (totalThreads>targetThreadCount) {
	
		    	 int[] rootMemberCounter = new int[rootCounter+1]; 
		    	 buildCountOfStagesForEachThread(graphManager, rootCounter, rootMemberCounter);
		    	 
		    	 int smallest1count = Integer.MAX_VALUE;
		    	 int smallest1root = -1;
		    	 
		    	 int smallest2count = Integer.MAX_VALUE;
		    	 int smallest2root = -1;
		    	 
		    	 int j = rootMemberCounter.length;
		    	 while (--j>=0) {
		    		 
		    		 if (rootMemberCounter[j]>0) {
		    			 
		    			if (rootMemberCounter[j] <= smallest2count) {
		    				
		    				smallest1count = smallest2count;
		    				smallest1root  = smallest2root;
		    				
		    				smallest2count = rootMemberCounter[j];
		    				smallest2root  = j;
		    				
		    			}
		    		 }
		    	 }
		    	 
		    	 
		    	 if (smallest1count==Integer.MAX_VALUE) {
		    		 break;//nothing more could be done
		    	 } else {
		    		 rootCounter = combineToSameRoot(rootCounter, smallest1root, smallest2root);
		    		 totalThreads--;//removed one thread
		    	 }	
		    	
		    }
	    }
	    
		
	    
	    ///////////////////////////

	    threadCount = totalThreads;
	    //logger.info("Threads Requested: {} Threads Used: {}",targetThreadCount,threadCount);
		return rootCounter;
	}

	//rules because some stages should not be combined
	private boolean isValidToCombine(int ringId, int consumerId, int producerId, GraphManager graphManager) {
				
		Pipe p = GraphManager.getPipe(graphManager, ringId);
		
		if (Pipe.isForSchema(p, PipeMonitorSchema.instance)) {
			return true;//all monitors can be combined freely as needed.
		}
		
				
		//the consumer stage has 2 or more of the same pipe schema as this one it consumes so we should not share the same root
        if (countParallelConsumers(consumerId, producerId, graphManager, p)) {
        	return false;
        }
        //the producer stage has 2 or more of the same pipe schema as this one it producers so we should not share the same root.
        if (countParallelProducers(consumerId, producerId, graphManager, p)) {
        	return false;
        }
        
		//do not combine with stages which are 2 standard deviations above the mean input/output count
	    RunningStdDev pipesPerStage = GraphManager.stdDevPipesPerStage(graphManager);
	    int thresholdToNeverCombine = (int) (RunningStdDev.mean(pipesPerStage)+ (2*RunningStdDev.stdDeviation(pipesPerStage)));
	    if ((GraphManager.getInputPipeCount(graphManager,consumerId)
	         +GraphManager.getOutputPipeCount(graphManager, consumerId)) > thresholdToNeverCombine) {
	    	return false;
	    }
	    if ((GraphManager.getInputPipeCount(graphManager,producerId)
		    +GraphManager.getOutputPipeCount(graphManager, producerId)) > thresholdToNeverCombine) {
		    return false;
        }    
		
		return true;
	}

	private boolean countParallelProducers(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
		int noMatchCount = 0;
		int proOutCount = GraphManager.getOutputPipeCount(graphManager, producerId);
        while (--proOutCount>=0) {
			//all the other output coming from the producer
			Pipe outputPipe = GraphManager.getOutputPipe(graphManager, producerId, 1+proOutCount);
			
			//is this the same schema as the pipe in question.
			if (Pipe.isForSameSchema(outputPipe, p)) {
				
				//determine if they are consumed by the same place or not
				int conId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);				
				if (consumerId  != conId) {
					//only count if they are not consumed at the same place
					noMatchCount++;
				}
				//else if they are consumed by the same place then the pipes are just for parallel storage not parallel compute
				
			}
		}
		return noMatchCount>=1;
	}

	private boolean countParallelConsumers(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
		int noMatchCount = 0;
		int conInCount = GraphManager.getInputPipeCount(graphManager, consumerId);
		while (--conInCount>=0) {
			//all the other inputs going into the consumer
			Pipe inputPipe = GraphManager.getInputPipe(graphManager, consumerId, 1+conInCount);
			
			//is this the same schema as the pipe in question.
			if (Pipe.isForSameSchema(inputPipe, p)) {
				
				//determine if they are coming from the same place or not
				int prodId = GraphManager.getRingProducerId(graphManager, inputPipe.id);
				if (producerId != prodId) {
					//only count if they are NOT coming from the same place
					noMatchCount++;
					
				} 
				//else if they are all from the samme place then pipes are used here for holding parallel work not for CPU intensive activities

			}
		}
		return noMatchCount>=1;
	}

	private int combineToSameRoot(int rootCounter, int consRoot, int prodRoot) {
		//combine these two roots.
		int newRootId = ++rootCounter;
		if (!IntHashTable.setItem(rootsTable, consRoot, newRootId)) {
			throw new UnsupportedOperationException();
		}
		if (!IntHashTable.setItem(rootsTable, prodRoot, newRootId)) {
			throw new UnsupportedOperationException();
		}
		return rootCounter;
	}

	private int[] buildCountOfStagesForEachThread(GraphManager graphManager, int rootCounter, int[] rootMemberCounter) {
		//long start = System.nanoTime();
		
		Arrays.fill(rootMemberCounter, 0);
	   	
	    int countStages = GraphManager.countStages(graphManager);
	    
		for(int stages=1; stages <= countStages; stages++) { 
	    
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages);
	    	if (null!=stage) {
	    		
	    		int rootId = rootId(stage.stageId, rootsTable);
				rootMemberCounter[rootId]++;	    			
				
				GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, rootId, stage);
				
	    	}
	    	
	    }
		//System.err.println("performance issue here calling this too often "+countStages+" duration "+(System.nanoTime()-start));
	    //logger.info("group counts "+Arrays.toString(rootMemberCounter));
		return rootMemberCounter;
	}
	
	
	

	private PronghornStage[][] buildOrderedArraysOfStages(GraphManager graphManager, int rootCounter) {
	    
		int[] rootMemberCounter = new int[rootCounter+1]; 

	    buildCountOfStagesForEachThread(graphManager, rootCounter, rootMemberCounter);	    

		int stages;
		PronghornStage[][] stageArrays = new PronghornStage[rootCounter+1][]; //one for every stage plus 1 for our new root working room
	    	    
		//////////////
		//walk over all stages once and build the actual arrays
		/////////////
	    stages = GraphManager.countStages(graphManager); 
	    while (stages>=0) {	    		    	
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
	    	if (null!=stage) {
	    		//get the root for this table
	    		int root =rootId(stage.stageId, rootsTable);	    			    		

				int inputCount = GraphManager.getInputPipeCount(graphManager, stage);
				lazyCreateOfArray(rootMemberCounter, stageArrays, root);
				
				boolean isTop=false; 
				
				//find all the entry points where the inputs to this stage are NOT this same root
				for(int j=1; j<=inputCount; j++) {	    			
					int ringProducerStageId = GraphManager.getRingProducerStageId(graphManager, GraphManager.getInputPipe(graphManager, stage, j).id);
					if (rootId(ringProducerStageId, rootsTable) != root 
							|| GraphManager.isStageInLoop(graphManager, stage.stageId)
					    ) {
						isTop = true;
					}
				}
				
				if (0==inputCount || null!=GraphManager.getNota(graphManager, stage.stageId, GraphManager.PRODUCER, null)) {
					isTop=true;
				}								
				
				//Add this and all down stream stages in order only if we have discovered
				//this stage is the top where it starts
				if (isTop) {
					//logger.debug("adding to {}",root);
					add(stageArrays[root], stage, root, graphManager, rootsTable);
				}
				
	    	}
	    }
		return stageArrays;
	}

	private void lazyCreateOfArray(int[] rootMemberCounter, PronghornStage[][] stageArrays, int root) {
		if (rootMemberCounter[root]>0) {
			//create array if not created since count is > 0
			if (null == stageArrays[root]) {
				//System.out.println("count of group "+rootMemberCounter[root]);
				stageArrays[root] = new PronghornStage[rootMemberCounter[root]];
			}					
		}
	}

	//Is this stage in a loop where the pipes pass back to its self.
	
	private void createSchedulers(GraphManager graphManager, PronghornStage[][] stageArrays) {
	
		/////////////
	    //for each array of stages create a scheduler
	    ///////////// 
	    ntsArray = new NonThreadScheduler[threadCount];
	
	    int k = stageArrays.length;
	    int ntsIdx = 0;
	    while (--k >= 0) {
	    	if (null!=stageArrays[k]) {
	    		
	    		if (logger.isDebugEnabled()) {
	    			logger.debug("{} Single thread for group {}", ntsIdx, Arrays.toString(stageArrays[k]) );
	    		}
	    		PronghornStage pronghornStage = stageArrays[k][stageArrays[k].length-1];
				String name = pronghornStage.stageId+":"+pronghornStage.getClass().getSimpleName()+"...";
	    		
	    		
	    		ntsArray[ntsIdx++]=new NonThreadScheduler(graphManager, stageArrays[k], name, true);	    		     
	    	}
	    }
	}

	private void add(PronghornStage[] pronghornStages, PronghornStage stage, final int root, final GraphManager graphManager, final IntHashTable rootsTable) {
		int i = 0;
		while (i<pronghornStages.length && pronghornStages[i]!=null) {
			if (pronghornStages[i]==stage) {
				return;//already added
			}
			i++;
		}
		//now add the new stage at index i
		pronghornStages[i]=stage;
		
		//Recursively add the ones under the same root.
		
		int outputCount = GraphManager.getOutputPipeCount(graphManager, stage.stageId);
		for(int r = 1; r<=outputCount; r++) {
			Pipe outputPipe = GraphManager.getOutputPipe(graphManager, stage, r);
			
			int consumerId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
			//this exists and has the same root so add it
			if (consumerId>=0 && rootId(consumerId, rootsTable)==root) {
				add(pronghornStages, GraphManager.getStage(graphManager, consumerId), root, graphManager, rootsTable);
			}
		}
	}

    private int[] lastKnownRoot = new int[128];
	
	private int rootId(int id, IntHashTable hashTable) {
		
		int item = 0;
		int orig = id;
		do {
			if (id<lastKnownRoot.length && lastKnownRoot[id]!=0) {
				id = lastKnownRoot[id];
			}
			//this code must only read the hash table
			item = IntHashTable.getItem(hashTable, id);
			if (item!=0) {
				cacheLastKnown(orig, item);			
				id = item;
			}
			
		} while (item!=0);
		return id;
	}

	private void cacheLastKnown(int item, int result) {
		int i = lastKnownRoot.length;
		if (item>=i) {
			
			int[] newLast = new int[item*2];
			System.arraycopy(lastKnownRoot, 0, newLast, 0, i);
			lastKnownRoot = newLast;
			
		}
		lastKnownRoot[item]=result;
		
	}

	@Override
	public void startup() {
				
		int realStageCount = threadCount;
		if (realStageCount<=0) {
			System.out.println("Success!, You have a new empty project.");
			return;
		}	
		
        ThreadFactory threadFactory = new ThreadFactory() {
        	int count = threadCount;
			@Override
			public Thread newThread(Runnable r) {
				if (--count>=0) {
					return new Thread(r, ntsArray[count].name());
				} else {
					logger.info("Warning: fixed thread scheduler did not expect more threads to be created than {}", threadCount);
					return new Thread(r,"Unknown");
				}
			}        	
        };
		this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
		

		CyclicBarrier allStagesLatch = new CyclicBarrier(realStageCount+1);
		
		int i = threadCount;
		while (--i>=0) {
			executorService.execute(buildRunnable(allStagesLatch,ntsArray[i]));			
		}		
		
		logger.trace("waiting for startup");
		//force wait for all stages to complete startup before this method returns.
		try {
		    allStagesLatch.await();
        } catch (InterruptedException e) {
        } catch (BrokenBarrierException e) {
        }
		logger.trace("all started up");
		
	}

	private Runnable buildRunnable(final CyclicBarrier allStagesLatch, final NonThreadScheduler nts) {
		assert(null!=allStagesLatch);
		assert(null!=nts);
	
		return new Runnable() {

			@Override
			public void run() {
				
				nts.startup();
				
				try {
			            allStagesLatch.await();
			        } catch (InterruptedException e) {
			        } catch (BrokenBarrierException e) {
			        }
				
				//TODO: fixed thread scheduler must also group by common frequencies
				//      if we have a list with the same rate they can be on a simple loop
				//      this saves the constant checking of which one is to run next...
				
				while (!NonThreadScheduler.isShutdownRequested(nts)) {					
					
					nts.run();
					
					Thread.yield();
				}
			}	
			
		};
	}

	@Override
	public void shutdown() {

		int i = ntsArray.length;
		while (--i>=0) {
			if (null != ntsArray[i]) {
				ntsArray[i].shutdown();	
			}
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
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		
		int i = threadCount;
		boolean cleanExit = true;

		while (--i>=0) {			
			cleanExit &= ntsArray[i].awaitTermination(timeout, unit);			
		}	

		if (!cleanExit) {
			validShutdownState();
			return false;
		}
		//will be null upon empty project, this is ok, just exit.
		if (null!=executorService) {
			//each child scheduler has already completed await termination so no need to wait for this 
			executorService.shutdownNow();
		}
	
		if (null!=firstException) {
		    throw new RuntimeException(firstException);
		}
		return true;
		
	}

	@Override
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


	


}
