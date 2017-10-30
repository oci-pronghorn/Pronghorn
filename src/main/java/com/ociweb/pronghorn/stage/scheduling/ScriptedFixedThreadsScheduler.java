package com.ociweb.pronghorn.stage.scheduling;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorStage;
import com.ociweb.pronghorn.util.ma.RunningStdDev;
import com.ociweb.pronghorn.util.primitive.IntArrayHolder;

public class ScriptedFixedThreadsScheduler extends StageScheduler {

	private int threadCount;

	private ExecutorService executorService;
	private volatile Throwable firstException;//will remain null if nothing is wrong
	private static final Logger logger = LoggerFactory.getLogger(ScriptedFixedThreadsScheduler.class);
	private ScriptedNonThreadScheduler[] ntsArray;

	public ScriptedFixedThreadsScheduler(GraphManager graphManager) {
		//this is often very optimal since we have enough granularity to swap work but we do not
		//have so many threads that it overwhelms the operating system context switching
		this(graphManager, Runtime.getRuntime().availableProcessors()*2);
	}

	public ScriptedFixedThreadsScheduler(GraphManager graphManager, int targetThreadCount) {
		this(graphManager, targetThreadCount, true);
	}

	public ScriptedFixedThreadsScheduler(GraphManager graphManager, int targetThreadCount, boolean enforceLimit) {
		super(graphManager);

		PronghornStage[][] stageArrays = buildStageGroups(graphManager, targetThreadCount, enforceLimit);
		
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

	public static PronghornStage[][] buildStageGroups(GraphManager graphManager, int targetThreadCount, boolean enforceLimit) {
		//must add 1 for the tree of roots also adding 1 more to make hash more efficient.
	    final int countStages = GraphManager.countStages(graphManager);  
		int bits = 1 + (int)Math.ceil(Math.log(countStages)/Math.log(2));
		IntHashTable rootsTable = new IntHashTable(bits);
		
		IntArrayHolder lastKnownRoot = new IntArrayHolder(128);
		
		
		
		PronghornStage[][] stageArrays = buildStageGroups(graphManager, targetThreadCount, 
				                                          enforceLimit, countStages, 
				                                          rootsTable, lastKnownRoot);
		
		verifyNoStagesAreMissing(graphManager, stageArrays);
		
		
		return stageArrays;
	}

	public static void verifyNoStagesAreMissing(GraphManager graphManager, PronghornStage[][] stageArrays) {
		///////////
		//double check that nothing was lost
		/////////
		PronghornStage[] all = GraphManager.allStages(graphManager);
		boolean[] found = new boolean[all.length];
		for(PronghornStage[] list: stageArrays) {
			if (null!=list) {
				for(PronghornStage stage: list) {
					int i = all.length;
					while (--i>=0) {
						if (stage == all[i]) {
							found[i] = true;
							break;
						}
					}
				}
			}
		}
		int j = found.length;
		boolean error = false;
		while (--j>=0) {
			if (!found[j]) {
				error = true;
				System.err.println("not found "+all[j].getClass()+" "+all[j].stageId);			
			}
		}
		if (error) {
			throw new RuntimeException("internal errror, some stages were not scheduled");
		}
	}

	private static PronghornStage[][] buildStageGroups(GraphManager graphManager, 
			int targetThreadCount, boolean enforceLimit,
			final int countStages, 
			final IntHashTable rootsTable, IntArrayHolder lastKnownRoot
			) {
		
		int logLimit=4000;//how many stages that we we can schedule without significant delay.
		
		if (countStages>logLimit) {
			logger.info("beginning to schedule work if {} stages into thread count of {}, this may take some time.",
					countStages,
					targetThreadCount);
		}

	    //created sorted list of pipes by those that should have there stages combined first.
	    Pipe[] pipes = GraphManager.allPipes(graphManager);	  
	    Arrays.sort(pipes, GraphManager.joinFirstComparator(graphManager));
        if (countStages>logLimit) {
        	logger.info("finished inital sorting");
        }
	   
	    
	    //counter for root ids, must not collide with stageIds so we start above that point.	
	    //this is where we decide which stages will be in which thread groups.
	    final int rootCounter = hierarchicalClassifier(rootsTable, lastKnownRoot, graphManager,
									    		targetThreadCount, 
									    		pipes, countStages, enforceLimit);    
	    if (countStages>logLimit) {
	        	logger.info("finished hierarchical classifications");
	    }
	    PronghornStage[][] stageArrays = buildOrderedArraysOfStages(graphManager, rootCounter,
	    														rootsTable, lastKnownRoot);
	  

//	    int total = 0;
//	    int i = stageArrays.length;
//	    while (--i>=0) {
//	    	if (null!=stageArrays[i]) {
//	    		int j = stageArrays[i].length;
//	    		total += j;
//	    	}
//	    	
//	    }
	    
	    if (countStages>logLimit) {
	    	logger.info("finished scheduling");
	    }
		return stageArrays;
	}

	private static int hierarchicalClassifier(IntHashTable rootsTable, IntArrayHolder lastKnownRoot,
			                           GraphManager graphManager, int targetThreadCount, 
			                           Pipe[] pipes, int totalThreads, boolean enforceLimit) {
				
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
    		int consRoot = rootId(consumerId, rootsTable, lastKnownRoot);
    		int prodRoot = rootId(rbms.stageId , rootsTable, lastKnownRoot);
    		if (consRoot!=prodRoot) {
				rootCounter = combineToSameRoot(rootCounter, consRoot, prodRoot, rootsTable);							
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
		    		int consRoot = rootId(consumerId, rootsTable, lastKnownRoot);
		    		int prodRoot = rootId(producerId, rootsTable, lastKnownRoot);
		    		if (consRoot!=prodRoot) {		    			
		    			rootCounter = combineToSameRoot(rootCounter, consRoot, prodRoot, rootsTable);
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
		    	 
		    	 buildCountOfStagesForEachThread(graphManager, rootCounter, 
		    			                         rootMemberCounter, 
		    			                         rootsTable, lastKnownRoot);
		    	 
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
		    		 rootCounter = combineToSameRoot(rootCounter, smallest1root, smallest2root, rootsTable);
		    		 totalThreads--;//removed one thread
		    	 }	
		    	
		    }
	    }
	  
		return rootCounter;
	}

	//rules because some stages should not be combined
	private static boolean isValidToCombine(int ringId, int consumerId, int producerId, GraphManager graphManager) {
				
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

	private static boolean countParallelProducers(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
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

	private static boolean countParallelConsumers(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
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

	private static int combineToSameRoot(int rootCounter, int consRoot, int prodRoot, IntHashTable rootsTable) {
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

	private static int[] buildCountOfStagesForEachThread(GraphManager graphManager,
			 int rootCounter, int[] rootMemberCounter,
			 IntHashTable rootsTable, IntArrayHolder lastKnownRoot) {
		
		Arrays.fill(rootMemberCounter, 0);

		//long start = System.nanoTime();
		
		int countStages = GraphManager.countStages(graphManager);
	    
	    
		for(int stages=0; stages <= countStages; stages++) { 
	    
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages);
	    	if (null!=stage) {	    		
	    		rootMemberCounter[rootId(stage.stageId, rootsTable, lastKnownRoot)]++;	    			

	    	}
	    	
	    }
		//System.err.println("performance issue here calling this too often "+countStages+" duration "+(System.nanoTime()-start));

	    //logger.info("group counts "+Arrays.toString(rootMemberCounter));
		return rootMemberCounter;
	}
	
	
	

	private static PronghornStage[][] buildOrderedArraysOfStages(GraphManager graphManager, int rootCounter,
			IntHashTable rootsTable, IntArrayHolder lastKnownRoot) {
	    
		int[] rootMemberCounter = new int[rootCounter+1]; 

	    buildCountOfStagesForEachThread(graphManager, rootCounter, rootMemberCounter, rootsTable, lastKnownRoot);	    

		PronghornStage[][] stageArrays = new PronghornStage[rootCounter+1][]; //one for every stage plus 1 for our new root working room
	    	    
		//////////////
		//walk over all stages once and build the actual arrays
		/////////////
		int stages = GraphManager.countStages(graphManager);
	    int addsCount = 0;
	    int expectedCount = 0;
	    while (stages>=0) {	    		    	
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
	    	if (null!=stage) {
	    		expectedCount++;
	    		//get the root for this table
	    		int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
	    		lazyCreateOfArray(rootMemberCounter, stageArrays, root);
	    		
				int inputCount = GraphManager.getInputPipeCount(graphManager, stage);
				
				boolean isTop=false; 
				
				//find all the entry points where the inputs to this stage are NOT this same root
				for(int j=1; j<=inputCount; j++) {	    			
					if (root != rootId(GraphManager.getRingProducerStageId(graphManager, GraphManager.getInputPipe(graphManager, stage, j).id), rootsTable, lastKnownRoot)  
							|| GraphManager.isStageInLoop(graphManager, stage.stageId)
					    ) {
						isTop = true;
					}
				}
				
				//if this is a producer use it
				if (0==inputCount 
					|| null!=GraphManager.getNota(graphManager, stage.stageId, GraphManager.PRODUCER, null)) {
					isTop=true;
				}								
				
				//Add this and all down stream stages in order only if we have discovered
				//this stage is the top where it starts
				if (isTop) {
					//logger.debug("adding to {}",root);
					addsCount = add(stageArrays[root], stage, 
						root, graphManager, rootsTable, lastKnownRoot, addsCount);
				}
				
	    	}
	    }
	    
	    //if any were skipped add them now.
	    if (expectedCount != addsCount) {	    	
	    	//A loop was detected and not added, at this point each must be added 
	    	stages = GraphManager.countStages(graphManager);
	    	//get root if not on this list then add.
	    	while (stages>=0) {	    		    	
		    	final PronghornStage stage = GraphManager.getStage(graphManager, stages--);
		    	if (null!=stage) {
		    		//get the root for this table
		    		final int root = rootId(stage.stageId, rootsTable, lastKnownRoot);
		    
		    		boolean needToAdd = true;
		    		PronghornStage[] array = stageArrays[root];
		    		int i = array.length;
		    		if (array[i-1]==null) {
		    			//this array is missing a value
		    			while (--i>=0) {
		    				if (array[i]==stage) {
		    					needToAdd = false;
		    					break;
		    				}
		    			}
		    		}
		    		
		    		if (needToAdd) {
		    			
		    			logger.info("WARNING: loop detected, graph can start faster if one Stage was selected as the Producer. {}"
		    					, stage.getClass() );
		    			add(stageArrays[root], stage, 
		    				root, graphManager, rootsTable, lastKnownRoot, 0);

		    		}
		    	}
	    	}
	    }
		return stageArrays;
	}

	private static void lazyCreateOfArray(int[] rootMemberCounter, PronghornStage[][] stageArrays, int root) {
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
	
		int j = stageArrays.length; //TODO: refactor into a recursive single pass count.
		int count = 0;
		while (--j>=0) {
			if (null!=stageArrays[j]) {
				count++;
			}
		}
		threadCount=count;
		
		/////////////
	    //for each array of stages create a scheduler
	    ///////////// 
	    ntsArray = new ScriptedNonThreadScheduler[threadCount];
	
	    int k = stageArrays.length;
	    int ntsIdx = 0;
	    while (--k >= 0) {
	    	if (null!=stageArrays[k]) {
	    		
	    		if (logger.isDebugEnabled()) {
	    			logger.debug("{} Single thread for group {}", ntsIdx, Arrays.toString(stageArrays[k]) );
	    		}
	    		PronghornStage pronghornStage = stageArrays[k][stageArrays[k].length-1];
				String name = pronghornStage.stageId+":"+pronghornStage.getClass().getSimpleName()+"...";
	    
				//TODO: NOTE: this is optimized for low load conditions, eg the next pipe has room.
				//      This boolean can be set to true IF we know the pipe will be full all the time
				//      By setting this to true the scheduler is optimized for heavy loads
				//      Each individual part of the graph can have its own custom setting... 
				boolean reverseOrder = false;
	    		
	    		ntsArray[ntsIdx++] = new ScriptedNonThreadScheduler(graphManager, reverseOrder, stageArrays[k], name, true);
	    	}
	    }
	}

	private static int add(PronghornStage[] pronghornStages, PronghornStage stage,
			              final int root, final GraphManager graphManager, 
			              final IntHashTable rootsTable, IntArrayHolder lastKnownRoot,
			              int count) {
		
		int i = 0;
		while (i<pronghornStages.length && pronghornStages[i]!=null) {
			if (pronghornStages[i]==stage) {
				return count;//already added
			}
			i++;
		}
		//now add the new stage at index i
		pronghornStages[i]=stage;
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, root, stage);
		count++;
		
		//Recursively add the ones under the same root.
		
		int outputCount = GraphManager.getOutputPipeCount(graphManager, stage.stageId);
		for(int r = 1; r<=outputCount; r++) {
			Pipe outputPipe = GraphManager.getOutputPipe(graphManager, stage, r);
			
			int consumerId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
			//this exists and has the same root so add it
			if (consumerId>=0 && rootId(consumerId, rootsTable, lastKnownRoot)==root) {
				count = add(pronghornStages, GraphManager.getStage(graphManager, consumerId),
						     root, graphManager, rootsTable, lastKnownRoot, count);
			}
		}
		return count;
	}


	private static int rootId(int id, IntHashTable hashTable, IntArrayHolder lastKnownRoot) {
		
		int item = 0;
		int orig = id;
		do {
			if (id<lastKnownRoot.data.length && lastKnownRoot.data[id]!=0) {
				id = lastKnownRoot.data[id];
			}
			//this code must only read the hash table
			item = IntHashTable.getItem(hashTable, id);
			if (item!=0) {
				lastKnownRoot.growIfNeeded(orig);
				lastKnownRoot.data[orig]=item;			
				id = item;
			}
			
		} while (item!=0);
		return id;
	}

	@Override
	public void startup() {
				
		int realStageCount = threadCount;
		if (realStageCount<=0) {
			System.out.println("Success!, You have a new empty project.");
			return;
		}	
		
        ThreadFactory threadFactory = new ThreadFactory() {
        	int count = ntsArray.length;
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
		
		int i = ntsArray.length;
		while (--i>=0) {
			executorService.execute(buildRunnable(allStagesLatch, ntsArray[i]));
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

	private Runnable buildRunnable(final CyclicBarrier allStagesLatch, final ScriptedNonThreadScheduler nts) {
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
				
				while (!ScriptedNonThreadScheduler.isShutdownRequested(nts)) {
					
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
		
		int i = ntsArray.length;
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
