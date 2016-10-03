package com.ociweb.pronghorn.stage.scheduling;

import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;

public class FixedThreadsScheduler extends StageScheduler {

	public class JoinFirstComparator implements Comparator<Pipe> {

		private final GraphManager graphManager;
		public JoinFirstComparator(GraphManager graphManager) {
			this.graphManager = graphManager;
		}

		@Override
		public int compare(Pipe p1, Pipe p2) {
			return Integer.compare(weight(p1),weight(p2));
		}
		
		public int weight(Pipe p) {
			
			int result = (int)p.config().slabBits();
			
			//returns the max pipe length from this pipe or any of the pipes that feed its producer.
			//if this value turns out to be large then we should probably not join these two stages.
			
			int producerId = GraphManager.getRingProducerId(graphManager, p.id);		
			if (producerId>=0) {
				PronghornStage producer = GraphManager.getStage(graphManager, producerId);			
				int count = GraphManager.getInputPipeCount(graphManager, producer);
				while (--count>=0) {
					Pipe<MessageSchema> inputPipe = GraphManager.getInputPipe(graphManager, producer, count);				
					result = Math.max(result, inputPipe.config().slabBits());
				}
			} else {
				//no producer found, an external thread must be pushing data into this, there is nothing to combine it with				
			}
			return result;
			
		}
		
	}

	private int threadCount;
	private final IntHashTable rootsTable;
	private ExecutorService executorService;
	private volatile Throwable firstException;//will remain null if nothing is wrong
	private static final Logger logger = LoggerFactory.getLogger(FixedThreadsScheduler.class);
	private NonThreadScheduler[] ntsArray;
	
	public FixedThreadsScheduler(GraphManager graphManager, int targetThreadCount) {
		super(graphManager);
		
		
	    final Comparator<? super Pipe> joinFirstComparator = new JoinFirstComparator(graphManager);
	    //created sorted list of pipes by those that should have there stages combined first.
	    Pipe[] pipes = GraphManager.allPipes(graphManager);	  
	    Arrays.sort(pipes, joinFirstComparator);

	    
	    int totalThreads = GraphManager.countStages(graphManager);  
	  
	    //must add 1 for the tree of roots also adding 1 more to make hash more efficient.
	    rootsTable = new IntHashTable(2 + (int)Math.ceil(Math.log(totalThreads)/Math.log(2)));
	    
	    int rootCounter = totalThreads+1; //counter for root ids, must not collide with stageIds so we start above that point.
	   
	    rootCounter = hierarchicalClassifier(graphManager, targetThreadCount, pipes, totalThreads, rootCounter);    
	    

	    int[] rootMemberCounter = buildCountOfStagesForEachThread(graphManager, rootCounter);	    

	    
	    PronghornStage[][] stageArrays = buildOrderedArraysOfStages(graphManager, rootCounter, rootMemberCounter);
	    
	    createSchedulers(graphManager, stageArrays);
	    
	    stageArrays=null;
	    
	    
	    //TODO: build dot file? ///////////////////////////////////////
	    //GraphManager.writeAsDOT(graphManager, System.out);
	    
	    //clean up now before we begin.
		System.gc();
		
	}

	private int hierarchicalClassifier(GraphManager graphManager, int targetThreadCount, Pipe[] pipes, int totalThreads, int rootCounter) {
		//logger.debug("beginning threads {}",totalThreads);
		//loop over pipes once and stop early if total threads is smaller than or matches the target thread count goal
	    int i = pipes.length;
	    while (totalThreads>targetThreadCount && --i>=0) {	    	
	    	int ringId = pipes[i].id;
	    	
	    	int consumerId = GraphManager.getRingConsumerId(graphManager, ringId);
	    	int producerId = GraphManager.getRingProducerId(graphManager, ringId);
	    	//only join stages if they both exist
	    	if (consumerId>0 && producerId>0) {	    		
	    		//determine if they are already merged in the same tree?
	    		int consRoot = rootId(consumerId, rootsTable);
	    		int prodRoot = rootId(producerId, rootsTable);
	    		if (consRoot!=prodRoot) {
	    			
	    			//TODO: if this is already added wait ...... we must buld up from short depths to longer.
	    			
	    			
	    			
	    			
	    			
	    			
	    			//combine these two roots.
	    			int newRootId = ++rootCounter;
	    			if (!IntHashTable.setItem(rootsTable, consRoot, newRootId)) {
	    				throw new UnsupportedOperationException();
	    			}
	    			if (!IntHashTable.setItem(rootsTable, prodRoot, newRootId)) {
	    				throw new UnsupportedAddressTypeException();
	    			}
	    			//logger.debug("grouping {} and {} under {} ",consRoot,prodRoot,newRootId);
	    			totalThreads--;//removed one thread
	    		}
	    		//else nothing already combined
	    	}
	    }
	    threadCount = totalThreads;
	    //logger.debug("Threads Requested: {} Threads Used: {}",targetThreadCount,threadCount);
		return rootCounter;
	}

	private int[] buildCountOfStagesForEachThread(GraphManager graphManager, int rootCounter) {
		
	    int[] rootMemberCounter = new int[rootCounter+1]; //TODO: keep this for later??
	    int countStages = GraphManager.countStages(graphManager);
	    
		for(int stages=1;stages<=countStages;stages++) { 
	    
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages);
	    	if (null!=stage) {
	    		int rootId = rootId(stage.stageId, rootsTable);
				rootMemberCounter[rootId]++;	    			
				//This late NOTA only works because we wrote a placeholder of null when stages are created.
				GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, rootId, stage);
	    	}
	    }
	    //logger.info("group counts "+Arrays.toString(rootMemberCounter));
		return rootMemberCounter;
	}

	private PronghornStage[][] buildOrderedArraysOfStages(GraphManager graphManager, int rootCounter, int[] rootMemberCounter) {
		int stages;
		PronghornStage[][] stageArrays = new PronghornStage[rootCounter+1][];
	    	    
	    stages = GraphManager.countStages(graphManager); 
	    while (stages>=0) {	    		    	
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
	    	if (null!=stage) {
	    		int root =rootId(stage.stageId, rootsTable);	    			    		
	    		//find all the entry points where the inputs to this stage are NOT this same root

				int inputCount = GraphManager.getInputPipeCount(graphManager, stage);
				if (rootMemberCounter[root]>0) {
					//create array if not created since count is > 0
					if (null==stageArrays[root]) {
						//System.out.println("count of group "+rootMemberCounter[root]);
						stageArrays[root] = new PronghornStage[rootMemberCounter[root]];
					}					
				}
				
				boolean isTop=false; 
				
				for(int j=1; j<=inputCount; j++) {	    			
					int ringProducerStageId = GraphManager.getRingProducerStageId(graphManager, GraphManager.getInputPipe(graphManager, stage, j).id);
					if (rootId(ringProducerStageId, rootsTable) != root || 
					    isInLoop(stage.stageId, graphManager)) {
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

	private static boolean isInLoop(int stageId, GraphManager graphManager) {		
		return isInPath(stageId, stageId, graphManager, GraphManager.countStages(graphManager));
	}

	private static boolean isInPath(int stageId, final int targetId, GraphManager graphManager, int maxRecursionDepth) {
		//search all nodes until the end is reached or we see the duplicate
		if (maxRecursionDepth>0) {
			PronghornStage stage = GraphManager.getStage(graphManager, stageId);
			
			int c = GraphManager.getOutputPipeCount(graphManager, stageId);
			for(int i=1; i<=c; i++) {
				
				Pipe<MessageSchema> outputPipe = GraphManager.getOutputPipe(graphManager, stage, i);
							
				int consumerId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
				if (consumerId >= 0) {
					//if stageId is not found then it is not in a loop but the consumer Id could be in a loop unrelated to the StageID, 
					//to defend against this we have a maximum depth based on the stage count in the graphManager					
				    if ((consumerId == targetId) || (isInPath(consumerId, targetId, graphManager, --maxRecursionDepth))) {
				    	return true;
				    }
				}			
			}
		}
		return false;
	}
	
	

	private void createSchedulers(GraphManager graphManager, PronghornStage[][] stageArrays) {
	
		/////////////
	    //for each array of stages create a scheduler
	    ///////////// 
	    ntsArray = new NonThreadScheduler[threadCount];
	
	    int k = stageArrays.length;
	    int ntsIdx = 0;
	    while (--k >= 0) {
	    	if (null!=stageArrays[k]) {
	    		
	    		//System.err.println("NonThreadScheduler for "+Arrays.toString(stageArrays[k]) );
	    		
	    		ntsArray[ntsIdx++]=new NonThreadScheduler(graphManager, stageArrays[k]);	    		     
	    	}
	    }
	}

	private static void add(PronghornStage[] pronghornStages, PronghornStage stage, final int root, final GraphManager graphManager, final IntHashTable rootsTable) {
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
			Pipe<MessageSchema> outputPipe = GraphManager.getOutputPipe(graphManager, stage, r);
			
			int consumerId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
			//this exists and has the same root so add it
			if (consumerId>=0 && rootId(consumerId, rootsTable)==root) {
				add(pronghornStages, GraphManager.getStage(graphManager, consumerId), root, graphManager, rootsTable);
			}
		}
	}

	private static int rootId(int id, IntHashTable hashTable) {		
		if (!IntHashTable.hasItem(hashTable, id)) {
			return id;//not found so the id is the root
		}
		return rootId(IntHashTable.getItem(hashTable, id), hashTable);
	}

	@Override
	public void startup() {
				
        this.executorService = Executors.newFixedThreadPool(threadCount);
		
		int realStageCount = threadCount;

		CyclicBarrier allStagesLatch = new CyclicBarrier(realStageCount+1);
		
		int i = threadCount;
		while (--i>=0) {
			executorService.execute(buildRunnable(allStagesLatch,ntsArray[i]));			
		}		
		
		//force wait for all stages to complete startup before this method returns.
		try {
		    allStagesLatch.await();
        } catch (InterruptedException e) {
        } catch (BrokenBarrierException e) {
        }
		
		
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
				
				while (!NonThreadScheduler.isShutdownRequested(nts)) {
					nts.run();//nts.run has its own internal sleep, nothing needed here.					
				}
			}	
			
		};
	}

	@Override
	public void shutdown() {
		
		int i = threadCount;
		while (--i>=0) {			
			ntsArray[i].shutdown();			
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
		//each child scheduler has already completed await termination so no need to wait for this 
		executorService.shutdownNow();
	
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
