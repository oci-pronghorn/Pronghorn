package com.ociweb.pronghorn.stage.scheduling;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.sun.corba.se.impl.orbutil.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientSocketWriterStage;
import com.ociweb.pronghorn.network.ServerNewConnectionStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorStage;
import com.ociweb.pronghorn.util.ma.RunningStdDev;
import com.ociweb.pronghorn.util.primitive.IntArrayHolder;

public class ScriptedFixedThreadsScheduler extends StageScheduler {

	private int threadCount;

	private ExecutorService executorService;
	private volatile Throwable firstException;//will remain null if nothing is wrong
	private static final Logger logger = LoggerFactory.getLogger(ScriptedFixedThreadsScheduler.class);
	private ScriptedNonThreadScheduler[] ntsArrayForward;
	private Pipe[] ntsArrayForwardInputs;
	private long halfTotalSlabSize;
	private ScriptedNonThreadScheduler[] ntsArrayReverse;

    private static final Comparator comp = new Comparator<PronghornStage[]>(){

		@Override
		public int compare(PronghornStage[] o1, PronghornStage[] o2) {
			int len1 = null==o1?-1:o1.length;
			int len2 = null==o2?-1:o2.length;
			
			return len2-len1;
		}
    	
    };
    
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
    
	    if (enforceLimit) {
	    	enforceThreadLimit(targetThreadCount, stageArrays);
	    }
	    
	    
	    if (countStages>logLimit) {
	    	logger.info("finished scheduling");
	    }
		return stageArrays;
	}

	private static void enforceThreadLimit(int targetThreadCount, PronghornStage[][] stageArrays) {
		////////////
	    
	    Arrays.sort(stageArrays, comp );
	    
	    int countOfGroups = 0;
	    int x = stageArrays.length;
	    while (--x>=0) {
	    	if (null!=stageArrays[x] && stageArrays[x].length>0) {
	    		countOfGroups++;
	    	}
	    }
	    
	    //we have too many threads so start building some combindations
	    //this is done from the middle because the small 1's must be isolated and the
	    //large ones already have too much work to do.
	    int threadCountdown = (countOfGroups-targetThreadCount);
	    boolean debug = false;
	    while (--threadCountdown>=0) {
	    	int idx = countOfGroups/2;

	    	PronghornStage[] newArray = new PronghornStage[
	    	                                                stageArrays[idx].length
	    	                                               +stageArrays[idx-1].length];
	    	
	    	System.arraycopy(stageArrays[idx], 0, newArray, 0, stageArrays[idx].length);
	    	System.arraycopy(stageArrays[idx-1], 0, newArray, stageArrays[idx].length, stageArrays[idx-1].length);
	    	stageArrays[idx] = newArray;
	    	stageArrays[idx-1] = null;
	    	
		    Arrays.sort(stageArrays, comp );
		    
		    countOfGroups = 0;
		    x = stageArrays.length;
		    while (--x>=0) {
		    	if (null!=stageArrays[x] && stageArrays[x].length>0) {
		    		countOfGroups++;
		    	}
		    }
	    	
	    }
	 
	    
	    if (debug) {
		    countOfGroups = 0;
		    x = stageArrays.length;
		    while (--x>=0) {
		    	if (null!=stageArrays[x] && stageArrays[x].length>0) {
		    		countOfGroups++;
		    		System.err.println(stageArrays[x].length);
		    	}
		    }
	    }
	}

	private static int hierarchicalClassifier(IntHashTable rootsTable, IntArrayHolder lastKnownRoot,
			                           GraphManager graphManager, int targetThreadCount, 
			                           Pipe[] pipes, int totalThreads, boolean enforceLimit) {
				
		int rootCounter =  totalThreads+1;
		
		//////////////////////////////////////////////////
		//before we begin combine all monitior stages on a single thread to minimize disruption across the graph.
		///////////////////////////////////////////////////
		PronghornStage[] monitors = GraphManager.allStagesByType(graphManager, PipeMonitorStage.class);
		int m = monitors.length;
		PronghornStage prevMonitor = null;
		while (--m >= 0) {
			PipeMonitorStage rbms = (PipeMonitorStage)monitors[m];
			if (prevMonitor!=null) {
			
				int prodRoot1 = rootId(rbms.stageId , rootsTable, lastKnownRoot);
				int prodRoot2 = rootId(prevMonitor.stageId , rootsTable, lastKnownRoot);
				if (prodRoot1!=prodRoot2) {
					rootCounter = combineToSameRoot(rootCounter, prodRoot1, prodRoot2, rootsTable);
					totalThreads--;//removed one thread
				}
			}
			prevMonitor = rbms;			
		}
		///////////////////////////////////////////////
		//done combining the monitor stages.
		///////////////////////////////////////////////
			
		
		//loop over pipes once and stop early if total threads is smaller than or matches the target thread count goal
	    int i = pipes.length;
	    while (/*totalThreads>targetThreadCount &&*/ --i>=0) {	    	
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
	    
	  
		return rootCounter;
	}

	//rules because some stages should not be combined
	private static boolean isValidToCombine(int ringId, int consumerId, int producerId, GraphManager graphManager) {
				
		Pipe p = GraphManager.getPipe(graphManager, ringId);
		
		PronghornStage consumerStage = GraphManager.getStage(graphManager, consumerId);
		PronghornStage producerStage = GraphManager.getStage(graphManager, producerId);

		if (consumerStage instanceof MonitorConsoleStage ) {
			return false;
		}
		if (producerStage instanceof MonitorConsoleStage ) {
			return false;
		}		
		
		if (producerStage instanceof ServerNewConnectionStage ) {
			return false;
		}
		if (consumerStage instanceof ServerNewConnectionStage ) {
			return false;
		}	
		
		//if producer sends to n consumers each with the same scheme 
	    //and each with a heavy compute stage then keep the split, never join.
		if (isSplittingHeavyComputeLoad(consumerId, producerId, graphManager, p)) {
			return false;
		}
		if (isMergeOfHeavyComputeLoad(consumerId, producerId, graphManager, p)) {
			return false;
		}
		

        //NOTE: this also helps with the parallel work above, any large merge or split will show up here.
		//do not combine with stages which are 2 standard deviations above the mean input/output count
		if (GraphManager.countStages(graphManager)>100) {//only apply if we have large enough sample.
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
		}
		
		return true;
	}
	
		

	private static boolean isSplittingHeavyComputeLoad(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
		int countOfHeavyComputeConsumers = 0;
		final int totalOutputsCount = GraphManager.getOutputPipeCount(graphManager, producerId);
		if (totalOutputsCount<=1) {
			return false;
		}
		int proOutCount = totalOutputsCount;
        while (--proOutCount>=0) {
			//all the other output coming from the producer
			Pipe outputPipe = GraphManager.getOutputPipe(graphManager, producerId, 1+proOutCount);
			
			//is this the same schema as the pipe in question.
			if (Pipe.isForSameSchema(outputPipe, p)) {
				
				//determine if they are consumed by the same place or not
				int conId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
				
				if (GraphManager.hasNota(graphManager, conId, GraphManager.HEAVY_COMPUTE)) {
					countOfHeavyComputeConsumers++;
					
				} else {
					//TODO: we can go deeper down this chain as needed to discover heavy compute....
					
					
				}
				
			}
		}
		//every output pipe has the same schema and leads to heavy compute work
        return countOfHeavyComputeConsumers == totalOutputsCount;
	}

	
	
	private static boolean isMergeOfHeavyComputeLoad(int consumerId, int producerId, GraphManager graphManager, Pipe p) {
		int countOfHeavyComputeProducers = 0;
		int totalInputsCount = GraphManager.getInputPipeCount(graphManager, consumerId);
		if (totalInputsCount<=1) {
			return false;
		}
		int conInCount = totalInputsCount;
		while (--conInCount>=0) {
			//all the other inputs going into the consumer
			Pipe inputPipe = GraphManager.getInputPipe(graphManager, consumerId, 1+conInCount);
			
			//is this the same schema as the pipe in question.
			if (Pipe.isForSameSchema(inputPipe, p)) {
				
				//determine if they are consumed by the same place or not
				int prodId = GraphManager.getRingProducerId(graphManager, inputPipe.id);
				
				if (GraphManager.hasNota(graphManager, prodId, GraphManager.HEAVY_COMPUTE)) {
					countOfHeavyComputeProducers++;
					
				} else {
					//TODO: we can go deeper up this chain as needed to discover heavy compute....
					
					
				}
			}
		}
		return countOfHeavyComputeProducers == totalInputsCount;
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
	
	
	

	private static PronghornStage[][] buildOrderedArraysOfStages(GraphManager graphManager,
			int rootCounter,
			IntHashTable rootsTable,
			IntArrayHolder lastKnownRoot) {
	    
		int[] rootMemberCounter = new int[rootCounter+1]; 

	    buildCountOfStagesForEachThread(graphManager, rootCounter, rootMemberCounter, rootsTable, lastKnownRoot);	    

		PronghornStage[][] stageArrays = new PronghornStage[rootCounter+1][]; //one for every stage plus 1 for our new root working room
	    	    
		//////////////
		//walk over all stages once and build the actual arrays
		/////////////
		int stages = GraphManager.countStages(graphManager);
	    int addsCount = 0;
	    int expectedCount = 0;
	    while (stages >= 0) {	    		 
	    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
	    	if (null!=stage) {	    	    
	    		expectedCount++;
	    		//Add all the producers first...
	    		if (
	    			   (0==GraphManager.getInputPipeCount(graphManager, stage.stageId))	
	    			|| (null!=GraphManager.getNota(graphManager, stage.stageId, GraphManager.PRODUCER, null))) {
	    			
	    			//get the root for this table
					int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
					lazyCreateOfArray(rootMemberCounter, stageArrays, root);
					addsCount = add(stageArrays, stage, 
											root, graphManager, rootsTable, 
											lastKnownRoot, addsCount, rootMemberCounter, false);
	    			
	    		}
				
	    	}
	    }
	    
	    //collect a list of down stream stages to do next right after the prodcuers.
	    
	    
	    if (expectedCount != addsCount) {
		    
	    	logger.warn("We have {} stages which do not consume from any known producers, To ensure efficient scheduling please mark one as the producer.", expectedCount-addsCount);
	    			
	    	
		    stages = GraphManager.countStages(graphManager);
		    while (stages >= 0) {	    		 
		    	//Then ensure everything has beed added
		    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
		    	if (null!=stage) {
		    	    	
		    		
		    		
					//get the root for this table
					int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
					lazyCreateOfArray(rootMemberCounter, stageArrays, root);
	
						
		    		addsCount = add(stageArrays, stage, 
									root, graphManager, rootsTable, 
									lastKnownRoot, addsCount, rootMemberCounter, true);
					
		    	}
		    }
	    }
	    
	    assert(expectedCount==addsCount) : "Some stages left unscheduled, internal error.";
	    
	    boolean debug = false;
	    if (debug){
	        for(PronghornStage[] st: stageArrays) {
	        	if(null!=st) {
		        	System.err.println();
			        System.err.println("----------full stages ------------- "); //these look backward??
			        for(int i = 0; i<st.length; i++) {
			        	
			        	StringBuilder target = new StringBuilder();
			    		target.append("full stages "+st[i].getClass().getSimpleName());
			    		target.append("  inputs:");
			    		GraphManager.appendInputs(graphManager, target, st[i]);
			    		target.append(" outputs:");
			    		GraphManager.appendOutputs(graphManager, target, st[i]);
			    		        		
			    		System.err.println("   "+target);
			        	
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

		// Determine number of input pipes in the forward array.
		int numberInputPipes = 0;
		for (int i = 0; i < count; i++) {
			PronghornStage[] stages = stageArrays[i];

			for (int k = 0; k < stages.length; k++) {
				numberInputPipes += GraphManager.getInputPipeCount(graphManager, stages[k]);
			}
		}

		// Create metadata for forward pipes.
		ntsArrayForwardInputs = new Pipe[numberInputPipes];

		int ntsArraysIdx = 0;
		for (int i = 0; i < count; i++) {
			PronghornStage[] stages = stageArrays[i];

			for (int k = 0; k < stages.length; k++) {

				for (int L = 1; L <= GraphManager.getInputPipeCount(graphManager, stages[k]); L++) {

					ntsArrayForwardInputs[ntsArraysIdx] = GraphManager.getInputPipe(graphManager, stages[k], L);
					halfTotalSlabSize += GraphManager.getInputPipe(graphManager, stages[k], L).sizeOfSlabRing;
				}
			}
		}

		// Finalize total slab size based on cumulative total.
		halfTotalSlabSize = halfTotalSlabSize / 2;

		// Allocate NTS arrays.
		ntsArrayForward = new ScriptedNonThreadScheduler[count];
		ntsArrayReverse = new ScriptedNonThreadScheduler[count];

	    int ntsIdx = 0;
	    for (int i = 0; i < count; i++) {
	    	if (null != stageArrays[i]) {
	    		
	    		if (logger.isDebugEnabled()) {
	    			logger.debug("{} Single thread for group {}", ntsIdx, Arrays.toString(stageArrays[i]));
	    		}
	    		PronghornStage pronghornStage = stageArrays[i][stageArrays[i].length - 1];
				String name = pronghornStage.stageId+":"+pronghornStage.getClass().getSimpleName()+"...";
	    		
	    		ntsArrayForward[ntsIdx] = new ScriptedNonThreadScheduler(graphManager, false, stageArrays[i], name, true);
				ntsArrayReverse[ntsIdx] = new ScriptedNonThreadScheduler(graphManager, true, stageArrays[i], name, true);

				ntsIdx++;
	    	}
	    }
	}

	private static int[] tempRanks = new int[1024];
	
	private static int add(PronghornStage[][] stageArrays, PronghornStage stage,
			              final int root, final GraphManager graphManager, 
			              final IntHashTable rootsTable, IntArrayHolder lastKnownRoot,
			              int count, int[] rootMemberCounter, boolean log) {

		
		boolean isLarge = false;//TODO: add support for large threading model
		
		PronghornStage[] pronghornStages = stageArrays[root];
				
		int i = 0;
		while (i<pronghornStages.length && pronghornStages[i]!=null) {
			if (pronghornStages[i]==stage) {
				return count;//already added
			}
			i++;
		}
		//now add the new stage at index i
		pronghornStages[i]=stage;
		
		if (log) {
			logger.info("added stage {}",stage);
		}
		
		GraphManager.addNota(graphManager, GraphManager.THREAD_GROUP, root, stage);
		count++;
		
		//Recursively add the ones under the same root.
		
		int outputCount = GraphManager.getOutputPipeCount(graphManager, stage.stageId);
		
		if (GraphManager.hasNota(graphManager, stage.stageId, GraphManager.LOAD_BALANCER)) {

			//load balancer must add all left to right as they are defined.
			for(int r = 1; r<=outputCount; r++) {
				
				Pipe outputPipe = GraphManager.getOutputPipe(graphManager, stage, r);
				count = recursiveAdd(stageArrays, root, graphManager, 
						             rootsTable, lastKnownRoot, count, rootMemberCounter,
						             log, 
						             GraphManager.getRingConsumerId(graphManager, outputPipe.id));
				
			}
			
		} else {
			
			if (tempRanks.length <= outputCount) {
				tempRanks = new int[outputCount+1];
			}
			
			for(int r = 1; r<=outputCount; r++) {
				int consumerId = GraphManager.getRingConsumerId(graphManager, GraphManager.getOutputPipe(graphManager, stage, r).id);
				tempRanks[r] = getNearnessRank(consumerId, graphManager, pronghornStages);
			}
						
			int targetRank = 0;
			do {
				for(int r = 1; r<=outputCount; r++) {
					
					if ( ((targetRank>=i)&&(tempRanks[r]>=targetRank)) 
						 ||(targetRank==tempRanks[r])) {
					
						count = recursiveAdd(stageArrays, root, graphManager, 
								             rootsTable, lastKnownRoot, count, rootMemberCounter,
								             log,
								             GraphManager.getRingConsumerId(graphManager, GraphManager.getOutputPipe(graphManager, stage, r).id));
						
					}
				}
				targetRank++;
			} while (targetRank<=i && pronghornStages[pronghornStages.length-1]==null);
			
		}
		
		return count;
	}

	private static int recursiveAdd(PronghornStage[][] stageArrays, final int root, final GraphManager graphManager,
			final IntHashTable rootsTable, IntArrayHolder lastKnownRoot, 
			int count, int[] rootMemberCounter,
			boolean log, int consumerId) {
		
		
		if ( GraphManager.hasNota(graphManager, consumerId, GraphManager.LOAD_MERGE)) {
	
			////////////////
			//if all the consumers inputs have been added then we can add this one
			//but never before this point
			///////////////
			int inC = GraphManager.getInputPipeCount(graphManager, consumerId);
			
			for(int i = 1; i<=inC ; i++) {
				Pipe inPipe = GraphManager.getInputPipe(graphManager, consumerId, i);				
				int inProd = GraphManager.getRingProducerStageId(graphManager, inPipe.id);
				PronghornStage[] prodsList = stageArrays[rootId(inProd, rootsTable, lastKnownRoot)];
				
				boolean found = false;
				int j = prodsList.length;
				while (--j>=0) {
					if (null != prodsList[j]) {
						found |= (prodsList[j].stageId==inProd);
					}
				}
				if (!found) {
					//this one was not already added so push this off till later.
					return count;
				}				
			}
			
		}
		
		/////////////
		//recursive add
		//////////////
		int localRootId = rootId(consumerId, rootsTable, lastKnownRoot);
		if (localRootId==root) {
			count = add(stageArrays, GraphManager.getStage(graphManager, consumerId),
					     root, graphManager, rootsTable, lastKnownRoot, count, 
					     rootMemberCounter, log);
		} else {
			lazyCreateOfArray(rootMemberCounter, stageArrays, localRootId);
			PronghornStage[] otherArray = stageArrays[localRootId];
			if (null==otherArray[0]) {
				//only add for this one case
				//at this point we have changed nodes since this is an entry point.
				count = add(stageArrays, GraphManager.getStage(graphManager, consumerId),
						localRootId, graphManager, rootsTable, lastKnownRoot,
						count, rootMemberCounter, log);
			}
		}
		////////
		//end of recursive add
		////////
		return count;
	}

    /**
     * starting at the i position in the array work back and determine which input of consumerId
     * has the maximum distance, then return that value. if not found return max int.
     * 
     * this provides a "rank" to determine how "near" this consumer is to the most recent stages.
     */
	private static int getNearnessRank(int consumerId,
			                           GraphManager graphManager, 
			                           PronghornStage[] pronghornStages) {

		if (GraphManager.getStage(graphManager, consumerId) instanceof ClientSocketWriterStage ) { //HTTPClientRequestTrafficStage 4
			System.err.println();
		}
		
		int maxSteps = 0;
		int inC = GraphManager.getInputPipeCount(graphManager, consumerId);
		for(int j = 1; j<=inC; j++) {
			
			int prodId = GraphManager.getRingProducerId(graphManager, GraphManager.getInputPipe(graphManager, consumerId, j).id);
			
			int x = pronghornStages.length;
			int steps = 0;
			while (--x>=0) {
				if (null != pronghornStages[x]) {
					if (prodId == pronghornStages[x].stageId) {
						break;
					} else {
						steps++;//count how many steps back
					}
				}
			}
			if (x<0) {
				//was not found, so this gets the longest distance
				steps = Integer.MAX_VALUE;
			}
			
			if (steps > maxSteps) {
				maxSteps = steps;
			}
			
		}
		return maxSteps;
		
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
        	int count = ntsArrayForward.length;
			@Override
			public Thread newThread(Runnable r) {
				if (--count>=0) {
					return new Thread(r, ntsArrayForward[count].name());
				} else {
					logger.info("Warning: fixed thread scheduler did not expect more threads to be created than {}", threadCount);
					return new Thread(r,"Unknown");
				}
			}        	
        };
		this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
		

		CyclicBarrier allStagesLatch = new CyclicBarrier(realStageCount+1);
		
		int i = ntsArrayForward.length;
		while (--i>=0) {
			executorService.execute(buildRunnable(allStagesLatch, i));
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

	private Runnable buildRunnable(final CyclicBarrier allStagesLatch, final int ntsIdx) {
		assert(null!=allStagesLatch);

		final ScriptedNonThreadScheduler ntsf = ntsArrayForward[ntsIdx];
		final ScriptedNonThreadScheduler ntsr = ntsArrayReverse[ntsIdx];

		assert(null!=ntsf);
		assert(null!=ntsr);
	
		return new Runnable() {

			// Interval to check if we should run forward or reverse scheduler.
			boolean forward = true;
			long checkCounter = 0;
			int checkInterval = 1024;

			@Override
			public void run() {
				
				ntsf.startup();
				ntsr.startup();
				
				try {
			            allStagesLatch.await();
			        } catch (InterruptedException e) {
			        } catch (BrokenBarrierException e) {
			        }
				
				//TODO: fixed thread scheduler must also group by common frequencies
				//      if we have a list with the same rate they can be on a simple loop
				//      this saves the constant checking of which one is to run next...
				
				while (!ScriptedNonThreadScheduler.isShutdownRequested(ntsf) &&
					   !ScriptedNonThreadScheduler.isShutdownRequested(ntsr)) {

					// Check to see if forward pipe is backed up on an interval.
					if (checkCounter % checkInterval == 0) {

						// Sum total pipes.
						long sum = 0;
						for (int i = 0; i < ntsArrayForwardInputs.length; i++) {
							sum += Pipe.contentRemaining(ntsArrayForwardInputs[i]);
						}

						// Run forward if the sum is less than half the total slab size.
						forward = sum < halfTotalSlabSize;
					}

					// Progress check counter.
					checkCounter++;

					// Run forward or reverse pipe depending on current pipe data.
					if (forward) {
						ntsf.run();
					} else {
						ntsr.run();
					}
					
					Thread.yield();
				}
			}	
			
		};
	}

	@Override
	public void shutdown() {

		int i = ntsArrayForward.length;
		while (--i>=0) {
			if (null != ntsArrayForward[i]) {
				ntsArrayForward[i].shutdown();
			}

			if (null != ntsArrayReverse[i]) {
				ntsArrayReverse[i].shutdown();
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
		
		int i = ntsArrayForward.length;
		boolean cleanExit = true;

		while (--i>=0) {			
			cleanExit &= ntsArrayForward[i].awaitTermination(timeout, unit);
			cleanExit &= ntsArrayReverse[i].awaitTermination(timeout, unit);
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
