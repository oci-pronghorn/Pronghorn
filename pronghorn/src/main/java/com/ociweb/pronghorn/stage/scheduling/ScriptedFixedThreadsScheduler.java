package com.ociweb.pronghorn.stage.scheduling;

import java.lang.reflect.Field;
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

import com.ociweb.pronghorn.network.ClientSocketReaderStage;
import com.ociweb.pronghorn.network.ClientSocketWriterStage;
import com.ociweb.pronghorn.network.OrderSupervisorStage;
import com.ociweb.pronghorn.network.ServerNewConnectionStage;
import com.ociweb.pronghorn.network.ServerSocketReaderStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStage;
import com.ociweb.pronghorn.network.http.HTTPLogUnificationStage;
import com.ociweb.pronghorn.network.mqtt.IdGenStage;
import com.ociweb.pronghorn.network.mqtt.MQTTClientResponseStage;
import com.ociweb.pronghorn.network.mqtt.MQTTClientStage;
import com.ociweb.pronghorn.network.mqtt.MQTTClientToServerEncodeStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorStage;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.primitive.IntArrayHolder;

public class ScriptedFixedThreadsScheduler extends StageScheduler {

	private int threadCount;

	private ExecutorService executorService;
	private volatile Throwable firstException;//will remain null if nothing is wrong
	static final Logger logger = LoggerFactory.getLogger(ScriptedFixedThreadsScheduler.class);
	private ScriptedNonThreadScheduler[] ntsArray;

	private StageVisitor longRunVisitor = new StageVisitor() {
		
		byte[] seen = new byte[GraphManager.countStages(graphManager)+1];
				
		@Override
		public void visit(PronghornStage stage) {

			///////////////////////
			//Code disabled 4/24
			//when it splits the part cut off is sometimes missing the last stage or stage after cut on 2?
			//TODO: do not enable feature until this bug is found
			//TODO: this is also a little slow on the thread where it runs..
			//////////////////////
			
//			
//			
//			if (seen[stage.stageId]==0) {
//				seen[stage.stageId]=1;	//will only report this once per run.
//
//				//linear search, will only happen once.
//				int i = ntsArray.length;
//				while (--i>=0) {
//					int idx = -1;
//					final ScriptedNonThreadScheduler localNTS = ntsArray[i];
//					if ((idx=localNTS.indexOfStage(stage))>=0) {
//						assert(idx<localNTS.stages.length);
//
//						//found it and we have work to do
//						//if already at beginning there is nothing else to do
//						if (localNTS.stages.length>1 
//							&& (idx>0) //not beginning or end
//							&& (idx<localNTS.stages.length-1)) {
//							logger.warn("New thread started; This stage has been detected to be blocking and/or long running: {}  Please review the code and break this work into multiple smaller units.", stage.toString());
//							
//						    //adding one more thread to executer service
//							ScriptedNonThreadScheduler splitOn = localNTS.splitOn(idx);
//							ScriptedNonThreadScheduler[] newArray = new ScriptedNonThreadScheduler[ntsArray.length+1];
//							System.arraycopy(ntsArray, 0, newArray, 0, ntsArray.length);
//							newArray[newArray.length-1] = splitOn;
//							ntsArray = newArray;
//							executorService.execute(buildRunnable(splitOn));
//						} else {
//							//logger.trace("stage was already scheduled for the optimum time "+stage);
//						}
//						
//						return;
//					}
//				}
//				throw new UnsupportedOperationException("Internal error, expected to find stage "+stage+" in one of the schedulers.");
//			}
			
			
			
		}
	};
	
	private final BloomFilter hangman = new BloomFilter(10000, .00001); //32K
		
	private void hangDetection(long nowNS) {
		//all threads will check up on the other threads,
		//this works providing we have more than 1 thread in play
		ScriptedNonThreadScheduler[] localArray = ntsArray;
		int c = localArray.length;
		while (--c>=0) {
			PronghornStage hungStage = localArray[c].hungStage(nowNS);
			if (null != hungStage) {
				synchronized(hangman) {
					String stageNname = hungStage.toString();					
					if (!hangman.mayContain(stageNname)) {
						
						//TODO: should report back to telemetry screen 
						long hangTime = localArray[c].hangTime(nowNS);
						//TODO: not sure why we need to filter here, code needs review
						if (hangTime>1_000_000_000) {
						
							logger.info("{} Hung stage {}", Appendables.appendNearestTimeUnit(new StringBuilder(), hangTime), hungStage);
							hangman.addValue(stageNname);
						}
					}
				}
			}
		}
		
	}
	
	//////////////////////////////////
	//methods for increasing and decreasing the thread count
	//these will be callable from the GreenRuntime and from Telemetry server?
	//////////////////////////////////
	
	//why to reduce threads?
	// * If we need to lower the latency at the expense of volume
	// * If the threads are far larger than cores causing time slicing	
	
	//why to increase threads?
	// * If we need greater volume at the expense of latency
	// * If threads are fewer in count than the number of cores paid for
	
	//TODO: must prevent rapid changes or changes "out of bounds"
	//TODO: must have method to return the count of threads
	//     show this as a scale low to high with two buttons for
	//     lower latency and increase volume
	
	
	
	public void reduceThreads() {
		long min1 = Integer.MAX_VALUE;
		int min1Idx = -1;
		long min2 = Integer.MAX_VALUE;
		int min2Idx = -1;
				
		int i = ntsArray.length;
		while(--i>=0) {
			long elap = ntsArray[i].nominalElapsedTime(graphManager);
			if (elap<min1) {				
				if (min1 < min2) {
					min2 = min1;
					min2Idx = min1Idx;
				}
				min1 = elap;
				min1Idx = i;						
			}
		}
		
		//TODO: buld new ntsArray from the sum of both arrays
		//TODO: lock both old arrays, remove them, insert new item
		//TODO: remove old arrays from executor and add new item
		
		
	}
	public void increaseThreads() {
		long max = -1;
		int maxIdx = -1;
		
		int i = ntsArray.length;
		while(--i>=0) {
			long elap = ntsArray[i].nominalElapsedTime(graphManager);
			if (elap>max) {
				max = elap;
				maxIdx = i;
			}
		}

		int idx = ntsArray[maxIdx].recommendedSplitPoint(graphManager);
		
		ntsArray[maxIdx].splitOn(idx);
		
		
		
		//TODO: find the slowest, find its slowest
		//      split on that point.
		
		
	}
	//////////////////////////////
	//////////////////////////////
	
	
	public ScriptedFixedThreadsScheduler(GraphManager graphManager) {
		//this is often very optimal since we have enough granularity to swap work but we do not
		//have so many threads that it overwhelms the operating system context switching
		this(graphManager, CoresUtil.availableProcessors()*2);
	}

	public ScriptedFixedThreadsScheduler(GraphManager graphManager, int targetThreadCount) {
		this(graphManager, targetThreadCount, true);
	}

	public ScriptedFixedThreadsScheduler(final GraphManager graphManager, int targetThreadCount, boolean enforceLimit) {
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

	public static PronghornStage[][] buildStageGroups(final GraphManager graphManager, int targetThreadCount, boolean enforceLimit) {
		//must add 1 for the tree of roots also adding 1 more to make hash more efficient.
	    final int countStages = GraphManager.countStages(graphManager);  
		int bits = 1 + (int)Math.ceil(Math.log(countStages)/Math.log(2));
		IntHashTable rootsTable = new IntHashTable(bits);
		
		IntArrayHolder lastKnownRoot = new IntArrayHolder(128);
	
		Comparator<PronghornStage[]> comp = new Comparator<PronghornStage[]>(){

			@Override
			public int compare(PronghornStage[] o1, PronghornStage[] o2) {
						
				//keep all the montiors on one end to merge them last		
				
				boolean mon1 = o1!=null && GraphManager.hasNota(graphManager, o1[0].stageId, GraphManager.MONITOR);
				boolean mon2 = o2!=null && GraphManager.hasNota(graphManager, o2[0].stageId, GraphManager.MONITOR);

				if (mon1 && mon2) {
					mon1 = false;
					mon2 = false;							
				}
				
				int len1 = null==o1 ? -1 : mon1 ? Integer.MAX_VALUE : o1.length;
				int len2 = null==o2 ? -1 : mon2 ? Integer.MAX_VALUE : o2.length;

				return (len2>len1)?1:(len2<len1)?-1:0;
			}
	    	
	    };
		
		PronghornStage[][] stageArrays = buildStageGroups(graphManager, targetThreadCount, 
				                                          enforceLimit, countStages, 
				                                          rootsTable, lastKnownRoot, comp);
		
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
			final IntHashTable rootsTable, IntArrayHolder lastKnownRoot, Comparator comp
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
	
	    boolean showRoots = false;
	    if (showRoots) {
	    		
			for(int stages=0; stages <= GraphManager.countStages(graphManager); stages++) { 
		    
		    	PronghornStage stage = GraphManager.getStage(graphManager, stages);
		    	if (null!=stage) {	    		
		    		int id = 		rootId(stage.stageId, rootsTable, lastKnownRoot);	    			
		    		
		    		System.err.println("root"+id+"  "+stage.getClass().getSimpleName()+" "+stage.stageId);
		    		
		    	}
		    	
		    }
		}
	    
	    
	    if (countStages>logLimit) {
	        	logger.info("finished hierarchical classifications");
	    }
	    PronghornStage[][] stageArrays = buildOrderedArraysOfStages(graphManager, rootCounter,
	    														rootsTable, lastKnownRoot);
    
	    if (enforceLimit) {
	    	enforceThreadLimit(graphManager, targetThreadCount, stageArrays, comp);
	    }
	    
	    
	    if (countStages>logLimit) {
	    	logger.info("finished scheduling");
	    }
		return stageArrays;
	}

	private static void enforceThreadLimit(GraphManager graphManager, int targetThreadCount, PronghornStage[][] stageArrays, Comparator comp) {
		////////////
	    assert(null!=comp);
	  
	    Arrays.sort(stageArrays, comp );
	    
	    int countOfGroups = 0;
	    int x = stageArrays.length;
	    while (--x>=0) {
	    	if (null!=stageArrays[x] && stageArrays[x].length>0) {
	    		countOfGroups++;
	    	}
	    }
	 
//    	
//    	for(int i = 0; i<stageArrays.length; i++) {
//    		PronghornStage[] array = stageArrays[i];
//    		if (null!=array) {
//    			System.err.println("00000000000000");
//    			for(int z = 0; z<array.length; z++) {
//    				
//		        	StringBuilder target = new StringBuilder();
//		    		target.append("full stages "+array[z].getClass().getSimpleName());
//		    		target.append("  inputs:");
//		    		GraphManager.appendInputs(graphManager, target, array[z]);
//		    		target.append(" outputs:");
//		    		GraphManager.appendOutputs(graphManager, target, array[z]);
//		    		System.err.println(target);        		
//    			}
//    		}
//    	}
//	    
	    //we have too many threads so start building some combos
	    //this is done from the middle because the small 1's must be isolated and the
	    //large ones already have too much work to do.
	    int threadCountdown = (countOfGroups-targetThreadCount);
	    final boolean debug = false;
	    while (--threadCountdown>=0) {
	    	
	    	int idx = countOfGroups/2;
	    	
	    	if (null == stageArrays[idx]) {
	    		break;
	    	}
	    	
	    	if (    GraphManager.hasNota(graphManager, stageArrays[idx][0].stageId, GraphManager.MONITOR)
	    		||	GraphManager.hasNota(graphManager, stageArrays[idx-1][0].stageId, GraphManager.MONITOR)) {
	    		//can not combine any further than this. TODO: or can we???
	    		//the monitor can not be mixed in.
	    		break;
	    	}
	    	
	    	if (    GraphManager.hasNota(graphManager, stageArrays[idx][0].stageId, GraphManager.ISOLATE)
		    	||	GraphManager.hasNota(graphManager, stageArrays[idx-1][0].stageId, GraphManager.ISOLATE)) {
		    		//can not combine any further than this. TODO: or can we???
		    		break;
		    }
	    	
	    	
	    	assert(null!=stageArrays[idx]);
	    	assert(null!=stageArrays[idx-1]);
	    	

	    	PronghornStage[] newArray = new PronghornStage[stageArrays[idx].length
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

		int i = pipes.length;
	    while (--i>=0 /*&& totalThreads>targetThreadCount*/) {	    //can stop early but this may not be optimal??	
	    	int ringId = pipes[i].id;
	    	
	    	int consumerId = GraphManager.getRingConsumerId(graphManager, ringId);
	    	int producerId = GraphManager.getRingProducerId(graphManager, ringId);
	    	//only join stages if they both exist
	    	if (consumerId>0 && producerId>0) {	  

	    		if (isValidToCombine(ringId, consumerId, producerId, graphManager, targetThreadCount)) {
	    		
		    		//determine if they are already merged in the same tree?
		    		int consRoot = rootId(consumerId, rootsTable, lastKnownRoot);
		    		int prodRoot = rootId(producerId, rootsTable, lastKnownRoot);
		    		
		    		if (consRoot != prodRoot) {	
		    		
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
	private static boolean isValidToCombine(int ringId, int consumerId, int producerId, GraphManager graphManager, int targetThreadCount) {


		
		if (targetThreadCount>=3) {
			//these stages must be isolated from their neighbors.
			//  1. they may be a hub and a bottleneck for traffic
			//  2. they may be blocking calls
			if (GraphManager.hasNota(graphManager, producerId, GraphManager.ROUTER_HUB)) {
				return false;
			}
			if (GraphManager.hasNota(graphManager, consumerId, GraphManager.ROUTER_HUB)) {
				//if this a small single use allow for join
				if (GraphManager.getInputPipeCount(graphManager, consumerId)>2) {
					return false;
				}
			}
		}

		
		//these stages must always be isolated
		if (GraphManager.hasNota(graphManager, producerId, GraphManager.ISOLATE)) {
			return false;
		}
		if (GraphManager.hasNota(graphManager, consumerId, GraphManager.ISOLATE)) {
			return false;
		}

		
		Pipe p = GraphManager.getPipe(graphManager, ringId);
		if (Pipe.schemaName(p).contains("Ack") || Pipe.schemaName(p).contains("Release") ) {
			return false;//never use an Ack connection as the primary data flow.
		}
		
		PronghornStage consumerStage = GraphManager.getStage(graphManager, consumerId);
		PronghornStage producerStage = GraphManager.getStage(graphManager, producerId);

		
		//TODO: for single thread we may need to short circut the log and return true.
		
		/////////////////
		//TODO: add a new scheduler which has 2 scripts
		//      one for high volume and another for low latency
		//      that scheduler can swap on the fly while data is in flight.
		////////////////
		boolean useMoreThreads = false;
		
	
		if (isMergeOfComputeLoad(consumerId, producerId, graphManager, p, useMoreThreads)) {
			return false;
		}

//		//eliminate server call input backups
//		if ((producerStage instanceof ServerSocketReaderStage)
//			&& (GraphManager.getOutputPipeCount(graphManager, producerId)>4)
//				) {
//			return false;
//		}

		//eliminate network call input response backups
		if ((producerStage instanceof ClientSocketReaderStage) 
			&& (GraphManager.getOutputPipeCount(graphManager, producerId)>4)			
				) {
			return false;
		}
		
		if ((producerStage instanceof ClientSocketWriterStage) 
				&& (GraphManager.getInputPipeCount(graphManager, producerId)>4)			
					) {
				return false;
			}

	// GraphManager.LOAD_MERGE split is very questionable, it should be removed..	
		
		//Caution if server socket reader spins to fast it can block all windows networking.
		if (!(producerStage instanceof ServerSocketReaderStage)) {
			//stops connecting via HTTP1xResponseParser and ServerSocketReader
			if (GraphManager.hasNota(graphManager, producerId, GraphManager.LOAD_BALANCER) 
					&& (GraphManager.getOutputPipeCount(graphManager, producerId)>2)			
						) {
							
				//TODO: and must be going to different places
						
					return false;
					
					
			}		
		}
		
		
		if (consumerStage instanceof MQTTClientStage) {		
		   if  ((producerStage instanceof MQTTClientResponseStage) ||
			    (producerStage instanceof IdGenStage))
		   {
			   //do not block, this is part of the internal implementation	   
			   return true;
		   } else {
			   return false; 
			//block all others
		   }
		}
		
		if (producerStage instanceof MQTTClientStage) {		
			   if  ((consumerStage instanceof MQTTClientToServerEncodeStage) ||
				    (consumerStage instanceof IdGenStage))
			   {
				 //do not block, this is part of the internal implementation
				   return true;
			   } else {
				   return false; 
				//block all others
			   }
			}
		
		
		if (consumerStage instanceof HTTPLogUnificationStage) {
			return false;//never combine log info producer with the log unification stage
		}
		
		if (consumerStage instanceof PipeMonitorCollectorStage ) {
			return false;
		}
		if (producerStage instanceof PipeMonitorCollectorStage ) {
			return false;
		}		
		
		if (consumerStage instanceof PipeMonitorStage ) {
			return false;
		}
		if (producerStage instanceof PipeMonitorStage ) {
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
		if (isSplittingComputeLoad(consumerId, producerId, graphManager, p, useMoreThreads)) {
			return false;
		}
		

		//TODO: for a matrix of two rows of behaviors only let prod take on n consumers
		//      where n is the count of producers per consumers over the consumers...
		
//		//this rule appears to always be wrong..
//		if (useMoreThreads) {
//			int totalInputsCount = GraphManager.getInputPipeCount(graphManager, consumerId);
//			if (totalInputsCount>1) {
//				//do not combine with super if it has multiple inputs
//				if (consumerStage instanceof OrderSupervisorStage) {				
//					int pipeId = GraphManager.getInputPipeId(graphManager, consumerId, 1);	
//					//do combine if its the first pipe
//					if (producerId != GraphManager.getRingProducerId(graphManager, pipeId)) {
//						return false;
//					}				
//				}
//			}
//		}
		
		//this server pipe is only used for 404 based on route failures
		//since this is not a priority it should not be an optimized relationship
		if ((producerStage instanceof HTTP1xRouterStage)
		&& (consumerStage instanceof OrderSupervisorStage)
			) {
			return false;
		}
			
		
		//if this consumer comes from a different produer up stream do not combine
		//if the size of a group is above average do not combine.
		

//No longer needed....
        //NOTE: this also helps with the parallel work above, any large merge or split will show up here.
		//do not combine with stages which are 2 standard deviations above the mean input/output count
//		if (GraphManager.countStages(graphManager)>80) {//only apply if we have large enough sample.
//		    RunningStdDev pipesPerStage = GraphManager.stdDevPipesPerStage(graphManager);
//		    int thresholdToNeverCombine = (int) (RunningStdDev.mean(pipesPerStage)+ (2*RunningStdDev.stdDeviation(pipesPerStage)));
//		    if ((GraphManager.getInputPipeCount(graphManager,consumerId)
//		         +GraphManager.getOutputPipeCount(graphManager, consumerId)) > thresholdToNeverCombine) {
//		    	return false;
//		    }
//		    if ((GraphManager.getInputPipeCount(graphManager,producerId)
//			    +GraphManager.getOutputPipeCount(graphManager, producerId)) > thresholdToNeverCombine) {
//		    	return false;
//	        }    
//		}
		
		return true;
	}

	private static boolean isSplittingComputeLoad(int consumerId, 
			                                    int producerId, 
			                                    GraphManager graphManager, 
			                                    Pipe p, boolean useMoreThreads) {
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
				if (!useMoreThreads) {
					//determine if they are consumed by the same place or not
					int conId = GraphManager.getRingConsumerId(graphManager, outputPipe.id);
					if ((totalOutputsCount-1)==proOutCount
							&& conId == consumerId) {
						return false; //allow this first one.
					}
				}
				
				countOfHeavyComputeConsumers++;
				
			}
		}
		//every output pipe has the same schema and leads to heavy compute work
        return countOfHeavyComputeConsumers == totalOutputsCount;
	}

	
	
	private static boolean isMergeOfComputeLoad(int consumerId, 
			              int producerId, GraphManager graphManager, 
			              Pipe p, boolean useMoreThreads) {
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
				
				if (!useMoreThreads) {
					//determine if they are consumed by the same place or not
					int prodId = GraphManager.getRingProducerId(graphManager, inputPipe.id);
					if ( (totalInputsCount-1) == conInCount
							&& 
							prodId == producerId
							) {
					
						//NOTE: this is very dependent on order of pipes...
						return false;
					}
				}
				
				countOfHeavyComputeProducers++;

			}
		}

		//if all the inputs are of the same type then this is join
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
	    	    
		int addsCount = 0;
		int expectedCount = 0;
		int stages;
		//////////////
		//walk over all stages and build the actual arrays
		/////////////
		{
			stages = GraphManager.countStages(graphManager);
		    //Add all the real producers first...
		    while (stages >= 0) {	    		 
		    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
		    	if (null!=stage) {	    	    
		    		expectedCount++;
		    		//Definition of "producers" is somewhat complex
		    		//we do zero inputs first...
		    		if (
		    				(0==GraphManager.getInputPipeCount(graphManager, stage.stageId))
		    			  
		    				) {
		    			
		    			//get the root for this table
						int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
						lazyCreateOfArray(rootMemberCounter, stageArrays, root);
						addsCount = add(stageArrays, stage, 
												root, graphManager, rootsTable, 
												lastKnownRoot, addsCount, 
												rootMemberCounter, false, true);
		    			
		    		}
					
		    	}
		    }
		}
		
		{
			stages = GraphManager.countStages(graphManager);
		    //Add all the real producers first...
		    while (stages >= 0) {	    		 
		    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
		    	if (null!=stage) {	    	    
		    		
		    		//Definition of "producers" is somewhat complex
		    		//we do those marked as producers second
		    		if (
		    			(null!=GraphManager.getNota(graphManager, stage.stageId, GraphManager.PRODUCER, null))
		    		
		    				) {
		    			
		    			//get the root for this table
						int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
						lazyCreateOfArray(rootMemberCounter, stageArrays, root);
						addsCount = add(stageArrays, stage, 
												root, graphManager, rootsTable, 
												lastKnownRoot, addsCount, 
												rootMemberCounter, false, true);
		    			
		    		}
					
		    	}
		    }
		}
		
	    //Add all the local producers (those with no inputs to their own shared root)...
		{
			stages = GraphManager.countStages(graphManager);
		    //Add all the real producers first...
		    while (stages >= 0) {	    		 
		    	PronghornStage stage = GraphManager.getStage(graphManager, stages--);
		    	if (null!=stage) {	    	    
		    		
		    		//Definition of "producers" is somewhat complex
		    		//we do local producers third
		    		if (
		    				hasNoInternalInputs(graphManager, stage, rootsTable, lastKnownRoot)	
		    			) {
		    			
		    			//get the root for this table
						int root = rootId(stage.stageId, rootsTable, lastKnownRoot);	    			    		
						lazyCreateOfArray(rootMemberCounter, stageArrays, root);
						addsCount = add(stageArrays, stage, 
												root, graphManager, rootsTable, 
												lastKnownRoot, addsCount, 
												rootMemberCounter, false, true);
		    			
		    		}
					
		    	}
		    }
		}
	    
	    
	    //collect a list of down stream stages to do next right after the producers.
	    
	    
	    if (expectedCount != addsCount) {
		    
	    	logger.trace("We have {} stages which do not consume from any known producers, To ensure efficient scheduling please mark one as the producer.", expectedCount-addsCount);
	    			
	    	
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
									lastKnownRoot, addsCount, rootMemberCounter, true, false);
					
		    	}
		    }
	    }

	    boolean debug = (expectedCount!=addsCount);
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
        
	    
	    assert(expectedCount==addsCount) : "Some stages left unscheduled, internal error. "+expectedCount+" vs "+addsCount;
	    
	    
		return stageArrays;
	}

	//are there any inputs which come from the same root as this stage, if so this can not be the head.
	private static boolean hasNoInternalInputs( GraphManager graphManager, 
												PronghornStage stage, 
												IntHashTable rootsTable,
												IntArrayHolder lastKnownRoot) {
		
		boolean result = true;
		final int root = rootId(stage.stageId, rootsTable, lastKnownRoot);
		
		int inC = GraphManager.getInputPipeCount(graphManager, stage.stageId);
		
		for(int i=1; i<=inC; i++) {
			//if we find an input in the same root then we do have inputs
			int prodId = GraphManager.getRingProducerId(graphManager, GraphManager.getInputPipe(graphManager, stage.stageId, i).id);
			
			if (root ==	rootId(prodId, rootsTable, lastKnownRoot)) {
				result = false;
			}
		}
		return result;
	}


	private static void lazyCreateOfArray(int[] rootMemberCounter, PronghornStage[][] stageArrays, int root) {
		if (root>=0 && rootMemberCounter[root]>0) {
			//create array if not created since count is > 0
			if (null == stageArrays[root]) {
				//System.out.println("count of group "+rootMemberCounter[root]);
				stageArrays[root] = new PronghornStage[rootMemberCounter[root]];
			}					
		}
	}

	//Is this stage in a loop where the pipes pass back to its self.
	
	private void createSchedulers(GraphManager graphManager, PronghornStage[][] stageArrays) {
	
		int idxThreadCheckingForLongRuns = 0;
		int j = stageArrays.length; //TODO: refactor into a recursive single pass count.
		int count = 0;
		while (--j>=0) {
			if (null!=stageArrays[j]) {
				assert(stageArrays[j].length>0);
				//if we find a monitor use it for long run checks instead of primary				
				if (GraphManager.hasNota(graphManager, stageArrays[j][0].stageId, GraphManager.MONITOR)) {
					idxThreadCheckingForLongRuns=j;
				}
				count++;
				
			}
			
			
		}
		threadCount=count;
		logger.info("actual thread count {}", threadCount);
		
		//logger.trace("thread checking for long runs is {}",idxThreadCheckingForLongRuns);
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

	    		//boolean isMonitor = GraphManager.hasNota(graphManager, stageArrays[k][0].stageId, GraphManager.MONITOR);
	    	//	System.out.println("order: "+reverseOrder+" isMonitor "+isMonitor);

				//TODO: NOTE: when false this is optimized for low load conditions, eg the next pipe has room.
				//      This boolean can be set to true IF we know the pipe will be full all the time
				//      By setting this to true the scheduler is optimized for heavy loads
				//      Each individual part of the graph can have its own custom setting... 
				boolean reverseOrder = false;

				//System.out.println("only seems to help if we can toggle this fast; reverse:"+reverseOrder);
				
	    		StageVisitor checkForLongRuns = (idxThreadCheckingForLongRuns!=k) ? null : longRunVisitor;
	    		ntsArray[ntsIdx++] = new ScriptedNonThreadScheduler( 
	    				             graphManager, reverseOrder, 
	    				             checkForLongRuns, stageArrays[k]);
	    	}
	    }
	}

	private static int[] tempRanks = new int[1024];
	
	private static int add(PronghornStage[][] stageArrays, PronghornStage stage,
			              final int root, final GraphManager graphManager, 
			              final IntHashTable rootsTable, IntArrayHolder lastKnownRoot,
			              int count, int[] rootMemberCounter, boolean log, boolean doNotOrder) {

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
		
//		if (log) {
//			logger.info("added stage {}",stage);
//		}
		
		count++;
		
		//Recursively add the ones under the same root.
		
		int outputCount = GraphManager.getOutputPipeCount(graphManager, stage.stageId);
		
		boolean simpleAddInOrder = GraphManager.hasNota(graphManager, stage.stageId, GraphManager.LOAD_BALANCER);
		
		if (simpleAddInOrder) {

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
				
				int consumerId = GraphManager.getRingConsumerId(
						graphManager, 
						GraphManager.getOutputPipe(
								graphManager, 
								stage, 
								r).id);

				if (doNotOrder) {
				
					if (GraphManager.hasNota(graphManager, consumerId, GraphManager.TRIGGER)) {
						tempRanks[r] =  -1;
					} else {
						tempRanks[r] = 1;
					}
					
				} else {
					
					tempRanks[r] = getNearnessRank(
							consumerId, graphManager, pronghornStages);
					
				}
				
				
				
			}
						
			int targetRank = -1;
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
			} while (targetRank<=i 
					&& pronghornStages[pronghornStages.length-1]==null);
			
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
				int tempRootId = rootId(inProd, rootsTable, lastKnownRoot);
				lazyCreateOfArray(rootMemberCounter, stageArrays, tempRootId);
				PronghornStage[] prodsList = stageArrays[tempRootId];
				
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
					     rootMemberCounter, log, false);
		} else {
			lazyCreateOfArray(rootMemberCounter, stageArrays, localRootId);
			if (localRootId>=0 && null==stageArrays[localRootId][0]) {
				//only add for this one case
				//at this point we have changed nodes since this is an entry point.
				count = add(stageArrays, GraphManager.getStage(graphManager, consumerId),
						localRootId, graphManager, rootsTable, lastKnownRoot,
						count, rootMemberCounter, log, true);
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
     * 
     * "TrafficCops" and other "triggers" however are always the highest priority and are given a rank of -1 by default.
     */
	private static int getNearnessRank(int consumerId,
			                           GraphManager graphManager, 
			                           PronghornStage[] pronghornStages) {

		if (GraphManager.hasNota(graphManager, consumerId, GraphManager.TRIGGER)) {
			return -1;
		}
		
		int maxSteps = 0;
		final int stepsLimit = 200;//do not look past this point its not helpful
		int inC = GraphManager.getInputPipeCount(graphManager, consumerId);
		for(int j = 1; j<= inC; j++) {
			
			int prodId = GraphManager.getRingProducerId(graphManager, GraphManager.getInputPipeId(graphManager, consumerId, j));
			
			int x = pronghornStages.length;
			int steps = 0;
			while (--x>=0) {
				PronghornStage pronghornStage = pronghornStages[x];
				if (null != pronghornStage) {
					if (prodId == pronghornStage.stageId || steps>=stepsLimit) {
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
		
		//skip any non stages, this happens in unit tests.
		if (id<0) {
			return -1;
		}
		
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
			@Override
			public Thread newThread(Runnable r) {
				Thread result = null;
				int prioirity = Thread.NORM_PRIORITY;
				Field[] fields = r.getClass().getDeclaredFields();
				int f = fields.length;
				while (--f>=0) {
					if (fields[f].getType().isAssignableFrom(NamedRunnable.class)) {
						fields[f].setAccessible(true);
						
						try {
							if (fields[f].get(r)==null || null==((NamedRunnable)fields[f].get(r)).name()) {
								result = new Thread(r,"Unknown");	
							} else {
								//logger.info("Creating new thread named {}",((NamedRunnable)fields[f].get(r)).name());
								NamedRunnable namedRunnable = (NamedRunnable)fields[f].get(r);
								result = new Thread(r, namedRunnable.name());
								namedRunnable.setThreadId(result.getId());
								
								//long names are more important and get a higher priority
								//may want to count commas instead..
								if (namedRunnable.name().length()>40) {
									logger.trace("priority thread {}",namedRunnable.name());
									prioirity = Thread.MAX_PRIORITY;
								}
							}
						} catch (IllegalArgumentException e) {
							logger.info("error pulling NamedRunnable",e);
							result = new Thread(r,"Unknown");
						} catch (IllegalAccessException e) {
							logger.info("error pulling NamedRunnable",e);
							result = new Thread(r,"Unknown");		
						} finally {
							fields[f].setAccessible(false);
						}
					}
					
				} 
				
				if (null==result) {
					result = new Thread(r,"Unknown");				
				}			
				
				result.setPriority(prioirity);
								
				//logger.info("new thread created for {}",r.getClass().getName());
				return result;
			}        	
        };
        
        this.executorService = Executors.newCachedThreadPool(threadFactory);
    

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
		logger.trace("all stages started up");
		
		//once code is complete we can scan for all the NIds which are needed for core affinity.
		//PinningUtil.visitStacks();
		
	}

	
	private Runnable buildRunnable(final CyclicBarrier allStagesLatch, final ScriptedNonThreadScheduler nts) {
		assert(null!=allStagesLatch);
		assert(null!=nts);
	
		return new NamedRunnable() {

			@Override
			public void run() {
				
				nts.startup();
				
				try {
			            allStagesLatch.await();
			        } catch (InterruptedException e) {
			        } catch (BrokenBarrierException e) {
			        }
				
	
				try {
					long c = 0;
					while (!ScriptedNonThreadScheduler.isShutdownRequested(nts)) {
						ScriptedNonThreadScheduler.playScript(nts);
						if ((++c&0xFFFF)==0) {
							hangDetection(System.nanoTime());
						}
					}		
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();	
					nts.shutdown();
					return;
				}

			}

			@Override
			public String name() {
				return nts.name();
			}

			@Override
			public void setThreadId(long id) {
				nts.setThreadId(id);
			}	
			
		};
	}

	private Runnable buildRunnable(final ScriptedNonThreadScheduler nts) {
		assert(null!=nts);
	
		return new NamedRunnable() {

			@Override
			public void run() {
					while (!ScriptedNonThreadScheduler.isShutdownRequested(nts)) {
						nts.run();										
					}		
			}

			@Override
			public String name() {
				return nts.name();
			}

			@Override
			public void setThreadId(long id) {
				nts.setThreadId(id);
			}	
			
		};
	}
	
	@Override
	public boolean checkForException() {
		int i = ntsArray.length;
		while (--i>=0) {
			ntsArray[i].checkForException();
		}
		return true;
	}
	
	@Override
	public void shutdown() {

		int i = ntsArray.length;
		while (--i>=0) {
			if (null != ntsArray[i]) {
				if (!ScriptedNonThreadScheduler.isShutdownRequested(ntsArray[i])) {
					ntsArray[i].shutdown();	
				}
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
		//will be null upon empty project, this is ok, just exit.
		if (null!=executorService) {
			//each child scheduler has already completed await termination so no need to wait for this 
			executorService.shutdownNow();
			executorService = null;
			ntsArray = null;		
		}
	
		if (null!=firstException) {
		    throw new RuntimeException(firstException);
		}
		if (!cleanExit) {
			validShutdownState();
			return false;
		}
		return true;
		
	}

	@Override
	public boolean terminateNow() {

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
