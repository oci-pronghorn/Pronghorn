package com.ociweb.pronghorn.stage.scheduling;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.RingBufferMonitorStage;
import com.ociweb.pronghorn.util.Appendables;

public class GraphManager {
	
	private static final String CHECK_GRAPH_CONSTRUCTION = "Check graph construction";
	
	private final static int INIT_RINGS = 32;
	private final static int INIT_STAGES = 32;
	
	private final static Logger logger = LoggerFactory.getLogger(GraphManager.class);

    private class GraphManagerStageStateData {
    	
		private Object lock = new Object();	
		private byte[] stageStateArray = new byte[INIT_STAGES];
		
		public final static byte STAGE_NEW = 0;
		public final static byte STAGE_STARTED = 1;
		public final static byte STAGE_STOPPING = 2;
		public final static byte STAGE_TERMINATED = 3;
		
	}
	
    //Nota bene attachments
	public final static String SCHEDULE_RATE = "SCHEDULE_RATE"; //in ns - this is the delay between calls regardless of how long call takes
	                                                        //If dependable/regular clock is required run should not return and do it internally.
	public final static String MONITOR         = "MONITOR"; //this stage is not part of business logic but part of internal monitoring.
	public final static String PRODUCER        = "PRODUCER";//explicit so it can be found even if it has feedback inputs.
	public final static String STAGE_NAME      = "STAGE_NAME";
	public final static String DOT_RANK_NAME   = "DOT_RANK_NAME";	

	public final static String UNSCHEDULED   = "UNSCHEDULED";//new nota for stages that should never get a thread (experimental)
	public final static String THREAD_GROUP  = "THREAD_GROUP";   //new nota for stages that do not give threads back (experimental)
	
	
	private final static Logger log = LoggerFactory.getLogger(GraphManager.class);
				
	//Used for assigning stageId and for keeping count of all known stages
	private final AtomicInteger stageCounter = new AtomicInteger();
	
	//for lookup of source id and target id from the ring id
	private int[] ringIdToStages  = new int[INIT_RINGS*2]; //stores the sourceId and targetId for every ring id.
	
	//for lookup of input ring start pos from the stage id 
	private int[] stageIdToInputsBeginIdx  = new int[INIT_STAGES];
	private int[] multInputIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topInput = 0;
	
	//for lookup of output ring start pos from the stage id
	private int[] stageIdToOutputsBeginIdx = new int[INIT_STAGES];
	private int[] multOutputIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topOutput = 0;
	
	//for lookup of RingBuffer from RingBuffer id
	private Pipe[] pipeIdToPipe = new Pipe[INIT_RINGS];
	
	private static final Pipe[] EMPTY_PIPE_ARRAY = new Pipe[0];
	
	//for lookup of Stage from Stage id
	private PronghornStage[]  stageIdToStage = new PronghornStage[INIT_STAGES];
	private long[] stageStartTimeNs = new long[INIT_STAGES];
	private long[] stageShutdownTimeNs = new long[INIT_STAGES];
	private long[] stageRunNS = new long[INIT_STAGES]; 
	
	
	
	//This object is shared with all clones
	private final GraphManagerStageStateData stageStateData;
	
	//add the nota to this list first so we have an Id associated with it
	private Object[] notaIdToKey = new Object[INIT_STAGES];
	private Object[] notaIdToValue = new Object[INIT_STAGES];
	private int[] notaIdToStageId = new int[INIT_STAGES];
	private int totalNotaCount = 0;

	//store the notas ids here
	private int[] stageIdToNotasBeginIdx = new int[INIT_STAGES];
	private int[] multNotaIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topNota = 0;
	
	//this is never used as a runtime but only as a construction lock
	private Object lock = new Object();	
	
	private boolean enableMutation = true;
	
	public GraphManager() {
		Arrays.fill(ringIdToStages, -1);
		Arrays.fill(stageIdToInputsBeginIdx, -1);
		Arrays.fill(multInputIds, -1);
		Arrays.fill(stageIdToOutputsBeginIdx, -1);
		Arrays.fill(multOutputIds, -1);
		Arrays.fill(notaIdToStageId, -1);
		Arrays.fill(stageIdToNotasBeginIdx, -1);
		Arrays.fill(multNotaIds, -1);
		
		stageStateData = new GraphManagerStageStateData();
	}
	
	private GraphManager(GraphManagerStageStateData parentStageStateData) {
		Arrays.fill(ringIdToStages, -1);
		Arrays.fill(stageIdToInputsBeginIdx, -1);
		Arrays.fill(multInputIds, -1);
		Arrays.fill(stageIdToOutputsBeginIdx, -1);
		Arrays.fill(multOutputIds, -1);
		Arrays.fill(notaIdToStageId, -1);
		Arrays.fill(stageIdToNotasBeginIdx, -1);
		Arrays.fill(multNotaIds, -1);
		
		//enables single point of truth for the stages states, all clones  share this object
		stageStateData = parentStageStateData;
	}
	
	

	public static GraphManager cloneAll(GraphManager m) {
		GraphManager clone = new GraphManager(m.stageStateData);
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				copyStage(m, clone, stage);
				copyNotasForStage(m, clone, stage);	
			}
		}
		clone.stageCounter.set(m.stageCounter.get());
		return clone;
	}
	
	public static GraphManager cloneStagesWithNotaKey(GraphManager m, Object key) {
		GraphManager clone = new GraphManager();
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (m != getNota(m, stage, key, m)) {
					copyStage(m, clone, stage);
					copyNotasForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	public static int newStageId(GraphManager gm) {
	    return gm.stageCounter.incrementAndGet();//no stage id can be 0 or smaller
	}
	
   @Deprecated	
   static PronghornStage[] getStages(GraphManager m) {
        return allStages(m);
    }
   
   @Deprecated    
   static int[] getInputRingIdsForStage(GraphManager gm, int stageId) {

       int[] ringIds = new int[INIT_RINGS];
       int index = gm.stageIdToInputsBeginIdx[stageId];

       while(-1 != gm.multInputIds[index]) {
           ringIds[index] = gm.multInputIds[index++];
       }

       for(; index < INIT_RINGS; ++index) {
           ringIds[index] = -1;
       }

       return ringIds;
   }
   
   public static int countStages(GraphManager gm) {       
       return gm.stageCounter.get();
   }
	
   public static int countStagesWithNotaKey(GraphManager m, Object key) {
        
        int count = 0;
        int i = m.stageIdToStage.length;
        while (--i>=0) {
            PronghornStage stage = m.stageIdToStage[i];
            if (null!=stage) {
                //count this stage if it has the required key
                if (null != getNota(m, stage, key, null)) {
                    count++;
                }
            }
        }
        return count;
    }
	
    public static PronghornStage getStageWithNotaKey(GraphManager m, Object key, int ordinal) {
       
       int i = m.stageIdToStage.length;
       while (--i>=0) {
           PronghornStage stage = m.stageIdToStage[i];
           if (null!=stage) {
               //count this stage if it has the required key
               if (null != getNota(m, stage, key, null)) {
                   if (--ordinal<=0) {
                       return stage;
                   }
               }
           }
       }
       throw new UnsupportedOperationException("Invalid configuration. Unable to find requested ordinal "+ordinal);
    }
	
	public static GraphManager cloneStagesWithNotaKeyValue(GraphManager m, Object key, Object value) {
		GraphManager clone = new GraphManager();
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (value.equals(getNota(m, stage, key, null))) {
					copyStage(m, clone, stage);
					copyNotasForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	
	public static boolean validShutdown(GraphManager m) {
				
		boolean result = true;

		
		PronghornStage[] neverStarted = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_NEW);
		int j = neverStarted.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.log.error("{} never completed startup",neverStarted[j]);
			}
		}		
		
		PronghornStage[] neverShutdown = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_STARTED);
		j = neverShutdown.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.log.error("{} never started shutdown",neverShutdown[j]);
			}
		}
		
		PronghornStage[] neverTerminated = GraphManager.allStagesByState(m, GraphManagerStageStateData.STAGE_STOPPING);
		j = neverTerminated.length;
		if  (j>0) {
			result = false;
			while (--j>=0) {
				StageScheduler.log.error("{} never finished shutdown",neverTerminated[j]);
			}
		}		
		
		int i = -1;
		while (++i<m.stageIdToStage.length) {
			if (null!=m.stageIdToStage[i]) {				
				if (!isStageTerminated(m, i) ) { 	
				    PronghornStage stage = getStage(m,i);
				    //if all the inputs are empty this is not where the stall will be found
				    if (!isInputsEmpty(m,stage)) {
				    
    					StageScheduler.log.error("-------------------");//divide the log for better clarity
    					logInputs(StageScheduler.log, m, stage);
    					StageScheduler.log.error("  Expected stage {} to be stopped but it appears to be running. terminated:{}", stage, isStageTerminated(m, i));
    					logOutputs(StageScheduler.log, m, stage);
    					StageScheduler.log.error("-------------------");//divide the log for better clarity
    					
    					result = false;
    					
    					reportUnexpectedThreadStacks(m);
					
			        }
				}				
			}
		}		
		if (!result) {
			StageScheduler.log.error("unclean shutdown");				
		}
		return result;
	}

    private static void reportUnexpectedThreadStacks(GraphManager m) {
        Map<Thread, StackTraceElement[]> allTraces = Thread.getAllStackTraces();
        for(Entry<Thread, StackTraceElement[]> item: allTraces.entrySet()) {
            
            StackTraceElement[] ste = item.getValue();
            boolean ignore = false;
            int j = ste.length;
            while (--j>=0) {
                //These are common to all of Java and normal ways to block a thread
                ignore |= ste[j].toString().contains("getAllStackTraces");					        
                ignore |= ste[j].toString().contains("java.lang.Object.wait");
                ignore |= ste[j].toString().contains("java.util.concurrent.locks.LockSupport.parkNanos");	
                ignore |= ste[j].toString().contains("junit.runner");
                
                //This is common from hazelcast
                ignore |= ste[j].toString().contains("com.hazelcast");  
               
                
            }
            
            j = ste.length;
            if (j>0) {
                //These are very frequent at the bottom of Pronghorn stack.
        	    ignore |= ste[0].toString().contains("ThreadPerStageScheduler.continueRunning");
        	    ignore |= ste[0].toString().contains("com.ociweb.pronghorn.stage.scheduling.GraphManager");
        	    if (!ignore) {
        	            			
        	        String threadName = item.getKey().getName();
        	        
        	        int idIdx = threadName.indexOf("id:");
        	        if (idIdx>=0) {
        	            int stageId = Integer.valueOf(threadName.substring(idIdx+3));
        	            if (isInputsEmpty(m, stageId)) {
        	                continue;//do next, this one has nothing blocking.
        	            }
        	        }
        	    
        	    
        		    System.err.println("");
        		    System.err.println(item.getKey().getName());
        		    while (--j>=0) {
        		        System.err.println("   "+ste[j]);
        		    }
        	    }
            }					    
        }
    }

	private static void copyNotasForStage(GraphManager m,	GraphManager clone, PronghornStage stage) {
		int idx;
		int notaId;
		int stageId = stage.stageId;
		
		idx = m.stageIdToNotasBeginIdx[stageId];
		while (-1 != (notaId=m.multNotaIds[idx++])) {
			Object key = m.notaIdToKey[notaId];
			Object value = m.notaIdToValue[notaId];					
			addNota(clone, key, value, stage);									
		}
	}

	private static void copyStage(GraphManager m, GraphManager clone, PronghornStage stage) {
		int stageId = beginStageRegister(clone, stage);
		
		int idx;
		int ringId;
		
		idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			assert(0==Pipe.contentRemaining(m.pipeIdToPipe[ringId]));
			regInput(clone, m.pipeIdToPipe[ringId], stageId);					
		}				
		
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			assert(0==Pipe.contentRemaining(m.pipeIdToPipe[ringId]));
			regOutput(clone, m.pipeIdToPipe[ringId], stageId);					
		}		
		
		endStageRegister(clone);
	}

	
	//NOTE: may extend this to use regular expressions against keys or values
	
	public static void disableMutation(GraphManager m) {
		m.enableMutation = false;
	}
	
	
	//Should only be called by methods that are protected by the lock
	private static int[] setValue(int[] target, int idx, int value) {		
		int[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, -1);
		}
		assert(-1==result[idx]) : "duplicate assignment detected, see stack and double check all the stages added to the graph.";
		
		result[idx] = value;
		return result;
	}
		
	private static <T> T[] setValue(T[] target, int idx, T value) {		
		T[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return (T[])result;
	}	
	
	private static byte[] setValue(byte[] target, int idx, final byte value) {
		byte[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}
		
		result[idx] = value;
		return result;
	}
	
	private static long[] setValue(long[] target, int idx, final long value) {
		long[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}		
		result[idx] = value;
		return result;
	}
	
	private static long[] incValue(long[] target, int idx, final long value) {
		long[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}		
		result[idx] += value;
		return result;
	}
	
	public static void register(GraphManager gm, PronghornStage stage, Pipe[] inputs, Pipe[] outputs) {
		
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			int i=0;
			int limit = inputs.length;
			while (i<limit) {
				regInput(gm, inputs[i++], stageId);
			}
			
			//loop over outputs
			i = 0;
			limit = outputs.length;
			while (i<limit) {
				regOutput(gm, outputs[i++], stageId);
			}
			
			endStageRegister(gm);
									
		}
		
	}
	
	//Experimental idea, not yet in use,  what if stages did not need to pass in pipes but could grab the ones needed by type matching??
	   private static void register(GraphManager gm, PronghornStage stage, MessageSchema inputSchema, MessageSchema outputSchema) {
	        
	        synchronized(gm.lock) {
	            int stageId = beginStageRegister(gm, stage);
	            setStateToNew(gm, stageId);
	            
	            if (null != inputSchema) {
	                int p = gm.pipeIdToPipe.length;
	                while (--p>=0) {
	                    Pipe tp = gm.pipeIdToPipe[p];
	                    if (null != tp) {
	                        if (tp.isForSchema(tp, inputSchema)) {
	                            regInput(gm, tp, stageId);
	                        }
	                    }
	                }
	            }
	            
	            if (null != outputSchema) {
                    int p = gm.pipeIdToPipe.length;
                    while (--p>=0) {
                        Pipe tp = gm.pipeIdToPipe[p];
                        if (null != tp) {
                            if (tp.isForSchema(tp, outputSchema)) {
                                regOutput(gm, tp, stageId);
                            }
                        }
                    }
                }
	            
	            endStageRegister(gm);
	        }
	}
	
	/**
	 * Returns all pipes of this time in the same order that they were created in the graph.
	 * @param gm
	 * @param targetSchema
	 */
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfType(GraphManager gm, T targetSchema) {
	    return pipesOfType(0, gm.pipeIdToPipe.length, gm, targetSchema);
	}

	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipesOfType(GraphManager gm, T targetSchema, int minimumPipeId) {
	    return pipesOfType(0, gm.pipeIdToPipe.length, gm, targetSchema, minimumPipeId);
	}
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfType(int count, int p, GraphManager gm, T targetSchema) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
                if (Pipe.isForSchema(tp, targetSchema)) {
                	Pipe<T>[] result = pipesOfType(count+1,p,gm,targetSchema);
                	result[(result.length-1)-count] = tp;
                    return result;
                }
            }
        }
        
        if (0==count) {
        	return EMPTY_PIPE_ARRAY;
        } else {
        	return new Pipe[count];
        }
	    
	}
	
	private static <T extends MessageSchema<T>> Pipe<T>[] pipesOfType(int count, int p, GraphManager gm, T targetSchema, int minimumPipeId) {
		//pass one to count all the instances
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
                if (Pipe.isForSchema(tp, targetSchema) && tp.id>=minimumPipeId) {
                	Pipe<T>[] result = pipesOfType(count+1,p,gm,targetSchema);
                	result[(result.length-1)-count] = tp;
                    return result;
                }
            }
        }
        
        if (0==count) {
        	return EMPTY_PIPE_ARRAY;
        } else {
        	return new Pipe[count];
        }
	    
	}
	
	public static  <T extends MessageSchema<T>> Pipe<T>[] allPipes(GraphManager gm) {
	    
	    //pass one to count all the instances
	    int count = 0;
	    int p = gm.pipeIdToPipe.length;
        while (--p>=0) {
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
            	count++;
            }
        }
        
        if (0==count) {
        	return EMPTY_PIPE_ARRAY;
        }
	    
        //pass two to collect all the instances.
        Pipe[] result = new Pipe[count];
        p = gm.pipeIdToPipe.length; 
        while (--p>=0) { //we are walking backwards over the pipes added
            Pipe tp = gm.pipeIdToPipe[p];
            if (null != tp) {
            	result[--count] = tp; //so we add them backwards to the input array
            }
        }
	    //the order of this array will be the same order that the pipes were added to the graph.
        return result;
	}
	   

	private static void endStageRegister(GraphManager gm) {
		gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
		gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		gm.multNotaIds = setValue(gm.multNotaIds, gm.topNota++, -1);
	}

	private static int beginStageRegister(GraphManager gm, PronghornStage stage) {
		
		assert(gm.enableMutation): "Can not mutate graph, mutation has been disabled";
		
		int stageId = stage.stageId;
		
		assert(stageId>=gm.stageIdToStage.length || null==gm.stageIdToStage[stageId]) : "Can only register the same stage once";
				
		//now store the stage
		gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
		gm.stageIdToInputsBeginIdx = setValue(gm.stageIdToInputsBeginIdx, stageId, gm.topInput);
		gm.stageIdToOutputsBeginIdx = setValue(gm.stageIdToOutputsBeginIdx, stageId, gm.topOutput);			
		gm.stageIdToNotasBeginIdx = setValue(gm.stageIdToNotasBeginIdx, stageId, gm.topNota);		
		gm.stageRunNS = setValue(gm.stageRunNS, stageId, 0);
		gm.stageShutdownTimeNs = setValue(gm.stageShutdownTimeNs, stageId, 0);
		gm.stageStartTimeNs = setValue(gm.stageStartTimeNs, stageId, 0);
		
		//add defaults if a value is not already present
		int d = gm.defaultsCount;
		while(--d >= 0) {
		    //stage does not have the default value so set it
		    GraphManager.addNota(gm, gm.defaultNotaKeys[d], gm.defaultNotaValues[d], stage);
		}
		
		return stageId;
	}



	public static void register(GraphManager gm, PronghornStage stage, Pipe input, Pipe[] outputs) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			//loop over inputs
			regInput(gm, input, stageId);
			
			int i = 0;
			int limit = outputs.length;
			while (i<limit) {
				regOutput(gm, outputs[i++], stageId);
			}
			
			endStageRegister(gm);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, Pipe[] inputs, Pipe output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			int i = 0;
			int limit = inputs.length;
			while (i<limit) {
				regInput(gm, inputs[i++], stageId);
			}
			
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, Pipe input, Pipe output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStateToNew(gm, stageId);
			
			//loop over inputs
			regInput(gm, input, stageId);
	
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm);
		}
	}
	
	private static void setStateToNew(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_NEW);
		}
	}
	
	public static void setStateToStopping(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_STOPPING);
		}
	}

	public static void setStateToStarted(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_STARTED);
			gm.stageStartTimeNs[stageId] = System.nanoTime();
		}
	}
	
	public static void setStateToShutdown(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_TERMINATED);
			//	assert(recordInputsAndOutputValuesForValidation(gm, stage.stageId));
			gm.stageShutdownTimeNs[stageId] = System.nanoTime();
		}
	}
	
	private static boolean recordInputsAndOutputValuesForValidation(GraphManager gm, int stageId) {
		
		//TODO: B, this is a test to see if the positions of the ring buffer have moved after shut down, would be called when the real shutdown is completed
		
		int ringId;
		int idx;
		
		idx = gm.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=gm.multInputIds[idx++])) {	
			
			//record the tail positions for this stage
			
			//gm.ringIdToRing[ringId];	//Must verify these are the same after the shutdown is complete
			
		}
		
		
		idx = gm.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=gm.multOutputIds[idx++])) {	
			
			//record the head positions for this stage
			
			//gm.ringIdToRing[ringId];	//Must verify these are the same after the shutdown is complete	
			
		}	
		
		return true;
	}

	private static void regOutput(GraphManager pm, Pipe output, int stageId) {
		if (null!=output) {
			int outputId = output.id;
			pm.ringIdToStages = setValue(pm.ringIdToStages, (outputId*2) , stageId); //source +0 then target +1
			pm.pipeIdToPipe = setValue(pm.pipeIdToPipe, outputId, output);				
			pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, outputId);
		}
	}
	

	private static void regInput(GraphManager pm, Pipe input,	int stageId) {
		if (null!=input) {
			int inputId = input.id;
			pm.ringIdToStages = setValue(pm.ringIdToStages, (inputId*2)+1, stageId); //source +0 then target +1
			pm.pipeIdToPipe = setValue(pm.pipeIdToPipe, inputId, input);
			pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, inputId);
		}
	}
	
	private final int maxDefaults = 10;
    private final Object[] defaultNotaKeys = new Object[maxDefaults];
    private final Object[] defaultNotaValues = new Object[maxDefaults];
    private int defaultsCount = 0;    
	
	
    public static void addDefaultNota(GraphManager graphManager, Object key, Object value) {
        
        graphManager.defaultNotaKeys[graphManager.defaultsCount] = key;
        graphManager.defaultNotaValues[graphManager.defaultsCount++] = value;
        
    }


	public static void addNota(GraphManager graphManager, Object key, Object value, PronghornStage ... stages) {
	    
		int i = stages.length;
		if (0==i) {
		    throw new UnsupportedOperationException("Must have at least 1 stage to assign this to");
		}
		while (--i>=0) {
			addNota(graphManager, key, value, stages[i]);
		}
	}
	
	public static void addNota(GraphManager m, Object key, Object value, PronghornStage stage) {
		synchronized(m.lock) {	
			
			//if Nota key already exists then replace previous value
			
			int notaId = findNotaIdForKey(m, stage.stageId, key);
			
			if (-1 != notaId) {
				//simple replace with new value
				m.notaIdToValue[notaId] = value;				
			} else {
				//extract as add nota method.
				//even if the same key/value is given to multiple stages we add it multiple times here
				//this allows for direct lookup later for every instance found
				m.notaIdToKey = setValue(m.notaIdToKey, m.totalNotaCount, key);
				m.notaIdToValue = setValue(m.notaIdToValue, m.totalNotaCount, value);
				m.notaIdToStageId = setValue(m.notaIdToStageId, m.totalNotaCount, stage.stageId);
				
				int beginIdx = m.stageIdToNotasBeginIdx[stage.stageId];
			    if (m.topNota == m.multNotaIds.length) {
			    	
			    	//create new larger array		    	
			    	int[] newMultiNotaIdx = new int[m.multNotaIds.length*2];			    	
			    	Arrays.fill(newMultiNotaIdx, -1);
			    	
			    	System.arraycopy(m.multNotaIds, 0, newMultiNotaIdx, 0, beginIdx);
			    	
			    	//copy over and move it down by one so we have room for the new entry
			    	System.arraycopy(m.multNotaIds, beginIdx, newMultiNotaIdx, beginIdx+1, m.topNota-(beginIdx));
			    	
			    	m.multNotaIds = newMultiNotaIdx;		    	
			    } else {
			    	
			    	//move all the data down by one so we have room.
			    	System.arraycopy(m.multNotaIds, beginIdx, m.multNotaIds, beginIdx+1, m.topNota-(beginIdx));
			    }
			    
			    m.multNotaIds[beginIdx] = m.totalNotaCount;//before we inc this value is the index to this key/value pair
			    
				m.totalNotaCount++;
				m.topNota++;
				
			}
			
		}
	}
	
	//also add get regex of key string
	
	private static int findNotaIdForKey(GraphManager m, int stageId, Object key) {
		int idx = m.stageIdToNotasBeginIdx[stageId];
		if (idx>=m.multNotaIds.length) {
			return -1;
		}
		int notaId;
		while(-1 != (notaId = m.multNotaIds[idx])) {
			if (m.notaIdToKey[notaId].equals(key)) {
				return notaId;
			}
			idx++;
		}		
		return -1;
	}
	

	
	/**
	 * Returns nota if one is found by this key on this stage, if nota is not found it returns the defaultValue
	 * @param m
	 * @param stage
	 * @param key
	 */
	public static Object getNota(GraphManager m, PronghornStage stage, Object key, Object defaultValue) {
	    return (null==stage) ? defaultValue : getNota(m, stage.stageId, key, defaultValue);
	}
	
	public static Object getNota(GraphManager m, int stageId, Object key, Object defaultValue) {
	    if (stageId>=0) {
    		int idx = m.stageIdToNotasBeginIdx[stageId];
    		int notaId;
    		while(-1 != (notaId = m.multNotaIds[idx])) {
    			if (m.notaIdToKey[notaId].equals(key)) {
    				return m.notaIdToValue[notaId];
    			}
    			idx++;
    		}
	    }
		return defaultValue;
	}
	
	public static void shutdownNeighborRings(GraphManager pm, PronghornStage baseStage) {
		
		int inputPos  = pm.stageIdToInputsBeginIdx[baseStage.stageId];
	    int outputPos =	pm.stageIdToOutputsBeginIdx[baseStage.stageId];		
		
	    int ringId;
	    
	    while ((ringId = pm.multInputIds[inputPos++])>=0) {
	    	Pipe.shutdown(pm.pipeIdToPipe[ringId]);	
	    }
	    
	    while ((ringId = pm.multOutputIds[outputPos++])>=0) {
		    Pipe.shutdown(pm.pipeIdToPipe[ringId]);	
		}
	}

	public static PronghornStage getStage(GraphManager m, int stageId) {
		return m.stageIdToStage[stageId];
	}
	
	public static Pipe getRing(GraphManager gm, int ringId) {
		return gm.pipeIdToPipe[ringId];
	}
	
	public static PronghornStage getRingProducer(GraphManager gm, int ringId) {
		int stageId = getRingProducerId(gm, ringId);
		if (stageId<0) {
		    throw new UnsupportedOperationException("Can not find input stage writing to pipe "+ringId+". Check graph construction.");
		}
        return  gm.stageIdToStage[stageId];
	}

    static int getRingProducerId(GraphManager gm, int ringId) {
        int idx = ringId*2;
		return idx < gm.ringIdToStages.length ? gm.ringIdToStages[idx] : -1;
    }
	
   public static int getRingProducerStageId(GraphManager gm, int ringId) {
        return getRingProducerId(gm, ringId);
    }
	
	public static PronghornStage getRingConsumer(GraphManager gm, int ringId) {
		int stageId = getRingConsumerId(gm, ringId);
	    if (stageId<0) {
	            throw new UnsupportedOperationException("Can not find stage reading from pipe "+ringId+" Check graph construction.");
	    }
        return  gm.stageIdToStage[stageId];
	}

    static int getRingConsumerId(GraphManager gm, int ringId) {
        int idx = (ringId*2)+1;
		return idx < gm.ringIdToStages.length ? gm.ringIdToStages[idx] : -1;
    }
	

	public static long delayRequiredNS(GraphManager m, int stageId) {
        long waitTime = computeDelayForConsumer(m.stageIdToInputsBeginIdx[stageId], m.pipeIdToPipe, m.multInputIds);
        return addDelayForProducer(m.stageIdToOutputsBeginIdx[stageId], waitTime, m.pipeIdToPipe, m.multOutputIds);
	}
	
   public static boolean isRateLimited(GraphManager m, int stageId) {
        Pipe[] pipeIdToPipe2 = m.pipeIdToPipe;
        
        return isRateLimitedConsumer(m.stageIdToInputsBeginIdx[stageId], pipeIdToPipe2, m.multInputIds) ||
               isRateLimitedProducer(m.stageIdToOutputsBeginIdx[stageId], pipeIdToPipe2, m.multOutputIds);
    }

    private static long computeDelayForConsumer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multInputIds2) {

        int pipeId;
        long waitTime = 0;
        while ((pipeId = multInputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedConsumer(pipeIdToPipe2[pipeId])) {
                long t = Pipe.computeRateLimitConsumerDelay(pipeIdToPipe2[pipeId]);
                if (t>waitTime) {
                    waitTime = t;
                }
            }
        }	
        return waitTime;
    }

    private static boolean isRateLimitedConsumer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multInputIds2) {

        int pipeId;
        while ((pipeId = multInputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedConsumer(pipeIdToPipe2[pipeId])) {
                return true;
            }
        }   
        return false;
    }
    
    private static long addDelayForProducer(int pipeIdx, long waitTimeNS, Pipe[] pipeIdToPipe2, int[] multOutputIds2) {
        int pipeId;
        while ((pipeId = multOutputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedProducer(pipeIdToPipe2[pipeId])) {

                long nsTime = Pipe.computeRateLimitProducerDelay(pipeIdToPipe2[pipeId]);
                if (nsTime > waitTimeNS) {
                    waitTimeNS = nsTime;
                }
            }
        }   
	    return waitTimeNS;
    }
    
    private static boolean isRateLimitedProducer(int pipeIdx, Pipe[] pipeIdToPipe2, int[] multOutputIds2) {
        int pipeId;
        while ((pipeId = multOutputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedProducer(pipeIdToPipe2[pipeId])) {
                return true;
            }
        }   
        return false;
    }
	
	/**
	 * Return false only when every path is checked so every ring is empty and every stage is terminated.
	 */
	public static boolean mayHaveUpstreamData(GraphManager m, int stageId) {
		
		if (isStageTerminated(m, stageId)) { //terminated 
			return false;
		}		
				
		int pipeId;
				
		//if all the output targets have terminated return false because this data will never be consumed
		//using this back pressure the shutdown of all stages can be done		
		int outputPos  = m.stageIdToOutputsBeginIdx[stageId];
		boolean noConsumers = true;
		int count = 0;
		while ((pipeId = m.multOutputIds[outputPos++])>=0) {
			count++;
			int ringConsumerId = GraphManager.getRingConsumerId(m, pipeId);
			noConsumers = noConsumers & (ringConsumerId<0 || isStageTerminated(m,ringConsumerId));						
		}				
		if (count>0 && noConsumers) {
			//ignore input because all the consumers have already shut down
			return false;
		}		
		
		
		int inputPos  = m.stageIdToInputsBeginIdx[stageId];
		int inputCounts=0;
		    
		while ((pipeId = m.multInputIds[inputPos++])>=0) {
							
				++inputCounts;
				
				//check that producer is terminated first.
				if (isProducerTerminated(m, pipeId)) {
					
					//ensure that we do not have any old data still on the ring from the consumer batching releases
    
					//splitter should never have release pending to release because it does not use the release counters	
				//	if (Pipe.hasReleasePending(m.pipeIdToPipe[pipeId])) {
					    Pipe.releaseAllBatchedReads(m.pipeIdToPipe[pipeId]);
			//			Pipe.releaseAll(m.pipeIdToPipe[pipeId]);
				///	}						
					
					//if producer is terminated check input ring, if not empty return true
			    	if (Pipe.contentRemaining( m.pipeIdToPipe[pipeId])>0) {
			    		return true;
			    	}
				} else {
					return true;
				}				
		}
		
		//true if this is a top level stage and was not terminated at top of this function
		//all other cases return false because the recursive check has already completed above.
		return 0==inputCounts && !isStageShuttingDown(m, stageId); //if input is shutting down it must not re-schedule
	}
	
	public static boolean isProducerTerminated(GraphManager m, int ringId) {
		int producerStageId = getRingProducerId(m, ringId);
        return producerStageId<0 || m.stageStateData.stageStateArray[producerStageId] == GraphManagerStageStateData.STAGE_TERMINATED;
	}

    public static boolean isStageTerminated(GraphManager m, int stageId) {
    	synchronized(m.stageStateData.lock) {
    		return GraphManagerStageStateData.STAGE_TERMINATED <= m.stageStateData.stageStateArray[stageId];
    	}
    }

    public static boolean isStageShuttingDown(GraphManager m, int stageId) {
    	return m.stageStateData.stageStateArray[stageId]>=GraphManagerStageStateData.STAGE_STOPPING; //or terminated
    }
    
    public static boolean isStageStarted(GraphManager m, int stageId) {
        return m.stageStateData.stageStateArray[stageId]>=GraphManagerStageStateData.STAGE_STARTED; //or running or shuttingdown or terminated
    }
    
    public static PronghornStage[] allStagesByState(GraphManager graphManager, int state) {
    	 //TODO: rewrite and simple recursive stack unroll to eliminate the duplication of the code here. see pipesOfType
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage && graphManager.stageStateData.stageStateArray[stage.stageId]==state) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage && graphManager.stageStateData.stageStateArray[stage.stageId]==state) {
                stages[--count] = stage;
            }
        }
        return stages;
    }
    
    public static PronghornStage[] allStages(GraphManager graphManager) {
        
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage) {
                stages[--count] = stage;
            }
        }
        return stages;
    }
    
    
    //TODO: AA must have blocking base stage to extend for blockers.
    
	public static void terminateInputStages(GraphManager m) {
		
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			if (null!=m.stageIdToStage[i]) {				
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInputsBeginIdx[m.stageIdToStage[i].stageId]]) {
					//terminate all stages without any inputs
					m.stageIdToStage[i].requestShutdown();
				} else if (null != getNota(m, m.stageIdToStage[i], PRODUCER, null)) {
					//also terminate all stages decorated as producers
					m.stageIdToStage[i].requestShutdown();
					//producers with inputs must be forced to terminate or the input queue will prevent shutdown
					setStateToShutdown(m, m.stageIdToStage[i].stageId); 					
				}
			}
		}
	}
	
	public static int getOutputStageCount(GraphManager m) {
	    int count = 0;
        int i = m.stageIdToStage.length;
        while (--i>=0) {
            if (null!=m.stageIdToStage[i]) {                
                //an input stage is one that has no input ring buffers
                if (-1 == m.multOutputIds[m.stageIdToOutputsBeginIdx[m.stageIdToStage[i].stageId]]) {
                    if (!stageForMonitorData(m, m.stageIdToStage[i])) {
                        count++;
                    }
                }
            }
        }
        return count;
	}
	
	   public static PronghornStage getOutputStage(GraphManager m, int ordinal) {
	        int count = 0;
	        int i = m.stageIdToStage.length;
	        while (--i>=0) {
	            if (null!=m.stageIdToStage[i]) {                
	                //an input stage is one that has no input ring buffers
	                if (-1 == m.multOutputIds[m.stageIdToOutputsBeginIdx[m.stageIdToStage[i].stageId]]) {
	                    if (!stageForMonitorData(m, m.stageIdToStage[i])) {
    	                    if (++count==ordinal) {
    	                        return m.stageIdToStage[i];
    	                    }
	                    }
	                }
	            }
	        }
	        throw new UnsupportedOperationException("Invalid configuration. Unable to find requested output ordinal "+ordinal);
	    }

	public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage) {
		return getOutputPipe(m, stage, 1);
	}
	
	public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, int stageId) {
		return getOutputPipe(m, stageId, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage, int ordinalOutput) {
		return getOutputPipe(m, stage.stageId, ordinalOutput);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getOutputPipe(GraphManager m, int stageId, int ordinalOutput) {
		
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {		
			if (--ordinalOutput<=0) {
				return m.pipeIdToPipe[ringId];
			}
		}	
		throw new UnsupportedOperationException("Invalid configuration. Unable to find requested output ordinal "+ordinalOutput);
	}
	
	public static int getOutputPipeCount(GraphManager m, int stageId) {
		
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stageId];
		int count = 0;
		while (-1 != (ringId=m.multOutputIds[idx++])) {		
			count++;
		}	
		return count;
	}

	public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, PronghornStage stage) {
		return getInputPipe(m, stage, 1);
	}
	
	public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, int stageId) {
		return getInputPipe(m, stageId, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, PronghornStage stage, int ordinalInput) {
		return getInputPipe(m, stage.stageId, ordinalInput);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema<S>> Pipe<S> getInputPipe(GraphManager m, int stageId, int ordinalInput) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			if (--ordinalInput<=0) {
				return m.pipeIdToPipe[ringId];
			}				
		}				
		throw new UnsupportedOperationException("Invalid configuration. Unable to find requested input ordinal "+ordinalInput);
	}
	
	public static int getInputPipeCount(GraphManager m, PronghornStage stage) {
		return getInputPipeCount(m, stage.stageId);
	}
	
	public static int getInputPipeCount(GraphManager m, int stageId) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stageId];
		int count = 0;
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			count++;	
		}				
		return count;
	}
	
	public void exportGraphDotFile() {
	    exportGraphDotFile(this, "graph.dot");
	}
	
	public static void exportGraphDotFile(GraphManager gm, String filename) {
		exportGraphDotFile(gm,filename,null,null);
	}
	
    public static void exportGraphDotFile(GraphManager gm, String filename, int[] percentileValues, int[] traffic) {
    	
    //	new Exception("GENERATING NEW DOT FILE "+filename).printStackTrace();
    	
        FileOutputStream fost;
        try {
            fost = new FileOutputStream(filename);
            PrintWriter pw = new PrintWriter(fost);
            gm.writeAsDOT(gm, pw, percentileValues, traffic);
            pw.close();
            
            
            //to produce the png we must call
            //  dot -Tpng -O deviceGraph.dot        
            Process result = Runtime.getRuntime().exec("dot -Tsvg -o"+filename+".svg "+filename);
            
            if (0!=result.waitFor()) {
                return;
            }
            
        } catch (Throwable e) {        	
        	logger.info("No runtime graph produced. ",e);
        }
       
        
    }
  
    public static void writeAsDOT(GraphManager m, Appendable target) {
    	writeAsDOT(m,target,null,null);
    }
    
	public static void writeAsDOT(GraphManager m, Appendable target, int[] percentileValues, int[] traffic) {
	    try {
	    
	        target.append("digraph {\n");
	        target.append("rankdir = LR\n");
	        
	        //TODO: redesign to be cleaner without garbage
	        Map<Object, StringBuilder> ranks = new HashMap<Object, StringBuilder>();
	        
	        int i = -1;
	        while (++i<m.stageIdToStage.length) {
	            PronghornStage stage = m.stageIdToStage[i];
	        
	            
	            if (null!=stage && !(stage instanceof MonitorConsoleStage) && !(stage instanceof RingBufferMonitorStage)) {       

	            	String stageName = "Stage"+Integer.toString(i);
	            	
	            	
	            	Object rankKey = getNota(m, stage.stageId, GraphManager.DOT_RANK_NAME, null);
	            	if (rankKey!=null) {
	            		
	            		//{ rank=same, b, c, d }
	            		StringBuilder b = ranks.get(rankKey);
	            		if (null==b) {
	            			b = new StringBuilder("{ rank=same");
	            			ranks.put(rankKey, b);
	            		}
	            		b.append(" \""+stageName+"\",");
	            		
	            	}

	            	Object group = GraphManager.getNota(m, stage.stageId, GraphManager.THREAD_GROUP, null);
	            	
	                target.append("\"").append(stageName).append("\"[label=\"").append(stage.toString().replace("Stage","").replace(" ", "\n"));
	                if (null!=group) {
	                	target.append(" grp:"+group);
	                }
	                //if supported give PCT used
	                long runNs = m.stageRunNS[stage.stageId];
	                int pct = 0;
	                if (runNs!=0){
	                	if (runNs<0) {
	                		target.append(" CPU 100%");
	                	} else {
	                		long shutdownTime = m.stageShutdownTimeNs[stage.stageId];
	                		if (shutdownTime<=0) {
	                			//NOTE: this is required becaue FixedThreadScheduler and NonThreadScheduler quit too early before this gets set.
	                			shutdownTime = System.nanoTime();
	                		}
	                		
	                		pct = (int)((10_000L*runNs)/ (shutdownTime - m.stageStartTimeNs[stage.stageId] ));
	                		if (pct>=0) {
	                			Appendables.appendValue(target," CPU ",pct/100,".");
	                			Appendables.appendFixedDecimalDigits(target,pct%100,10).append("%");	            			
	                		} else {
	                			target.append(" CPU N/A%");
	                			
	                			logger.info("A bad % value {} {} {} {}",pct,runNs, m.stageShutdownTimeNs[stage.stageId],  m.stageStartTimeNs[stage.stageId] );
	                					
	                		}
	                	}
	                } else {
	                	target.append(" CPU N/A%");
	                	
            			logger.trace("B bad % value {} {} {} {}",pct,runNs, m.stageShutdownTimeNs[stage.stageId],  m.stageStartTimeNs[stage.stageId] );
            	
            			
	                }
	                target.append("\"");
	                
	                if (pct>=8000) {
                		target.append(",color=red");	    
                	} else if (pct>=6000) {
                		target.append(",color=orange");	    
                	} else {
                	}
	                
	                target.append("]\n");
	                	                
	            }
	        }
	        /////
	        //DOT_RANK_NAME
	        /////
	        for (StringBuilder value: ranks.values()) {
	        	target.append(value.subSequence(0, value.length()-1)).append(" }\n");	        	
	        }
	        
	        /////
	        //pipes
	        /////
	        
	        int undefIdx = 0;
	        int j = m.pipeIdToPipe.length;
	        while (--j>=0) {
	            Pipe pipe = m.pipeIdToPipe[j];
	            if (null!=pipe) {
	                
	                int producer = getRingProducerId(m, j);
	                int consumer = getRingConsumerId(m, j);
	                
	                //skip all pipes that are gathering monitor data
	                if (consumer<0  || !(GraphManager.getStage(m, consumer) instanceof MonitorConsoleStage) ) {
		                
		                
		                if (producer>=0) {
		                    target.append("\"Stage").append(Integer.toString(producer));
		                } else {
		                    target.append("\"Undefined").append(Integer.toString(undefIdx++));	                    
		                }
		                
		                target.append("\" -> ");
		                
		                if (consumer>=0) {
		                    target.append("\"Stage").append(Integer.toString(consumer));
		                } else {
		                    target.append("\"Undefined").append(Integer.toString(undefIdx++));
		                }
		               
		                //compute the min and max count of messages that can be on this pipe at any time
	                    int minFrag = FieldReferenceOffsetManager.minFragmentSize(Pipe.from(pipe));
	                    int maxFrag = FieldReferenceOffsetManager.maxFragmentSize(Pipe.from(pipe));                    
	                    int maxMessagesOnPipe = pipe.sizeOfSlabRing/minFrag;
	                    int minMessagesOnPipe = pipe.sizeOfSlabRing/maxFrag;           
	                   
	                    
		                target.append("\"[label=\"").append(Pipe.schemaName(pipe).replace("Schema", ""));
		                
		                
		                if (null!=percentileValues) {		                	
		                	int pctFull = percentileValues[pipe.id];
		                	Appendables.appendValue(target.append(" Full:"), pctFull).append("% ");
		                	if (pctFull!=0) {
		                		Appendables.appendValue(target,"@", (pctFull*(long)pipe.sizeOfSlabRing/100L)).append(" ");
		                	}
		                }
		                if (null!=traffic) {
		                	int trafficCount = traffic[pipe.id];
		                	if (0!=trafficCount) {
		                		Appendables.appendValue(target.append(" Vol:"), trafficCount).append(" ");	
		                	}
		                }
		                
		                if (minMessagesOnPipe==maxMessagesOnPipe) {
		                    Appendables.appendValue(target," [",minMessagesOnPipe,"]");
		                } else {
		                    Appendables.appendValue( Appendables.appendValue(target," [",minMessagesOnPipe) ,"-",maxMessagesOnPipe,"]");
		                }
		                target.append("\"");
		                
		                //count bindings
		                int consumerStage = GraphManager.getRingConsumerId(m, pipe.id);
		                int producerStage = GraphManager.getRingProducerId(m, pipe.id);
		                float weight = computeWeightBetweenStages(m, consumerStage, producerStage);	                
		                target.append(",weight=").append(Float.toString(weight));
		                
		                if (null!=percentileValues) {		                	
		                	int pctFull = percentileValues[pipe.id];
		                	if (pctFull>=60) {
		                		target.append(",color=red");	    
		                	} else if (pctFull>=40) {
		                		target.append(",color=orange");	    
		                	} else {
		                	}
		                }
		                
		                target.append("]\n");
	                }	          
	            }
	        }	        
	    
            target.append("}\n");
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
	}

    private static float computeWeightBetweenStages(GraphManager m, int consumerStage, int producerStage) {
        float weight = 0f;
        Pipe[] pipes = m.pipeIdToPipe;
        int p = pipes.length;
        while (--p >= 0) {
            if (null != pipes[p]) {
                if (producerStage == GraphManager.getRingProducerId(m, pipes[p].id) &&
                    consumerStage == GraphManager.getRingConsumerId(m, pipes[p].id)) {
                    weight += 2;
                    
                } else if (consumerStage == GraphManager.getRingProducerId(m, pipes[p].id) &&
                           producerStage == GraphManager.getRingConsumerId(m, pipes[p].id)) {
                    weight += 2;
                    
                }
            }
        }
        return weight;
    }
	
	
	public static void logOutputs(Logger log, GraphManager m, PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			log.error("Output Queue :{}",m.pipeIdToPipe[ringId]);				
		}	
	}

	public static void logInputs(Logger log, GraphManager m,	PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			log.error("Input Queue :{}",m.pipeIdToPipe[ringId]);					
		}				
		
	}

	public static boolean isInputsEmpty(GraphManager m, PronghornStage stage) {
		 return isInputsEmpty(m, stage.stageId);
	}

	public static boolean isInputsEmpty(GraphManager m, int stageId) {
		int ringId;
	     int idx = m.stageIdToInputsBeginIdx[stageId];
	     while (-1 != (ringId=m.multInputIds[idx++])) { 
	         
	         if (Pipe.contentRemaining(m.pipeIdToPipe[ringId])>0) {
	             return false;
	         }                
	     }               
	     return true;
	}
	
	/**
	 * Initialize the buffers for the input rings of this stage and
	 * Block until some other code has initialized the output rings. 
	 * 
	 * @param m
	 * @param stageId
	 */
	public static void initAllPipes(GraphManager m, int stageId) {
		int idx;
		initInputPipesAsNeeded(m, stageId);
		//Does not return until some other stage has initialized the output rings
		int pipeId;
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (pipeId=m.multOutputIds[idx++])) {
		    
		    
		    try {
    		    //double check that this was not built wrong, there must be a consumer of this ring or it was explicitly initialized
		        int consumerId = getRingConsumerId(m, pipeId);
		        if (consumerId < 0 && !Pipe.isInit(getRing(m, pipeId))) {
		            
		            String schemaName = Pipe.schemaName(m.pipeIdToPipe[pipeId]);
		            
		            int producerId = getRingProducerId(m, pipeId);
		            if (producerId<0) {
		                throw new UnsupportedOperationException("Can not find stage consuming Pipe<"+schemaName+"> #"+pipeId+" "+CHECK_GRAPH_CONSTRUCTION);		                
		            } else {
		                PronghornStage prodStage = getRingProducer(m, pipeId);
		                
		                throw new UnsupportedOperationException("Can not find stage consuming Pipe<"+schemaName+"> #"+pipeId+" Which is produced by stage "+prodStage+". "+CHECK_GRAPH_CONSTRUCTION);
		                
		            }
		        }
		    } catch (ArrayIndexOutOfBoundsException aiobe) {
		        if ("-1".equals(aiobe.getMessage())) {
		            throw new UnsupportedOperationException("No consumer for pipe "+pipeId);
		        } else {
		            throw new RuntimeException(aiobe);
		        }
		    }
		    
		    
		    //STILL UNDER TEST IF THIS IS A GOOD IDEA
		    if (!stageForMonitorData(m, getStage(m, stageId))) {
			    
			    //blocking wait on the other stage to init this pipe, required for clean startup only.
			    long timeout = System.currentTimeMillis()+20_000;
				while (!Pipe.isInit(m.pipeIdToPipe[pipeId])) {
					Thread.yield();
					if (System.currentTimeMillis()>timeout) {
						throw new RuntimeException("Check Graph, unable to startup "+GraphManager.getStage(m, stageId)+" due to output "+m.pipeIdToPipe[pipeId]+" consumed by "+getRingConsumer(m,m.pipeIdToPipe[pipeId].id));
					}
				}				
			}
		}	
		
	}

	public static void initInputPipesAsNeeded(GraphManager m, int stageId) {
		int pipeIdIn;
		int idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (pipeIdIn=m.multInputIds[idx++])) {
			if (!Pipe.isInit(m.pipeIdToPipe[pipeIdIn])) {
				m.pipeIdToPipe[pipeIdIn].initBuffers();		
			}
		}
	}
	
	public static void reportError(GraphManager graphManager, final PronghornStage stage, Throwable t, Logger logger) {
          String msg = t.getMessage();
          if (null != msg && msg.contains(CHECK_GRAPH_CONSTRUCTION)) {
              logger.error(msg);
          } else {                
              if (null==stage) {
                  logger.error("Stage was never initialized");
              } else {
              
                  int inputCount = GraphManager.getInputPipeCount(graphManager, stage);
                  int outputCount = GraphManager.getOutputPipeCount(graphManager,stage.stageId);
                  
                  logger.error("Unexpected error in "+stage+" which has "+inputCount+" inputs and "+outputCount+" outputs", t);
                  
//                  int i = inputCount;
//                  while (--i>=0) {
//                      
//                      logger.error(stage+"  input pipe in state:"+ GraphManager.getInputPipe(graphManager, stage, i+1));
//                      
//                  }
                  
                  GraphManager.shutdownNeighborRings(graphManager, stage);
              }
          }
      }  
	
	public static Pipe[] attachMonitorsToGraph(GraphManager gm, Long monitorRate, PipeConfig ringBufferMonitorConfig) {

		int j = gm.pipeIdToPipe.length;
		int count = 0;
		while (--j>=0) {
			if (null!=gm.pipeIdToPipe[j]) {
				count++;
			}
		}
		if (0==count) {
			throw new UnsupportedOperationException("Nothing to monitor, move this call down to after graph is constructed.");
		}
		Pipe[] monBuffers = new Pipe[count];
		int monBufIdx = 0;
		j = gm.pipeIdToPipe.length;
		while (--j>=0) {
			
			Pipe ringBuffer = gm.pipeIdToPipe[j];
			//Do not monitor those rings that are part of other monitoring networks.
			if (null!=ringBuffer && !ringHoldsMonitorData(gm, ringBuffer) ) {

				monBuffers[monBufIdx] = new Pipe(ringBufferMonitorConfig);
				RingBufferMonitorStage stage = new RingBufferMonitorStage(gm, ringBuffer,  monBuffers[monBufIdx]);
				GraphManager.addNota(gm, GraphManager.MONITOR, "dummy", stage);
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, monitorRate, stage);
				
				monBufIdx++;
				
			}
		}
		return monBuffers;	
	}

	private static boolean ringHoldsMonitorData(GraphManager gm, Pipe ringBuffer) {
		return null != GraphManager.getNota(gm, GraphManager.getRingProducerStageId(gm, ringBuffer.id), GraphManager.MONITOR, null);
	}
	
    private static boolean stageForMonitorData(GraphManager gm, PronghornStage stage) {
        return null != GraphManager.getNota(gm, stage, GraphManager.MONITOR, null);
    }

	public static void enableBatching(GraphManager gm) {
		int j = gm.pipeIdToPipe.length;
		while (--j>=0) {
			Pipe ring = gm.pipeIdToPipe[j];
			//never enable batching on the monitor rings
			if (null!=ring && !ringHoldsMonitorData(gm, ring) ) {
				
				int ringId1 = ring.id;
				int stageId1 = GraphManager.getRingConsumerId(gm, ringId1);
				if (stageId1>=0) {
					if (PronghornStage.supportsBatchedRelease(gm.stageIdToStage[stageId1])) { 
						Pipe.setMaxReleaseBatchSize(ring);
					}
				}
				
				int ringId = ring.id;
				int stageId = GraphManager.getRingProducerId(gm, ringId);
				if (stageId>=0) {
					if (PronghornStage.supportsBatchedPublish(gm.stageIdToStage[stageId])) {
						Pipe.setMaxPublishBatchSize(ring);
					}
				}
			}
		}
	}

	public static String getRingName(GraphManager gm, Pipe ringBuffer) {
		
	    final int ringId = ringBuffer.id;
	    String consumerName = "UnknownConsumer";
	    {
            int stageId = getRingConsumerId(gm, ringId);
            if (stageId>=0) {
                PronghornStage consumer = gm.stageIdToStage[stageId];
                consumerName = getNota(gm, consumer, STAGE_NAME, consumer.getClass().getSimpleName()).toString()+"#"+stageId;
            }
	    }
	    String producerName = "UnknownProducer";
	    {
            int stageId = getRingProducerId(gm, ringId);
            if (stageId>=0) {                
                PronghornStage producer = gm.stageIdToStage[stageId];                
                producerName = getNota(gm, producer, STAGE_NAME, producer.getClass().getSimpleName()).toString()+"#"+stageId;  
            }
	    }
	    
		return producerName + "-"+Integer.toString(ringBuffer.id)+"-" + consumerName;
	}

	/**
	 * Start with ordinal selection of input stages then ordinal selection of each output ring there after.
	 * TODO: do generic return that extends pronghornStage
	 * @param m
	 * @param path
	 */
	public static PronghornStage findStageByPath(GraphManager m, int ... path) {
		
		int ordinal = path[0];
		int i = 0;
	    int limit = m.stageIdToStage.length;
		while (i<limit) {
			if (null!=m.stageIdToStage[i]) {				
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInputsBeginIdx[m.stageIdToStage[i].stageId]]) {
					if (--ordinal<=0) {
						//starting from 1 find this path
						return findStageByPath(m, m.stageIdToStage[i], 1, path);
					}
				}
			}
			i++;
		}	
		throw new UnsupportedOperationException("Unable to find ordinal input stage of "+path[0]);
	}

	private static PronghornStage findStageByPath(GraphManager m, PronghornStage stage, int idx, int[] path) {
		if (idx>=path.length) {
			return stage;
		}
		return findStageByPath(m,getRingConsumer(m, getOutputPipe(m,stage,path[idx]).id),1+idx,path);
	}

	//when batching is used we need to flush outstanding writes before yield
	public static void publishAllWrites(GraphManager m, PronghornStage stage) {
	    
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {
		    if (Pipe.getPublishBatchSize(m.pipeIdToPipe[ringId])>0) {
		        Pipe.publishAllBatchedWrites(m.pipeIdToPipe[ringId]);	
		    }
		}
		
	}

    public void blockUntilStageBeginsShutdown(PronghornStage stageToWatch) {
        blockUntilStageBeginsShutdown(this,stageToWatch);
    }
	


    public static void spinLockUntilStageOfTypeStarted(GraphManager gm, Class<?> stageClass) {
        boolean isStarted;
        do {
            isStarted = true;
            Thread.yield();
            PronghornStage[] stages = allStagesByType(gm, stageClass);
            int s = stages.length;
            while (--s>=0) {
                isStarted &= isStageStarted(gm, stages[s].stageId);
            }
        } while (!isStarted);
    }

    public static void blockUntilStageBeginsShutdown(GraphManager gm, PronghornStage stageToWatch) {
        //keep waiting until this stage starts it shut down or completed its shutdown, 
        //eg return on leading edge as soon as we detect shutdown in progress..
        while (!  (isStageShuttingDown(gm, stageToWatch.stageId)||isStageTerminated(gm, stageToWatch.stageId)) ) { 
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
   

    public static boolean blockUntilStageBeginsShutdown(GraphManager gm, PronghornStage stageToWatch, long timeoutMS) {
        //keep waiting until this stage starts it shut down or completed its shutdown, 
        //eg return on leading edge as soon as we detect shutdown in progress..
        while (--timeoutMS>=0 && (!  (isStageShuttingDown(gm, stageToWatch.stageId)||isStageTerminated(gm, stageToWatch.stageId))) ) { 
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt();
            	return true;
            }
        }
        return timeoutMS>=0;
    }
    

    public static PronghornStage[] allStagesByType(GraphManager graphManager, Class<?> stageClass) {
    	 //TODO: rewrite and simple recursive stack unroll to eliminate the duplication of the code here. see pipesOfType
        int count = 0;
        int s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null!=stage && stage.getClass().isAssignableFrom(stageClass)) {
                count++;
            }
        }
        
        PronghornStage[] stages = new PronghornStage[count];
        s = graphManager.stageIdToStage.length;
        while (--s>=0) {
            PronghornStage stage = graphManager.stageIdToStage[s];             
            if (null != stage && stage.getClass().isAssignableFrom(stageClass)) {
                stages[--count] = stage;
            }
        }
        return stages;
    }

    public static boolean isAllPipesEmpty(GraphManager graphManager) {        
        Pipe[] pipes = graphManager.pipeIdToPipe;
        int p = pipes.length;
        while (--p >= 0) {
            if (null != pipes[p]) {
                if (Pipe.contentRemaining(pipes[p]) > 0) {   
                    return false;
                }
            }
        }
        return true;
    }

	public static void accumRunTimeNS(GraphManager graphManager, int stageId, long duration) {
		graphManager.stageRunNS[stageId] += duration; 
	}

	public static void accumRunTimeAll(GraphManager graphManager, int stageId) {		
		graphManager.stageRunNS[stageId] = -1; //flag for 100%
	}


	

}
