package com.ociweb.pronghorn.stage.scheduling;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.RingBufferMonitorStage;

public class GraphManager {
	
	private class GraphManagerStageStateData {
		private Object lock = new Object();	
		private byte[] stageStateArray = new byte[0];
		
		public final static byte STAGE_NEW = 0;
		public final static byte STAGE_STARTED = 1;
		public final static byte STAGE_STOPPING = 2;
		public final static byte STAGE_TERMINATED = 3;
		
	}
	
    //Nota bene attachments
	public final static String SCHEDULE_RATE = "SCHEDULE_RATE"; //in ns - this is the delay between calls regardless of how long call takes
	                                                        //If dependable/regular clock is required run should not return and do it internally.
	public final static String MONITOR       = "MONITOR"; //this stage is not part of business logic but part of internal monitoring.
	public final static String PRODUCER      = "PRODUCER";//explicit so it can be found even if it has feedback inputs.
	public final static String STAGE_NAME    = "STAGE_NAME";
	
	//do not use thise they are under development
	public final static String UNSCHEDULED   = "UNSCHEDULED";//new nota for stages that should never get a thread (experimental)
	public final static String BLOCKING      = "BLOCKING";   //new nota for stages that do not give threads back (experimental)
	
	
	private final static int INIT_RINGS = 32;
	private final static int INIT_STAGES = 32;
	
	private final static Logger log = LoggerFactory.getLogger(GraphManager.class);

				
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
	
	//for lookup of Stage from Stage id
	private PronghornStage[]  stageIdToStage = new PronghornStage[INIT_STAGES];
			
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
	
   @Deprecated	
   static PronghornStage[] getStages(GraphManager m) {
        return m.stageIdToStage;
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

		int i = -1;
		while (++i<m.stageIdToStage.length) {
			if (null!=m.stageIdToStage[i]) {				
				if (!isStageTerminated(m, i) ) { 				
					PronghornStage stage = getStage(m,i);
					StageScheduler.log.error("-------------------");//divide the log for better clarity
					logInputs(StageScheduler.log, m, stage);
					StageScheduler.log.error("  Expected stage {} to be stopped but it appears to be running. terminated:{}", stage, isStageTerminated(m, i));
					logOutputs(StageScheduler.log, m, stage);
					StageScheduler.log.error("-------------------");//divide the log for better clarity
					
					result = false;
					
					reportUnexpectedThreadStacks();
					
					
				}				
			}
		}		
		if (!result) {
			StageScheduler.log.error("unclean shutdown");				
		}
		return result;
	}

    private static void reportUnexpectedThreadStacks() {
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
	
	private static Pipe[] setValue(Pipe[] target, int idx, Pipe value) {		
		Pipe[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return result;
	}
	
	private static PronghornStage[] setValue(PronghornStage[] target, int idx, PronghornStage value) {		
		PronghornStage[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return result;
	}	
	
	private static Object[] setValue(Object[] target, int idx, Object value) {		
		Object[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return result;
	}	
	
	private static byte[] setValue(byte[] target, int idx, final byte value) {		
		
		byte[] result = target;
		if (idx>=target.length) {
			int limit = (1+idx)*2;
			result = Arrays.copyOf(target, limit); //double the array
			Arrays.fill(result, target.length, limit, (byte)-1);
		}
		
		assert(value >= result[idx]) : "byte values are only allowed to move forward, check the state rules. Found "+result[idx]+" expected "+(value-1);
		
		result[idx] = value;
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

	private static void endStageRegister(GraphManager gm) {
		gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
		gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		gm.multNotaIds = setValue(gm.multNotaIds, gm.topNota++, -1);
	}

	private static int beginStageRegister(GraphManager gm, PronghornStage stage) {
		
		assert(gm.enableMutation): "Can not mutate graph, mutation has been disabled";
		
		int stageId = stage.stageId;
		
		assert(stageId>=gm.stageIdToStage.length || null==gm.stageIdToStage[stageId]) : "Can only register the same stage once";
				
		gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
		gm.stageIdToInputsBeginIdx = setValue(gm.stageIdToInputsBeginIdx, stageId, gm.topInput);
		gm.stageIdToOutputsBeginIdx = setValue(gm.stageIdToOutputsBeginIdx, stageId, gm.topOutput);			
		gm.stageIdToNotasBeginIdx = setValue(gm.stageIdToNotasBeginIdx, stageId, gm.topNota);
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
		setStateToStarted(gm,stageId);
	}
	
	public static void setStateToStopping(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_STOPPING);
		}
	}

	public static void setStateToStarted(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_STARTED);
		}
	}
	
	public static void setStateToShutdown(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageStateArray = setValue(gm.stageStateData.stageStateArray, stageId, GraphManagerStageStateData.STAGE_TERMINATED);
			//	assert(recordInputsAndOutputValuesForValidation(gm, stage.stageId));
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
			int outputId = output.ringId;
			pm.ringIdToStages = setValue(pm.ringIdToStages, (outputId*2) , stageId); //source +0 then target +1
			pm.pipeIdToPipe = setValue(pm.pipeIdToPipe, outputId, output);				
			pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, outputId);
		}
	}
	

	private static void regInput(GraphManager pm, Pipe input,	int stageId) {
		if (null!=input) {
			int inputId = input.ringId;
			pm.ringIdToStages = setValue(pm.ringIdToStages, (inputId*2)+1, stageId); //source +0 then target +1
			pm.pipeIdToPipe = setValue(pm.pipeIdToPipe, inputId, input);
			pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, inputId);
		}
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
			    	System.arraycopy(m.multNotaIds, beginIdx, newMultiNotaIdx, beginIdx, m.topNota-(beginIdx));
			    	
			    	m.multNotaIds = newMultiNotaIdx;		    	
			    } else {
			    	//move all the data down.
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
	 * @return
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
		int stageId = gm.ringIdToStages[ringId*2];
		if (stageId<0) {
		    throw new UnsupportedOperationException("Can not find input stage writing to pipe "+ringId+". Check graph construction.");
		}
        return  gm.stageIdToStage[stageId];
	}
	
   public static int getRingProducerStageId(GraphManager gm, int ringId) {
        return gm.ringIdToStages[ringId*2];
    }
	
	public static PronghornStage getRingConsumer(GraphManager gm, int ringId) {
		int stageId = gm.ringIdToStages[(ringId*2)+1];
	    if (stageId<0) {
	            throw new UnsupportedOperationException("Can not find output stage reading from  "+ringId+" Check graph construction.");
	    }
        return  gm.stageIdToStage[stageId];
	}
	

	public static long delayRequiredMS(GraphManager m, int stageId) {
        Pipe[] pipeIdToPipe2 = m.pipeIdToPipe;
        long waitTime = computeDelayForConsumer(m.stageIdToInputsBeginIdx[stageId], pipeIdToPipe2, m.multInputIds);
        return addDelayForProducer(m.stageIdToOutputsBeginIdx[stageId], waitTime, pipeIdToPipe2, m.multOutputIds);
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
    
    private static long addDelayForProducer(int pipeIdx, long waitTime, Pipe[] pipeIdToPipe2, int[] multOutputIds2) {
        int pipeId;
        while ((pipeId = multOutputIds2[pipeIdx++])>=0) {            
            if (Pipe.isRateLimitedProducer(pipeIdToPipe2[pipeId])) {
                long t = Pipe.computeRateLimitProducerDelay(pipeIdToPipe2[pipeId]);
                if (t>waitTime) {
                    waitTime = t;
                }
            }
        }   
	    return waitTime;
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
			noConsumers = noConsumers & isStageTerminated(m,GraphManager.getRingConsumer(m, pipeId).stageId);						
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
		int producerStageId = m.ringIdToStages[ringId*2];
		if (producerStageId<0) {
		    log.debug("No producer stage was found for ring {}, check the graph builder.",ringId);
		    return true;
		}
        return m.stageStateData.stageStateArray[producerStageId] == GraphManagerStageStateData.STAGE_TERMINATED;
	}

    public static boolean isStageTerminated(GraphManager m, int stageId) {
    	synchronized(m.stageStateData.lock) {
    		return GraphManagerStageStateData.STAGE_TERMINATED <= m.stageStateData.stageStateArray[stageId];
    	}
    }

    public static boolean isStageShuttingDown(GraphManager m, int stageId) {
    	return m.stageStateData.stageStateArray[stageId]>=GraphManagerStageStateData.STAGE_STOPPING; //or terminated
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

	public static <S extends MessageSchema> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage) {
		return getOutputPipe(m, stage, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema> Pipe<S> getOutputPipe(GraphManager m, PronghornStage stage, int ordinalOutput) {
		
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
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

	public static <S extends MessageSchema> Pipe<S> getInputPipe(GraphManager m, PronghornStage stage) {
		return getInputPipe(m, stage, 1);
	}
	
	@SuppressWarnings("unchecked")
    public static <S extends MessageSchema> Pipe<S> getInputPipe(GraphManager m,	PronghornStage stage, int ordinalInput) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			if (--ordinalInput<=0) {
				return m.pipeIdToPipe[ringId];
			}				
		}				
		throw new UnsupportedOperationException("Invalid configuration. Unable to find requested input ordinal "+ordinalInput);
	}
	
	public static int getInputPipeCount(GraphManager m, PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stage.stageId];
		int count = 0;
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			count++;	
		}				
		return count;
	}
	
	
	public static void writeAsDOT(GraphManager m, Appendable target) {
	    try {
	    
	        target.append("digraph {\n");
	        target.append("rankdir = LR\n");
	        
	        
	        int i = -1;
	        while (++i<m.stageIdToStage.length) {
	            PronghornStage stage = m.stageIdToStage[i];
	            if (null!=stage) {       
	                
	                target.append("\"Stage").append(Integer.toString(i)).append("\"[label=\"").append(stage.getClass().getSimpleName().replace("Stage","")).append("\"]\n");
	                	                
	            }
	        }
	        
	        int j = m.pipeIdToPipe.length;
	        while (--j>=0) {
	            Pipe pipe = m.pipeIdToPipe[j];	            
	            if (null!=pipe) {
	                
	                int producer = m.ringIdToStages[j*2];
	                int consumer = m.ringIdToStages[(j*2)+1];
	                
	                target.append("\"Stage").append(Integer.toString(producer)).append("\" -> \"Stage").append(Integer.toString(consumer)).
	                       append("\"[label=\"").append(Pipe.schemaName(pipe).replace("Schema", "")).append("\"]\n");
	                
	          
	            }
	        }
	        
	    
            target.append("}\n");
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

	/**
	 * Initialize the buffers for the input rings of this stage and
	 * Block until some other code has initialized the output rings. 
	 * 
	 * @param m
	 * @param stageId
	 */
	public static void initInputRings(GraphManager m, int stageId) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {
			m.pipeIdToPipe[ringId].initBuffers();				
		}
		//Does not return until some other stage has initialized the output rings
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {
		    
		    try {
    		    //double check that this was not built wrong, there must be a consumer of this ring
    		    if (null==GraphManager.getRingConsumer(m, ringId)) {
    		        throw new UnsupportedOperationException("No consumer for ring "+ringId);
    		    }				
		    } catch (ArrayIndexOutOfBoundsException aiobe) {
		        if ("-1".equals(aiobe.getMessage())) {
		            throw new UnsupportedOperationException("No consumer for ring "+ringId);
		        } else {
		            throw new RuntimeException(aiobe);
		        }
		    }
		    
			while (!Pipe.isInit(m.pipeIdToPipe[ringId])) {
				Thread.yield();
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
		return null != GraphManager.getNota(gm, GraphManager.getRingProducerStageId(gm, ringBuffer.ringId), GraphManager.MONITOR, null);
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
				
				PronghornStage consumer = GraphManager.getRingConsumer(gm, ring.ringId);
				if (PronghornStage.supportsBatchedRelease(consumer)) { 
					Pipe.setMaxReleaseBatchSize(ring);
				}
				
				PronghornStage producer = GraphManager.getRingProducer(gm, ring.ringId);
				if (PronghornStage.supportsBatchedPublish(producer)) {
					Pipe.setMaxPublishBatchSize(ring);
				}				
				
			}
		}
	}

	public static String getRingName(GraphManager gm, Pipe ringBuffer) {
		
	    final int ringId = ringBuffer.ringId;
	    String consumerName = "UnknownConsumer";
	    {
            int stageId = gm.ringIdToStages[(ringId*2)+1];
            if (stageId>=0) {
                PronghornStage consumer = gm.stageIdToStage[stageId];
                consumerName = getNota(gm, consumer, STAGE_NAME, consumer.getClass().getSimpleName()).toString();
            }
	    }
	    String producerName = "UnknownProducer";
	    {
            int stageId = gm.ringIdToStages[ringId*2];
            if (stageId>=0) {                
                PronghornStage producer = gm.stageIdToStage[stageId];                
                producerName = getNota(gm, producer, STAGE_NAME, producer.getClass().getSimpleName()).toString();  
            }
	    }
	    
		return producerName + "-"+Integer.toString(ringBuffer.ringId)+"-" + consumerName;
	}

	/**
	 * Start with ordinal selection of input stages then ordinal selection of each output ring there after.
	 * TODO: do generic return that extends pronghornStage
	 * @param gm
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
		return findStageByPath(m,getRingConsumer(m, getOutputPipe(m,stage,path[idx]).ringId),1+idx,path);
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

    public static void blockUntilStageBeginsShutdown(GraphManager gm, PronghornStage stageToWatch) {
        //keep waiting until this stage starts it shut down or completed its shutdown, 
        //eg return on leading edge as soon as we detect shutdown in progress..
        while (!  (isStageShuttingDown(gm, stageToWatch.stageId)||isStageTerminated(gm, stageToWatch.stageId)) ) { 
            LockSupport.parkNanos(100_000);
            
            //TODO: delete the folloing code after this is tested on the Edison, not trusting parkNanos yet.
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
    }
	
//TODO: B, integrate this into the schedulers	
//    public static void releaseAllReads(GraphManager m, PronghornStage stage) {
//        int ringId;
//        int idx = m.stageIdToInputsBeginIdx[stage.stageId];
//        while (-1 != (ringId=m.multInputIds[idx++])) {  //TODO: could be unrolled an inlined
//            RingBuffer.releaseAllBatchedReads(m.ringIdToRing[ringId]);           
//        }       
//    }
//	
    
    

}
