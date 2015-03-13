package com.ociweb.pronghorn.stage.scheduling;

import java.util.Arrays;

import org.slf4j.Logger;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;

public class GraphManager {
	
	private static class GraphManagerStageStateData {
		private Object lock = new Object();	
		public byte[] stageTerminationState = new byte[0];
	}

	public final static String SCHEDULE_RATE = "SCHEDULE_RATE";
	private final static int INIT_RINGS = 32;
	private final static int INIT_STAGES = 32;

				
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
	private RingBuffer[] ringIdToRing = new RingBuffer[INIT_RINGS];
	
	//for lookup of Stage from Stage id
	private PronghornStage[]  stageIdToStage = new PronghornStage[INIT_STAGES];
			
	//This object is shared with all clones
	private final GraphManagerStageStateData stageStateData;
	
	//add the annotation to this list first so we have an Id associated with it
	private Object[] annotationIdToKey = new Object[INIT_STAGES];
	private Object[] annotationIdToValue = new Object[INIT_STAGES];
	private int[] annotationIdToStageId = new int[INIT_STAGES];
	private int totalAnnotationCount = 0;

	//store the annotation ids here
	private int[] stageIdToAnnotationsBeginIdx = new int[INIT_STAGES];
	private int[] multAnnotationIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topAnnotation = 0;
	
	//this is never used as a runtime but only as a construction lock
	private Object lock = new Object();	
	
	private boolean enableMutation = true;
	
	public GraphManager() {
		stageStateData = new GraphManagerStageStateData();
	}
	
	private GraphManager(GraphManagerStageStateData parentStageStateData) {
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
				copyAnnotationsForStage(m, clone, stage);	
			}
		}
		return clone;
	}
	
	public GraphManager cloneStagesWithAnnotationKey(GraphManager m, Object key) {
		GraphManager clone = new GraphManager();
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (null != getAnnotation(m, stage, key, null)) {
					copyStage(m, clone, stage);
					copyAnnotationsForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	public GraphManager cloneStagesWithAnnotationKeyValue(GraphManager m, Object key, Object value) {
		GraphManager clone = new GraphManager();
		//register each stage
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			PronghornStage stage = m.stageIdToStage[i];
			if (null!=stage) {
				//copy this stage if it has the required key
				if (value.equals(getAnnotation(m, stage, key, null))) {
					copyStage(m, clone, stage);
					copyAnnotationsForStage(m, clone, stage);
				}
			}
		}
		return clone;
	}
	
	
	private static void copyAnnotationsForStage(GraphManager m,	GraphManager clone, PronghornStage stage) {
		int idx;
		int annotationId;
		int stageId = stage.stageId;
		
		idx = m.stageIdToAnnotationsBeginIdx[stageId];
		while (-1 != (annotationId=m.multAnnotationIds[idx++])) {
			Object key = m.annotationIdToKey[annotationId];
			Object value = m.annotationIdToValue[annotationId];					
			addAnnotation(clone, key, value, stage);									
		}
	}

	private static void copyStage(GraphManager m, GraphManager clone, PronghornStage stage) {
		int stageId = beginStageRegister(clone, stage);
		
		int idx;
		int ringId;
		
		idx = m.stageIdToInputsBeginIdx[stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			assert(0==RingBuffer.contentRemaining(m.ringIdToRing[ringId]));
			regInput(clone, m.ringIdToRing[ringId], stageId);					
		}				
		
		idx = m.stageIdToOutputsBeginIdx[stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			assert(0==RingBuffer.contentRemaining(m.ringIdToRing[ringId]));
			regOutput(clone, m.ringIdToRing[ringId], stageId);					
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
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		result[idx] = value;
		return result;
	}
	
	private static RingBuffer[] setValue(RingBuffer[] target, int idx, RingBuffer value) {		
		RingBuffer[] result = target;
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
	
	private static byte[] incValue(byte[] target, int idx) {		
		
		byte[] result = target;
		if (idx>=target.length) {
			result = Arrays.copyOf(target, (1+idx)*2); //double the array
		}
		//very large count that is not expected to roll-over
		result[idx]++;
		return result;
	}
	

	public static void register(GraphManager gm, PronghornStage stage, RingBuffer[] inputs, RingBuffer[] outputs) {
		
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStageInitialState(gm, stageId);
			
			int i;
			//loop over inputs
			i = inputs.length;
			while (--i>=0) {
				regInput(gm, inputs[i], stageId);
			}
			
			//loop over outputs
			i = outputs.length;
			while (--i>=0) {
				regOutput(gm, outputs[i], stageId);
			}
			
			endStageRegister(gm);
		}
		
	}

	private static void endStageRegister(GraphManager gm) {
		gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
		gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		gm.multAnnotationIds = setValue(gm.multAnnotationIds, gm.topAnnotation++, -1);
	}

	private static int beginStageRegister(GraphManager gm, PronghornStage stage) {
		
		assert(gm.enableMutation): "Can not mutate graph, mutation has been disabled";
		
		int stageId = stage.stageId;
		
		assert(stageId>=gm.stageIdToStage.length || null==gm.stageIdToStage[stageId]) : "Can only register the same stage once";
				
		gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
		gm.stageIdToInputsBeginIdx = setValue(gm.stageIdToInputsBeginIdx, stageId, gm.topInput);
		gm.stageIdToOutputsBeginIdx = setValue(gm.stageIdToOutputsBeginIdx, stageId, gm.topOutput);			
		gm.stageIdToAnnotationsBeginIdx = setValue(gm.stageIdToAnnotationsBeginIdx, stageId, gm.topAnnotation);
		return stageId;
	}

	private static void setStageInitialState(GraphManager gm, int stageId) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageTerminationState = incValue(gm.stageStateData.stageTerminationState, stageId); //state changes from 0 to 1
			assert(1 == gm.stageStateData.stageTerminationState[stageId]);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer input, RingBuffer[] outputs) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStageInitialState(gm, stageId);
			
			//loop over inputs
			regInput(gm, input, stageId);
			
			int i;
			//loop over outputs
			i = outputs.length;
			while (--i>=0) {
				regOutput(gm, outputs[i], stageId);
			}
			
			endStageRegister(gm);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer[] inputs, RingBuffer output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStageInitialState(gm, stageId);
			
			int i;
			//loop over inputs
			i = inputs.length;
			while (--i>=0) {
				regInput(gm, inputs[i], stageId);
			}
			
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer input, RingBuffer output) {
		synchronized(gm.lock) {
			int stageId = beginStageRegister(gm, stage);
			setStageInitialState(gm, stageId);
			
			//loop over inputs
			regInput(gm, input, stageId);
	
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			endStageRegister(gm);
		}
	}
	
	public static void terminate(GraphManager gm, PronghornStage stage ) {
		synchronized(gm.stageStateData.lock) {
			gm.stageStateData.stageTerminationState = incValue(gm.stageStateData.stageTerminationState, stage.stageId); //state changes from 1 to 2
			if ( gm.stageStateData.stageTerminationState[stage.stageId]>2) {
				gm.stageStateData.stageTerminationState[stage.stageId] = 2;
			}
			assert(2 == gm.stageStateData.stageTerminationState[stage.stageId]);
		}
	}

	private static void regOutput(GraphManager pm, RingBuffer output,
			int stageId) {
		int outputId = output.ringId;
		pm.ringIdToStages = setValue(pm.ringIdToStages, (outputId*2) , stageId); //source +0 then target +1
		pm.ringIdToRing = setValue(pm.ringIdToRing, outputId, output);				
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, outputId);
	}

	private static void regInput(GraphManager pm, RingBuffer input,	int stageId) {
		int inputId = input.ringId;
		pm.ringIdToStages = setValue(pm.ringIdToStages, (inputId*2)+1, stageId); //source +0 then target +1
		pm.ringIdToRing = setValue(pm.ringIdToRing, inputId, input);
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, inputId);
	}


	public static void addAnnotation(GraphManager graphManager, Object key, Object value, PronghornStage ... stages) {
		int i = stages.length;
		while (--i>=0) {
			addAnnotation(graphManager, key, value, stages[i]);
		}
	}
	
	public static void addAnnotation(GraphManager m, Object key, Object value, PronghornStage stage) {
		synchronized(m.lock) {	
			
			//if Annotation key already exists then replace previous value
			int annotationId = findAnnotationIdForKey(m, stage.stageId, key);
			
			if (-1 != annotationId) {
				//simple replace with new value
				m.annotationIdToValue[annotationId] = value;				
			} else {
				//extract as add annotation method.
				//even if the same key/value is given to multiple stages we add it multiple times here
				//this allows for direct lookup later for every instance found
				m.annotationIdToKey = setValue(m.annotationIdToKey, m.totalAnnotationCount, key);
				m.annotationIdToValue = setValue(m.annotationIdToValue, m.totalAnnotationCount, value);
				m.annotationIdToStageId = setValue(m.annotationIdToStageId, m.totalAnnotationCount, stage.stageId);
				
				int beginIdx = m.stageIdToAnnotationsBeginIdx[stage.stageId];
			    if (m.topAnnotation == m.multAnnotationIds.length) {
			    	//create new larger array		    	
			    	int[] newMultiAnnotationIdx = new int[m.multAnnotationIds.length*2];
			    	System.arraycopy(m.multAnnotationIds, 0, newMultiAnnotationIdx, 0, beginIdx);
			    	System.arraycopy(m.multAnnotationIds, beginIdx, newMultiAnnotationIdx, beginIdx+1, m.topAnnotation-(beginIdx));
			    	m.multAnnotationIds = newMultiAnnotationIdx;		    	
			    } else {
			    	//move all the data down.
			    	System.arraycopy(m.multAnnotationIds, beginIdx, m.multAnnotationIds, beginIdx+1, m.topAnnotation-(beginIdx));
			    }
			    
			    m.multAnnotationIds[beginIdx] = m.totalAnnotationCount;//before we inc this value is the index to this key/value pair
			    
				m.totalAnnotationCount++;
				m.topAnnotation++;
				
			}
			
		}
	}
	
	//also add get regex of key string
	
	private static int findAnnotationIdForKey(GraphManager m, int stageId, Object key) {
		int idx = m.stageIdToAnnotationsBeginIdx[stageId];
		int annotationId;
		while(-1 != (annotationId = m.multAnnotationIds[idx])) {
			if (m.annotationIdToKey[annotationId].equals(key)) {
				return annotationId;
			}
			idx++;
		}		
		return -1;
	}
	

	
	/**
	 * Returns annotation if one is found by this key on this stage, if Annotation is not found it returns the defaultValue
	 * @param m
	 * @param stage
	 * @param key
	 * @return
	 */
	public static Object getAnnotation(GraphManager m, PronghornStage stage, Object key, Object defaultValue) {
		int idx = m.stageIdToAnnotationsBeginIdx[stage.stageId];
		int annotationId;
		while(-1 != (annotationId = m.multAnnotationIds[idx])) {
			if (m.annotationIdToKey[annotationId].equals(key)) {
				return m.annotationIdToValue[annotationId];
			}
			idx++;
		}		
		return defaultValue;
	}
	
	public static void shutdownNeighborRings(GraphManager pm, PronghornStage baseStage) {
		
		int inputPos  = pm.stageIdToInputsBeginIdx[baseStage.stageId];
	    int outputPos =	pm.stageIdToOutputsBeginIdx[baseStage.stageId];		
		
	    int ringId;
	    
	    while ((ringId = pm.multInputIds[inputPos++])>=0) {
	    	RingBuffer.shutdown(pm.ringIdToRing[ringId]);	
	    }
	    
	    while ((ringId = pm.multOutputIds[outputPos++])>=0) {
		    RingBuffer.shutdown(pm.ringIdToRing[ringId]);	
		}
	}

	public static PronghornStage getStage(GraphManager m, int stageId) {
		return m.stageIdToStage[stageId];
	}
	
	public static RingBuffer getRing(GraphManager gm, int ringId) {
		return gm.ringIdToRing[ringId];
	}
	
	public static PronghornStage getRingProducer(GraphManager gm, int ringId) {
		return  gm.stageIdToStage[gm.ringIdToStages[ringId*2]];
	}
	
	public static PronghornStage getRingConsumer(GraphManager gm, int ringId) {
		return  gm.stageIdToStage[gm.ringIdToStages[(ringId*2)+1]];
	}
	


	
	/**
	 * Return false only when every path is checked so every ring is empty and every stage is terminated.
	 */
	public static boolean mayHaveUpstreamData(GraphManager m, int stageId) {
		
		if (2 == m.stageStateData.stageTerminationState[stageId]) { //terminated 
			return false;
		}
		
		
		int inputPos  = m.stageIdToInputsBeginIdx[stageId];
		int ringId;
		int inputCounts=0;
		    
		while ((ringId = m.multInputIds[inputPos++])>=0) {
							
				++inputCounts;
				
				//check that producer is terminated first.
				if (isProducerTerminated(m, ringId)) {
					//if producer is terminated check input ring, if not empty return true
			    	if (RingBuffer.contentRemaining( m.ringIdToRing[ringId])>0) {
			    		//return true because we found content sitting on a ring
			    		return true;
			    	}
				} else {
					return true;
				}				
		}
		
		//true if this is a top level stage and was not terminated at top of this function
		//all other cases return false because the recursive check has already completed above.
		return 0==inputCounts;
	}
	
	public static boolean isProducerTerminated(GraphManager m, int ringId) {
		return 2==m.stageStateData.stageTerminationState[m.ringIdToStages[ringId*2]];
	}

    public static boolean isStageTerminated(GraphManager m, int stageId) {
    	return 2==m.stageStateData.stageTerminationState[stageId];
    }

	public static void terminateInputStages(GraphManager m) {
				
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			if (null!=m.stageIdToStage[i]) {				
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInputsBeginIdx[m.stageIdToStage[i].stageId]]) {
					m.stageIdToStage[i].shutdown(); //TOOD: better error reporting here.
				}
			}
		}		
	}

	public static void logOutputs(Logger log, GraphManager m, PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToOutputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multOutputIds[idx++])) {					
			log.error("Output Queue :{}",m.ringIdToRing[ringId]);				
		}	
	}

	public static void logInputs(Logger log, GraphManager m,	PronghornStage stage) {
		int ringId;
		int idx = m.stageIdToInputsBeginIdx[stage.stageId];
		while (-1 != (ringId=m.multInputIds[idx++])) {	
			log.error("Input Queue :{}",m.ringIdToRing[ringId]);					
		}				
		
	}



	
	
	
	

}
