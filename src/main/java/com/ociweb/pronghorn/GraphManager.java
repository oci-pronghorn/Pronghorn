package com.ociweb.pronghorn;

import java.util.Arrays;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

public class GraphManager {
	
	private static int INIT_RINGS = 32;
	private static int INIT_STAGES = 32;
				
	//for lookup of source id and target id from the ring id
	private int[] ringIdToStages  = new int[INIT_RINGS*2]; //stores the sourceId and targetId for every ring id.
	
	//for lookup of input ring start pos from the stage id 
	private int[] stageIdToInput  = new int[INIT_STAGES];
	private int[] multInputIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topInput = 0;
	
	//for lookup of output ring start pos from the stage id
	private int[] stageIdToOutput = new int[INIT_STAGES];
	private int[] multOutputIds = new int[INIT_RINGS]; //a -1 marks the end of a run of values
	private int topOutput = 0;
	
	//for lookup of RingBuffer from RingBuffer id
	private RingBuffer[] ringIdToRing = new RingBuffer[INIT_RINGS];
	
	//for lookup of Stage from Stage id
	private PronghornStage[]  stageIdToStage = new PronghornStage[INIT_STAGES];
	private int[]             stageIdToRate  = new int[INIT_STAGES];
	
		
	//this is not tracking each time thread is running but instead the larger granularity of
	//the stage and if it has permanently terminated
	private byte[] stageTerminationState = new byte[0];// 0 - unknown, 1 - registered, 2 - terminated
	
	

	
	//this is never used as a runtime but only as a construction lock
	private Object lock = new Object();		
	
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
			int stageId = stage.stageId;
			
			gm.stageTerminationState = incValue(gm.stageTerminationState, stageId); //state changes from 0 to 1
			assert(1 == gm.stageTerminationState[stageId]);
			
			gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
			gm.stageIdToInput = setValue(gm.stageIdToInput, stageId, gm.topInput);
			gm.stageIdToOutput = setValue(gm.stageIdToOutput, stageId, gm.topOutput);
			
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
			
			gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
			gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		}
		
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer input, RingBuffer[] outputs) {
		synchronized(gm.lock) {
			int stageId = stage.stageId;
			
			gm.stageTerminationState = incValue(gm.stageTerminationState, stageId); //state changes from 0 to 1
			assert(1 == gm.stageTerminationState[stageId]);
			
			gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
			gm.stageIdToInput = setValue(gm.stageIdToInput, stageId, gm.topInput);
			gm.stageIdToOutput = setValue(gm.stageIdToOutput, stageId, gm.topOutput);
			
			//loop over inputs
			regInput(gm, input, stageId);
			
			int i;
			//loop over outputs
			i = outputs.length;
			while (--i>=0) {
				regOutput(gm, outputs[i], stageId);
			}
			
			gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
			gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer[] inputs, RingBuffer output) {
		synchronized(gm.lock) {
			int stageId = stage.stageId;
			
			gm.stageTerminationState = incValue(gm.stageTerminationState, stageId); //state changes from 0 to 1
			assert(1 == gm.stageTerminationState[stageId]);
			
			gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
			gm.stageIdToInput = setValue(gm.stageIdToInput, stageId, gm.topInput);
			gm.stageIdToOutput = setValue(gm.stageIdToOutput, stageId, gm.topOutput);
			
			int i;
			//loop over inputs
			i = inputs.length;
			while (--i>=0) {
				regInput(gm, inputs[i], stageId);
			}
			
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
			gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		}
	}


	public static void register(GraphManager gm, PronghornStage stage, RingBuffer input, RingBuffer output) {
		synchronized(gm.lock) {
			int stageId = stage.stageId;
					
			gm.stageTerminationState = incValue(gm.stageTerminationState, stageId); //state changes from 0 to 1
			assert(1 == gm.stageTerminationState[stageId]);
			
			gm.stageIdToStage = setValue(gm.stageIdToStage, stageId, stage);		
			gm.stageIdToInput = setValue(gm.stageIdToInput, stageId, gm.topInput);
			gm.stageIdToOutput = setValue(gm.stageIdToOutput, stageId, gm.topOutput);
			
			//loop over inputs
			regInput(gm, input, stageId);
	
			//loop over outputs
			regOutput(gm, output, stageId);			
			
			gm.multInputIds = setValue(gm.multInputIds, gm.topInput++, -1);
			gm.multOutputIds = setValue(gm.multOutputIds, gm.topOutput++, -1);
		}
	}
	
	public static void terminate(GraphManager gm, PronghornStage stage ) {
		synchronized(gm.lock) {
			gm.stageTerminationState = incValue(gm.stageTerminationState, stage.stageId); //state changes from 1 to 2
			if ( gm.stageTerminationState[stage.stageId]>2) {
				gm.stageTerminationState[stage.stageId] = 2;
			}
			assert(2 == gm.stageTerminationState[stage.stageId]);
		}
	}

	private static void regOutput(GraphManager pm, RingBuffer output,
			int stageId) {
		int outputId = output.ringId;
		pm.ringIdToStages = setValue(pm.ringIdToStages, (outputId*2) , stageId); //source +0 then target +1
		pm.ringIdToRing = setValue(pm.ringIdToRing, outputId, output);				
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, outputId);
	}

	private static void regInput(GraphManager pm, RingBuffer input,
			int stageId) {
		int inputId = input.ringId;
		pm.ringIdToStages = setValue(pm.ringIdToStages, (inputId*2)+1, stageId); //source +0 then target +1
		pm.ringIdToRing = setValue(pm.ringIdToRing, inputId, input);
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, inputId);
	}


	public static void shutdownNeighborRings(GraphManager pm, PronghornStage baseStage) {
				
		int inputPos  = pm.stageIdToInput[baseStage.stageId];
	    int outputPos =	pm.stageIdToOutput[baseStage.stageId];		
		
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
		
		if (2 == m.stageTerminationState[stageId]) { //terminated 
			return false;
		}
		
		
		int inputPos  = m.stageIdToInput[stageId];
		int ringId;
		int inputCounts=0;
		    
		while ((ringId = m.multInputIds[inputPos++])>=0) {
							
				++inputCounts;
				
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
		return 2==m.stageTerminationState[m.ringIdToStages[ringId*2]];
	}

    public static boolean isStageTerminated(GraphManager m, int stageId) {
    	return 2==m.stageTerminationState[stageId];
    }

	public static void terminateInputStages(GraphManager m) {
				
		int i = m.stageIdToStage.length;
		while (--i>=0) {
			if (null!=m.stageIdToStage[i]) {
				//an input stage is one that has no input ring buffers
				if (-1 == m.multInputIds[m.stageIdToInput[m.stageIdToStage[i].stageId]]) {
					m.stageIdToStage[i].shutdown();
				}
			}
		}
		
	}

	
	public static void setContinuousRun(GraphManager m, PronghornStage stage) {
		synchronized(m.lock) {			
			m.stageIdToRate = setValue(m.stageIdToRate,stage.stageId, 0);
		}
	}
	
	public static void setScheduleRate(GraphManager m, int nsScheduleRate, PronghornStage stage) {
		synchronized(m.lock) {			
			m.stageIdToRate = setValue(m.stageIdToRate,stage.stageId, nsScheduleRate);
		}
	}

	public static int getScheduleRate(GraphManager m, int stageId) {
		return m.stageIdToRate[stageId];
	}

	public static void setContinuousRun(GraphManager graphManager, PronghornStage ... stages) {
		int i = stages.length;
		while (--i>=0) {
			setScheduleRate(graphManager, 0, stages[i]);
		}
	}

	public static void setScheduleRate(GraphManager graphManager, int nsScheduleRate, PronghornStage ... stages) {
		int i = stages.length;
		while (--i>=0) {
			setScheduleRate(graphManager, nsScheduleRate, stages[i]);
		}
	}


	
	
	
	

}
