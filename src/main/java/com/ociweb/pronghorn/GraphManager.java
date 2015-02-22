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


	public static void register(GraphManager pm, PronghornStage baseStage, RingBuffer[] inputs, RingBuffer[] outputs) {
		
		
		int stageId = baseStage.stageId;
		
		pm.stageIdToStage = setValue(pm.stageIdToStage, stageId, baseStage);		
		pm.stageIdToInput = setValue(pm.stageIdToInput, stageId, pm.topInput);
		pm.stageIdToOutput = setValue(pm.stageIdToOutput, stageId, pm.topOutput);
		
		int i;
		//loop over inputs
		i = inputs.length;
		while (--i>=0) {
			regInput(pm, inputs[i], stageId);
		}
		
		//loop over outputs
		i = outputs.length;
		while (--i>=0) {
			regOutput(pm, outputs[i], stageId);
		}
		
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, -1);
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, -1);
		
		
	}


	public static void register(GraphManager pm, PronghornStage baseStage, RingBuffer input, RingBuffer[] outputs) {
		
		int stageId = baseStage.stageId;
		
		pm.stageIdToStage = setValue(pm.stageIdToStage, stageId, baseStage);		
		pm.stageIdToInput = setValue(pm.stageIdToInput, stageId, pm.topInput);
		pm.stageIdToOutput = setValue(pm.stageIdToOutput, stageId, pm.topOutput);
		
		//loop over inputs
		regInput(pm, input, stageId);
		
		int i;
		//loop over outputs
		i = outputs.length;
		while (--i>=0) {
			regOutput(pm, outputs[i], stageId);
		}
		
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, -1);
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, -1);
		
	}


	public static void register(GraphManager pm, PronghornStage baseStage, RingBuffer[] inputs, RingBuffer output) {
		
		int stageId = baseStage.stageId;
		
		pm.stageIdToStage = setValue(pm.stageIdToStage, stageId, baseStage);		
		pm.stageIdToInput = setValue(pm.stageIdToInput, stageId, pm.topInput);
		pm.stageIdToOutput = setValue(pm.stageIdToOutput, stageId, pm.topOutput);
		
		int i;
		//loop over inputs
		i = inputs.length;
		while (--i>=0) {
			regInput(pm, inputs[i], stageId);
		}
		
		//loop over outputs
		regOutput(pm, output, stageId);			
		
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, -1);
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, -1);
		
	}


	public static void register(GraphManager pm, PronghornStage baseStage, RingBuffer input, RingBuffer output) {
		
		int stageId = baseStage.stageId;
				
		pm.stageIdToStage = setValue(pm.stageIdToStage, stageId, baseStage);		
		pm.stageIdToInput = setValue(pm.stageIdToInput, stageId, pm.topInput);
		pm.stageIdToOutput = setValue(pm.stageIdToOutput, stageId, pm.topOutput);
		
		//loop over inputs
		regInput(pm, input, stageId);

		//loop over outputs
		regOutput(pm, output, stageId);			
		
		pm.multInputIds = setValue(pm.multInputIds, pm.topInput++, -1);
		pm.multOutputIds = setValue(pm.multOutputIds, pm.topOutput++, -1);
				
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


	public static void shutdownNeighbors(GraphManager pm, PronghornStage baseStage) {
				
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
	 * Walks all the way back up the tree until every source is reached or a ring with content is encountered.
	 * This is a slow method that should not be called at runtime but it is very helpful for clean shutdowns.
	 */
	public static boolean hasUpstreamData(GraphManager m, int stageId) {
		int inputPos  = m.stageIdToInput[stageId];
		int ringId;
		    
		while ((ringId = m.multInputIds[inputPos++])>=0) {
									
		    	if (RingBuffer.contentRemaining( m.ringIdToRing[ringId])>0) {
		    		return true;
		    	}
		    	
		    	//if the producer stage has data
		    	if (hasUpstreamData(m, m.ringIdToStages[ringId*2])) {
		    		return true;
		    	}
		}
		
		
		return false;
	}




	
	
	
	

}
