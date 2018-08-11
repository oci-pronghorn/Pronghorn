package com.ociweb.pronghorn.neural;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class NeuralGraphBuilder {

   //hash each word and ad them each to an input pipe 20is?
   //group stages into single stage with no thread scheudler to do one layer at a time
   //build pipes for both directions and feedback.
	
	
	
	public static <T extends MessageSchema<T>> Pipe<T>[][] buildPipeLayer(
			 GraphManager gm, PipeConfig<T> config,
			 Pipe<T>[] prevPipes, int nextNodesCount, StageFactory<T> factory) {
			
		final int thisNodesCount = prevPipes.length;
		
		Pipe<T>[][] fromStages = (Pipe<T>[][])new Pipe[thisNodesCount][nextNodesCount];
		int m = thisNodesCount;
		while (--m>=0) {
			fromStages[m] = new Pipe[nextNodesCount];
		}
		
		Pipe<T>[][] fromStageGroupedByNext = (Pipe<T>[][])new Pipe[nextNodesCount][thisNodesCount];
		int n = nextNodesCount;
		while (--n>=0) {
			fromStageGroupedByNext[n] = Pipe.buildPipes(thisNodesCount, config);
			int p = thisNodesCount;
			while (--p>=0) {
				fromStages[p][n] = fromStageGroupedByNext[n][p];
			}
		}
		
		//inputs are grouped by node1
		
		//create stages that write to inputs and take previous as argument to method
		int p = prevPipes.length;
		while (--p>=0) {			
			factory.newStage(gm, prevPipes[p], fromStages[p]);
			
		}
		
		return fromStageGroupedByNext; //grouped by nodes2
		
		//return Pipe.buildPipes(inputs, pipeConfig);				
	}
	
	public static <T extends MessageSchema<T>> Pipe<T>[][] buildPipeLayer(
			GraphManager gm, PipeConfig<T> config, Pipe<T>[][] prev, int nodes2, StageFactory<T> factory) {
		
		int nodes1 = prev.length;
		
		Pipe<T>[][] inputs = (Pipe<T>[][])new Pipe[nodes1][nodes2];
		int m = nodes1;
		while (--m>=0) {
			inputs[m] = new Pipe[nodes2];
		}
		
		Pipe<T>[][] outputs = (Pipe<T>[][])new Pipe[nodes2][nodes1];
		int n = nodes2;
		while (--n>=0) {
			outputs[n] = Pipe.buildPipes(nodes1, config);
			int p = nodes1;
			while (--p>=0) {
				inputs[p][n] = outputs[n][p];
			}
		}
		
		//inputs are grouped by node1
		
		//create stages that write to inputs and take previous as argument to method
		int p = prev.length;
		while (--p>=0) {			
			factory.newStage(gm, prev[p], inputs[p]);
			
		}
		
		return outputs; //grouped by nodes2
		
		//return Pipe.buildPipes(inputs, pipeConfig);				
	}
	
	//NOTE: build inputs with  Pipe.buildPipes
	
	public static <T extends MessageSchema<T>> Pipe<T>[] lastPipeLayer(
			           GraphManager gm, Pipe<T>[][] prev, StageFactory<T> factory) {
			
		int p = prev.length;
		Pipe<T>[] outputs = Pipe.buildPipes(p, prev[0][0].config());
		while (--p>=0) {			
			factory.newStage(gm, prev[p], outputs[p]);
		}
		return outputs;
	}

	
}
