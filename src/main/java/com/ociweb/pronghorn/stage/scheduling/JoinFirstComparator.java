package com.ociweb.pronghorn.stage.scheduling;

import java.util.Comparator;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;

public class JoinFirstComparator implements Comparator<Pipe> {

	private final GraphManager graphManager;
	private final int[] weights;

	public JoinFirstComparator(GraphManager graphManager) {
		this.graphManager = graphManager;
		this.weights = new int[Pipe.totalPipes()];
	}

	@Override
	public int compare(Pipe p1, Pipe p2) {
		return Integer.compare(weight(p1),weight(p2));
	}

	public int weight(Pipe p) {

		if (weights[p.id]==0) {

			int result = (int)p.config().slabBits();

			//returns the max pipe length from this pipe or any of the pipes that feed its producer.
			//if this value turns out to be large then we should probably not join these two stages.

			int producerId = GraphManager.getRingProducerId(graphManager, p.id);
			if (producerId>=0) {
				PronghornStage producer = GraphManager.getStage(graphManager, producerId);
				int count = GraphManager.getInputPipeCount(graphManager, producer);
				while (--count>=0) {
					Pipe inputPipe = GraphManager.getInputPipe(graphManager, producer, count+1);
					result = Math.max(result, inputPipe.config().slabBits());
				}
			} else {
				//no producer found, an external thread must be pushing data into this, there is nothing to combine it with
			}
			weights[p.id] = result;
		}
		return weights[p.id];

	}

}