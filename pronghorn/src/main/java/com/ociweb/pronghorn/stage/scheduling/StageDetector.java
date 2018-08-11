package com.ociweb.pronghorn.stage.scheduling;

public class StageDetector implements GraphVisitor {

	private int target;
	private boolean detected;
	private int producerStageId;
	
	@Override
	public boolean visit(GraphManager graphManager, int stageId, int depth) {
		
		if (null!=GraphManager.getNota(graphManager, stageId, GraphManager.PRODUCER, null)) {
			producerStageId = stageId;
		}
		
		detected |= (stageId == target);
		return !detected;//keep going if not found
	}

	public void setTarget(GraphManager graphManager, int stageId) {
		target = stageId;
		detected = false;
		
		if (null!=GraphManager.getNota(graphManager, stageId, GraphManager.PRODUCER, null)) {
			producerStageId = stageId;
		} else {
			producerStageId = -1;
		}
	}

	public int foundProducerStageId() {
		return producerStageId;
	}
	
	public boolean wasDetected() {
		return detected;
	}

}
