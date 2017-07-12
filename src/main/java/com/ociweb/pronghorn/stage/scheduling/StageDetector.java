package com.ociweb.pronghorn.stage.scheduling;

public class StageDetector implements GraphVisitor {

	private int target;
	private boolean detected;
	
	@Override
	public boolean visit(GraphManager graphManager, int stageId, int depth) {
		detected |= (stageId == target);
		return !detected;//keep going if not found
	}

	public void setTarget(int stageId) {
		target = stageId;
		detected = false;
	}

	public boolean wasDetected() {
		return detected;
	}

}
