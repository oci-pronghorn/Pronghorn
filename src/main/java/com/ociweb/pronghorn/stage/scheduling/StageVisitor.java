package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.stage.PronghornStage;

public interface StageVisitor {

	void visit(PronghornStage stage);

}
