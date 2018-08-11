package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface PronghornStageProcessor {

	public void process(GraphManager gm, PronghornStage stage);
}
