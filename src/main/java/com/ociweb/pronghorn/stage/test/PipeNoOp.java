package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PipeNoOp<T extends MessageSchema<T>> extends PronghornStage  {

    public static <T extends MessageSchema<T>> PipeNoOp<T> newInstance(GraphManager gm, Pipe<T> pipe) {
        return new PipeNoOp<T>(gm, pipe);
    }
	
	protected PipeNoOp(GraphManager graphManager, Pipe<T> output) {
		super(graphManager, NONE, output);
		GraphManager.addNota(graphManager, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, this);
	}

	@Override
	public void run() {
	}

}
