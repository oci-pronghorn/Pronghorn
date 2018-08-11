package com.ociweb.pronghorn.neural;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface StageFactory<T extends MessageSchema<T>> {

	void newStage(GraphManager gm, Pipe<T>[] input, Pipe<T> output);
    void newStage(GraphManager gm, Pipe<T> input, Pipe<T>[] output);
	void newStage(GraphManager gm, Pipe<T>[] input, Pipe<T>[] output);
	
}
