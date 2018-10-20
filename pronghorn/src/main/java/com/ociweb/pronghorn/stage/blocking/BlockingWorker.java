package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public interface BlockingWorker {//move to PH??

	void doWork(Pipe<RawDataSchema> input, Pipe<RawDataSchema> output);

}
