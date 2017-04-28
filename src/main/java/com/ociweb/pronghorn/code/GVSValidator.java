package com.ociweb.pronghorn.code;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface GVSValidator {

	TestFailureDetails validate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs);

	String status();

}
