package com.ociweb.pronghorn.code;

import java.util.Random;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public interface GGSGenerator {

	boolean generate(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, Random random);

}
