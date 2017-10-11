package com.ociweb.pronghorn;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class TelemtryTestTool {

	public static void main(String[] args) {
	
			GraphManager gm = new GraphManager();
			GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 200_000);
			
			Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(10, 300);
			new ExampleProducerStage(gm, output);
			
			int i = 30;
			Pipe[] targets = new Pipe[i];
			while (--i>=0) {
				targets[i] = new Pipe(output.config().grow2x());
		        new PipeCleanerStage<>(gm, targets[i]);
			}
			new ReplicatorStage<>(gm, output, targets);
			
			
			gm.enableTelemetry(8092);
			StageScheduler.defaultScheduler(gm).startup();
	
	}

}
