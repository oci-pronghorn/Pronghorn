package com.ociweb.pronghorn;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class TelemtryTestTool {

	public static void main(String[] args) {
	
			GraphManager gm = new GraphManager();
			GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 200_000);

			Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(2, 8);
			new ExampleProducerStage(gm, output);
			
			int i = 40;//100;
			Pipe[] targets = new Pipe[i];
			while (--i>=0) {
				targets[i] = new Pipe(output.config().grow2x());
				Pipe temp = null;
				Pipe prev = targets[i];
				
				int j = 3;//9;
				while (--j>=0) {
					temp = new Pipe(prev.config().grow2x());
					new ReplicatorStage<>(gm, prev, temp);
					prev = temp;
				}
				
		        new PipeCleanerStage<>(gm, temp);
			}			
			new ReplicatorStage<>(gm, output, targets);
					
			gm.enableTelemetry(8092);
			
			StageScheduler.defaultScheduler(gm).startup();
	
	}

}
