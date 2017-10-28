package com.ociweb.pronghorn;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class TelemetryTestTool {

	public static void main(String[] args) {
	
			GraphManager gm = new GraphManager();
			GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 100_000);

			Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(8, 8);
			new ExampleProducerStage(gm, output);
						
			int i = 100;
			Pipe[] targets = new Pipe[i];
			while (--i>=0) {
				targets[i] = new Pipe(output.config().grow2x());
				Pipe temp = null;
				Pipe prev = targets[i];
				
				int k = 10; //batching stage
				while (--k>=0) {
					temp = new Pipe(prev.config().grow2x());
					//slow replicator so it batches
					GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000, 
					new BatchingStage(gm, .90, prev, temp) );
					prev = temp;
				}
								
				
				int j = 3; //replicators
				while (--j>=0) {
					temp = new Pipe(prev.config().grow2x());
					//slow replicator so it batches
					GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, 
					new ReplicatorStage(gm, prev, temp) );
					prev = temp;
				}
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 2_000_000, 
		        new PipeCleanerStage(gm, temp) );
			}			
			//slow replicator so it batches
			GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, 
			new ReplicatorStage<>(gm, output, targets) );
					
			gm.enableTelemetry(8092);
						
			StageScheduler.defaultScheduler(gm).startup();
	
	}

}
