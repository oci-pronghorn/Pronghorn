package com.ociweb.pronghorn;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class TelemetryTestTool {

	////////////////////////////////////////////////////////////
	//To run this from the command prompt:
	// mvn -Dexec.classpathScope=test -Dexec.addResourcesToClasspath=true test-compile exec:java -Dexec.mainClass="com.ociweb.pronghorn.TelemetryTestTool"
	///////////////////////////////////////////////////////////
	
	public static void main(String[] args) {
	
			launch(8092, 14, 7);			
			launch(8093, 50, 2);
			launch(8094, 4, 13);
					
	}

	private static void launch(int port, int width, int height) {
		GraphManager gm = new GraphManager();
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500_000);

		Pipe<RawDataSchema> output = RawDataSchema.instance.newPipe(8, 8);
		ExampleProducerStage producer = new ExampleProducerStage(gm, output);
					
		int c = Math.min(1, height);
		int b = height-c;
		
		int i = width;
		Pipe[] targets = new Pipe[i];
		while (--i>=0) {
			targets[i] = new Pipe(output.config().grow2x());
			Pipe temp = null;
			Pipe prev = targets[i];
			
			int k = b;
			while (--k>=0) {
				//only grow some of these because this will take up too much memory
				temp = new Pipe(k<5 ? prev.config().grow2x() : prev.config());
				//slow replicator so it batches
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, 
				new BatchingStage(gm, .90, prev, temp) );
				prev = temp;
			}
							
			
			int j = c; //replicators
			while (--j>=0) {
				temp = new Pipe(prev.config().grow2x());
				//slow replicator so it batches
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, 
				new ReplicatorStage(gm, prev, temp) );
				prev = temp;
			}
			GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 5_000_000, 
		    new PipeCleanerStage(gm, temp) );
		}			
		//slow replicator so it batches
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, 
		new ReplicatorStage<>(gm, output, targets) );
		gm.enableTelemetry(port);

		StageScheduler.defaultScheduler(gm).startup();
	}

}
