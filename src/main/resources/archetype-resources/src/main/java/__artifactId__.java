package ${package};

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.FileBlobReadStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.MainArgs;

public class ${artifactId}  {

	public static void main(String[] args) {

	    //Get the file path from the arguments
		String inputFilePath = MainArgs.getOptArg("fileName", "-f", args, "./image.jpg");

		//Create a new GraphManager. The GraphManager is essential and keeps track of stages (nodes) and pipes (edges).
		GraphManager gm = new GraphManager();
		
		populateGraph(gm, inputFilePath);

		//By default, telemetry will be accessible on your public IP : given port
		gm.enableTelemetry(8089);

		//Actually start the default scheduler.
		StageScheduler.defaultScheduler(gm).startup();
	}

    private static void populateGraph(GraphManager gm, String inputFilePath) {

        //Create 3 pipes uing the RawDataSchema
        //RawDataSchema has one record, with one variable-length field, that allows you to put bytes in, ideal for
        //file streaming
		Pipe<RawDataSchema> pipe1 = RawDataSchema.instance.newPipe(10, 10_000); //10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1A = RawDataSchema.instance.newPipe(20, 20_000); //10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1B = RawDataSchema.instance.newPipe(20, 20_000); //10 chunks each 10K in  size
		
		new FileBlobReadStage(gm, pipe1, inputFilePath); //This stage reads a file
		
		//This stage replicates the data to two pipes, great for debugging while passing on the real data.
		new ReplicatorStage<>(gm, pipe1, pipe1A, pipe1B);

        //see all the data at the console.
		new ConsoleJSONDumpStage(gm, pipe1A);

        //dumps all data which came in
		new PipeCleanerStage(gm, pipe1B);
		
	}
          
}
