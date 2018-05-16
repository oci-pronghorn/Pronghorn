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

	// Create a new GraphManager. The GraphManager is essential and keeps track of stages (nodes) and pipes (edges).
	final GraphManager gm = new GraphManager();

	/**
	 * Main entry point that creates an instance of ${artifactId} and runs it.
	 */
	public static void main(String[] args) {

	    // Get the file path from the arguments
		String inputFilePath = MainArgs.getOptArg("fileName", "-f", args, "./image.jpg");

		// Create a new ${artifactId} instance, put the output stream on the system log
		${artifactId} program = new ${artifactId}(inputFilePath, System.out);

		program.startup();

	}

	/**
	 * Constructor for ${artifactId}
	 * @param inputFilePath The input path for the file to be logged
	 *        out An appendable in which the specified file gets written to
	 */
	public ${artifactId} (String inputFilePath, Appendable out) {

		// Add edges and pipes
		populateGraph(gm, inputFilePath, out);

		// Turning on the telemetry web page is as simple as adding this line
		// It will be accessible on the most public IP it can find by default
		gm.enableTelemetry(7777);

	}

	/**
	 * Use the default scheduler with the passed in GraphManager to start
	 */
	public void startup() {

		StageScheduler.defaultScheduler(gm).startup();

	}

	/**
	 * Creates the pipes and the 4 stages (FileBlobReadStage, ReplicatorStage, ConsoleJSONDumpStage, and PipeCleanerStage)
	 */
	private static void populateGraph(GraphManager gm, String inputFilePath, Appendable out) {

        // Create 3 pipes using the RawDataSchema
        // RawDataSchema has one record, with one variable-length field, that allows you to put bytes in, ideal for
        // file streaming
		Pipe<RawDataSchema> pipe1 = RawDataSchema.instance.newPipe(10, 10_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1A = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1B = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size
		
		new FileBlobReadStage(gm, pipe1, inputFilePath); // This stage reads a file
		
		// This stage replicates the data to two pipes, great for debugging while passing on the real data
		new ReplicatorStage<>(gm, pipe1, pipe1A, pipe1B);

        // See all the data in the console
		new ConsoleJSONDumpStage(gm, pipe1A);

        // Dumps all data which came in
		new PipeCleanerStage(gm, pipe1B);
		
	}
          
}
