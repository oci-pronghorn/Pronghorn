package ${package};

/**
 * *******************************************************************
 * For pronghorn support, training or feature reqeusts please contact:
 *   info@objectcomputing.com   (314) 579-0066
 * *******************************************************************
 */

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.FileBlobReadStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.MainArgs;

public class ${mainClass}  {

	// Create a new GraphManager. The GraphManager is essential and keeps track of stages (nodes) and pipes (edges).
	final GraphManager gm = new GraphManager();

	/**
	 * Main entry point that creates an instance of ${artifactId} and starts it.
	 * @param args Arguments for starting main
	 */
	public static void main(String[] args) {

	    // Get the file path from the arguments
		String inputFilePath = MainArgs.getOptArg("fileName", "-f", args, "./image.jpg");

		// Create a new ${artifactId} instance, put the output stream on the system log
		${mainClass} program = new ${mainClass}(inputFilePath, 7777, System.out);

		program.startup();

	}

	/**
	 * Constructor for ${artifactId}
	 * @param inputFilePath The input path for the file to be logged
	 * @param port The port on which telemetry will run
	 * @param out An appendable in which the specified file gets written to
	 */
	public ${mainClass} (String inputFilePath, int port, Appendable out) {

		// Add edges and pipes
		populateGraph(gm, inputFilePath, out);

		// Turning on the telemetry web page is as simple as adding this line
		// It will be accessible on the most public IP it can find by default
		gm.enableTelemetry(port);

	}

	/**
	 * Use the default scheduler with the passed in GraphManager to start
	 */
	void startup() {

		StageScheduler.defaultScheduler(gm).startup();

	}

	/**
	 * Creates the pipes and the 4 stages (FileBlobReadStage, ReplicatorStage, ConsoleJSONDumpStage, and PipeCleanerStage)
	 * @param gm The current graph manager
	 * @param inputFilePath The path to the file that will be passed through the pipes
	 * @param out The appendable in which the console log will be written into (i.e. System.out)
	 */
	private static void populateGraph(GraphManager gm, String inputFilePath, Appendable out) {

        // Create 3 pipes using the RawDataSchema
        // RawDataSchema has one record, with one variable-length field, that allows you to put bytes in, ideal for
        // file streaming
		Pipe<RawDataSchema> pipe1 = RawDataSchema.instance.newPipe(10, 10_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1A = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1B = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size

		// This stage reads a file
		FileBlobReadStage.newInstance(gm, pipe1, inputFilePath);

		// This stage replicates the data to two pipes, great for debugging while passing on the real data
		ReplicatorStage.newInstance(gm, pipe1, pipe1A, pipe1B);

		// See all the data in the console
		ConsoleJSONDumpStage.newInstance(gm, pipe1A, out);

		// Dumps all data which came in
		PipeCleanerStage.newInstance(gm, pipe1B);
		
	}
          
}
