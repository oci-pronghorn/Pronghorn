package ${package};

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import org.junit.jupiter.api.Test;
import com.ociweb.${artifactId};

class ${artifactId}Test {

    /**
     * Creates a new demo program and starts it using the default scheduler.
     * @result The string "ChunkedStream" appears in the output,
     *         and the volume of the pipe going of the file reader stage is the same
     *         as the volume going into the console JSON dump stage.
     */
    @Test
    void checkPopulateGraphAndResponse() {

        // Use a StringBuilder as Appendable so we can do an indexOf on the output to assert that a keyword exists
        StringBuilder sb = new StringBuilder();
        ${artifactId} program = new ${artifactId}("./image.jpg", 7778, sb);

        // Start the scheduler
        program.startup();

        // Fetch the stage based on the class
        PronghornStage fileBlobReadStage = GraphManager.allStagesByType(program.gm, FileBlobReadStage.class)[0];
        PronghornStage consoleJSONDumpStage = GraphManager.allStagesByType(program.gm, ConsoleJSONDumpStage.class)[0];

        // We are blocking until JSON output is done
        GraphManager.blockUntilStageTerminated(program.gm, consoleJSONDumpStage);

        // Run our final assertions
        assertAll(
                // Assert that we receive the string "ChunkedStream" in our JSON response
                () -> assertNotSame(-1, sb.indexOf("ChunkedStream")),

                // Assert that the volume of our pipe in the beginning of the graph is the same as the volume at the end of the graph
                () -> assertSame(Pipe.totalWrittenFragments(GraphManager.getOutputPipe(program.gm, fileBlobReadStage)),
                        Pipe.totalWrittenFragments(GraphManager.getInputPipe(program.gm, consoleJSONDumpStage)))
        );

    }

}
