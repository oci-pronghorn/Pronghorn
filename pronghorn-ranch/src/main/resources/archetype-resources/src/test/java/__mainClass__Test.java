package ${package};

import static org.junit.jupiter.api.Assertions.*;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.FileBlobReadStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import org.junit.jupiter.api.Test;
import com.ociweb.${artifactId};

import java.io.IOException;

class ${mainClass}Test {

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
        ${mainClass} program = new ${mainClass}("./image.jpg", 7778, sb);

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

    /** Non-scheduled tests
     * The tests below do not use a scheduler but demonstrate how to write directly to pipes using the low-level
     * and high-level APIs. This is useful for writing your own tests for your Pronghorn application.
     */

    /**
     * Creates a new GraphManager and demonstrates how to write to a pipe directly using low-level API
     * to verify JSON dumper.
     */
    @Test
    void checkLowAPIConsoleJSONDumpFromPrepopulatedPipeData() throws IOException {

        GraphManager gm = new GraphManager();

        // Create the pipe that we will write to
        Pipe<SchemaOneSchema> inputPipe = SchemaOneSchema.instance.newPipe(50, 500);

        /* --------- Writing to inputPipe --------- */

        inputPipe.initBuffers(); // this is always required on a new pipe!

        // First, assert that we have room to actually write
        assertTrue(Pipe.hasRoomForWrite(inputPipe));

        // Get the size of the schema for the inputPipe
        int size = Pipe.addMsgIdx(inputPipe, SchemaOneSchema.MSG_SOMEOTHERMESSAGE_2);

        // Write values to the pipe. Since we are using the low-level API, these need to be written in-order!
        Pipe.addIntValue(1000, inputPipe);
        Pipe.addLongValue(500000, inputPipe);

        // Confirm & publish
        Pipe.confirmLowLevelWrite(inputPipe, size);
        Pipe.publishWrites(inputPipe);

        // Send the poison pill
        Pipe.publishEOF(inputPipe);

        /* --------- Example stage that dumps inputPipe and writes to StringBuilder --------- */

        StringBuilder sb = new StringBuilder();

        PronghornStage consoleJSONDumpStage = ConsoleJSONDumpStage.newInstance(gm, inputPipe, sb);

        consoleJSONDumpStage.startup();
        consoleJSONDumpStage.run();
        consoleJSONDumpStage.shutdown();

        // Wait until consoleJSONDumpStage is done
        GraphManager.blockUntilStageTerminated(gm, consoleJSONDumpStage);

        /* --------- Assert that the message is correctly being dumped --------- */

        assertEquals("{\"SomeOtherMessage\":  {\"ASignedInt\":1000}  {\"ASignedLong\":500000}}\n", sb.toString());

    }

    /**
     * Creates a new GraphManager and demonstrates how to write to a pipe directly using high-level API
     * to verify JSON dumper.
     */
    @Test
    void checkHiAPIConsoleJSONDumpFromPrepopulatedPipeData() throws IOException {

        GraphManager gm = new GraphManager();

        // Create the pipe that we will write to
        Pipe<SchemaOneSchema> inputPipe = SchemaOneSchema.instance.newPipe(50, 500);

        /* --------- Writing to inputPipe --------- */

        inputPipe.initBuffers(); // this is always required on a new pipe!

        //Do not mix High and low!
        if(PipeWriter.tryWriteFragment(inputPipe, SchemaOneSchema.MSG_SOMEOTHERMESSAGE_2)) {

            // Assign value to your fields defined in your schema
            // This is where the hi-level API shine - assign in any order, plus readable field names.
            PipeWriter.writeLong(inputPipe, SchemaOneSchema.MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDLONG_202, 500000);
            PipeWriter.writeInt(inputPipe, SchemaOneSchema.MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDINT_103, 1000);

            // Publish the results
            PipeWriter.publishWrites(inputPipe);

            // Send the poison pill
            PipeWriter.publishEOF(inputPipe);

        } else {

            fail("There was no room in the pipe for a write in hi-level");

        }

        /* --------- Example stage that dumps inputPipe and writes to StringBuilder --------- */

        StringBuilder sb = new StringBuilder();

        PronghornStage consoleJSONDumpStage = ConsoleJSONDumpStage.newInstance(gm, inputPipe, sb);

        consoleJSONDumpStage.startup();
        consoleJSONDumpStage.run();
        consoleJSONDumpStage.shutdown();

        // Wait until consoleJSONDumpStage is done
        GraphManager.blockUntilStageTerminated(gm, consoleJSONDumpStage);

        /* --------- Assert that the message is correctly being dumped --------- */

        assertEquals("{\"SomeOtherMessage\":  {\"ASignedInt\":1000}  {\"ASignedLong\":500000}}\n", sb.toString());

    }

}
