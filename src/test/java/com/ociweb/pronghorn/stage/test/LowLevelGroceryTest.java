package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.*;
import com.ociweb.pronghorn.pipe.build.GroceryExampleWriterStage;
import com.ociweb.pronghorn.pipe.build.LowLevelReader;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.phast.PhastDecoder;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by jake on 9/19/16.
 */
public class LowLevelGroceryTest {

    @Test
    public void compileTest() throws IOException, SAXException, ParserConfigurationException {
        StringBuilder eTarget = new StringBuilder();

        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchema schema = new MessageSchemaDynamic(from);

        //decoder generator compile test
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(schema, eTarget, false);
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), eTarget);


        //encoder generator compile test
        StringBuilder dTarget = new StringBuilder();
        PhastEncoderStageGenerator dw = new PhastEncoderStageGenerator(schema, dTarget);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        validateCleanCompile(dw.getPackageName(), dw.getClassName(), dTarget);

    }

    private static void validateCleanCompile(String packageName, String className, StringBuilder target) {
        try {

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, FuzzDataStageGenerator.class);

            if (generateClass.isAssignableFrom(PronghornStage.class)) {
                Constructor constructor = generateClass.getConstructor(GraphManager.class, Pipe.class);
                assertNotNull(constructor);
            }

        } catch (ClassNotFoundException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }

    }


    //@Test
    public void runTimeReaderTest() throws IOException, SAXException, ParserConfigurationException {
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<MessageSchemaDynamic> pipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        pipe.initBuffers();
        PipeWriter.tryWriteFragment(pipe,1);
        Pipe.publishWrites(pipe);

        //LowLevelReader llr = new LowLevelReader(pipe);
        //llr.run();

    }
    //something wrong with delta int, looking into it.
    //@Test
    public void runtimeWriterTest() throws IOException, ParserConfigurationException, SAXException {
        GraphManager gm = new GraphManager();
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<MessageSchemaDynamic> pipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        pipe.initBuffers();

        //putting data to decode onto pipe
        DataOutputBlobWriter<MessageSchemaDynamic> writer = new DataOutputBlobWriter<MessageSchemaDynamic>(pipe);
        //putting pmap
        PhastEncoder.encodeLongPresent(writer,0,0,9);
        PhastEncoder.encodeIntPresent(writer,0,0,16);
        PhastEncoder.encodeLongPresent(writer,0,0,31);
        PhastEncoder.encodeString(writer, new StringBuilder("the first test"),0,0);
        PhastEncoder.encodeIntPresent(writer,0,0,25);
        PhastEncoder.encodeString(writer, new StringBuilder("The second test"),0,0);
        writer.close();


        GroceryExampleWriterStage writerStage = new GroceryExampleWriterStage(gm, pipe);

        writerStage.run();
    }
}