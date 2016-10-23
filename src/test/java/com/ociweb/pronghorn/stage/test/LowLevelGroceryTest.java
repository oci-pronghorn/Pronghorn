package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.*;
import com.ociweb.pronghorn.pipe.build.GroceryExampleDecoderStage;
import com.ociweb.pronghorn.pipe.build.GroceryExampleEncoderStage;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.RandomWriterStage;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
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

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, PhastEncoderStageGenerator.class);

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

    @Test
    public void runTimeTest() throws IOException, ParserConfigurationException, SAXException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException {
        GraphManager gm = new GraphManager();

        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<MessageSchemaDynamic> inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        inPipe.initBuffers();
        Pipe<RawDataSchema> sharedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));
        sharedPipe.initBuffers();
        Pipe<MessageSchemaDynamic> outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        outPipe.initBuffers();

        //get encoder ready
        /*
        StringBuilder eTarget = new StringBuilder();
        PhastEncoderStageGenerator ew = new PhastEncoderStageGenerator(messageSchema, eTarget);
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        Constructor econstructor =  LoaderUtil.generateThreeArgConstructor(ew.getPackageName(), ew.getClassName(), eTarget, PhastEncoderStageGenerator.class);

        //get decoder ready
        /*
        StringBuilder dTarget = new StringBuilder();
        PhastDecoderStageGenerator dw = new PhastDecoderStageGenerator(messageSchema, dTarget, false);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        Constructor dconstructor =  LoaderUtil.generateThreeArgConstructor(dw.getPackageName(), dw.getClassName(), dTarget, PhastDecoderStageGenerator.class);
        */
        GroceryExampleDecoderStage dconstructor = new GroceryExampleDecoderStage(gm,sharedPipe,outPipe);
        GroceryExampleEncoderStage econstructor = new GroceryExampleEncoderStage(gm, inPipe,sharedPipe);
        RandomWriterStage random1 = new RandomWriterStage(gm, inPipe);
        //econstructor.newInstance(gm, inPipe, sharedPipe);
        //dconstructor.newInstance(gm, sharedPipe, outPipe);
        //RandomReaderStage rand2 = new RandomReaderStage(gm, outPipe);
        ConsoleJSONDumpStage json = new ConsoleJSONDumpStage(gm, outPipe);

        //encoding data
        //GraphManager.enableBatching(gm);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        //scheduler.playNice=false;
        scheduler.startup();


        GraphManager.blockUntilStageBeginsShutdown(gm,json);
        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);



    }
}