package com.ociweb.pronghorn.stage.generator;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.*;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.IntegrityFuzzGenerator;
import com.ociweb.pronghorn.stage.IntegrityTestFuzzConsumer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import org.junit.Assert;
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
 * Created by jake on 10/29/16.
 */
public class EncodeDecodeRuntimeTest {
    @Test
    public void compileTest() throws IOException, SAXException, ParserConfigurationException {
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/template/integrityTest.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);

        StringBuilder target = new StringBuilder();
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(messageSchema, target, false);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }


        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target, PhastDecoderStageGenerator.class);

        StringBuilder target2 = new StringBuilder();
        PhastEncoderStageGenerator encoder = new PhastEncoderStageGenerator(messageSchema, target2);

        try {
            encoder.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }


        validateCleanCompile(encoder.getPackageName(), encoder.getClassName(), target2, PhastEncoderStageGenerator.class);

    }

    @Test
    public void runTimeTest() throws IOException, SAXException, ParserConfigurationException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
        GraphManager gm = new GraphManager();

        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/template/integrityTest.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<MessageSchemaDynamic> inPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        inPipe.initBuffers();
        Pipe<RawDataSchema> sharedPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));
        sharedPipe.initBuffers();
        Pipe<MessageSchemaDynamic> outPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        outPipe.initBuffers();
        //get encoder ready
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
        StringBuilder dTarget = new StringBuilder();
        PhastDecoderStageGenerator dw = new PhastDecoderStageGenerator(messageSchema, dTarget, false);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        Constructor dconstructor =  LoaderUtil.generateThreeArgConstructor(dw.getPackageName(), dw.getClassName(), dTarget, PhastDecoderStageGenerator.class);

        IntegrityFuzzGenerator random1 = new IntegrityFuzzGenerator(gm, inPipe);
        econstructor.newInstance(gm, inPipe, sharedPipe);
        dconstructor.newInstance(gm, sharedPipe, outPipe);
        StringBuilder result = new StringBuilder();
        IntegrityTestFuzzConsumer consumer = new IntegrityTestFuzzConsumer(gm, outPipe, result);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();


        //GraphManager.blockUntilStageBeginsShutdown(gm,json);
        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue("44633,2,42363,41696,15806,2,35054,46050".equals(result.toString()));

    }

    private static void validateCleanCompile(String packageName, String className, StringBuilder target, Class clazz) {
        try {

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, clazz);

            if (generateClass.isAssignableFrom(PronghornStage.class)) {
                Constructor constructor =  generateClass.getConstructor(GraphManager.class, Pipe.class, Pipe.class);
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
}
