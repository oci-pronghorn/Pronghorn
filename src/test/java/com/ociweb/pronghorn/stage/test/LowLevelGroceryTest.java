package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.*;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
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
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(schema, eTarget, "com.ociweb.pronghorn.pipe.build");
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
/*
    @Test
    public void integrityTest() throws IOException, SAXException, ParserConfigurationException {
        StringBuilder eTarget = new StringBuilder();

        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchema schema = new MessageSchemaDynamic(from);

        //decoder generator compile test
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(schema, eTarget, "com.ociweb.pronghorn.pipe.build");
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }


        //encoder generator compile test
        StringBuilder dTarget = new StringBuilder();
        PhastEncoderStageGenerator dw = new PhastEncoderStageGenerator(schema, dTarget);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), eTarget, PhastEncoderStageGenerator.class);


            GraphManager gm = new GraphManager();

            Pipe<?> pipe = new Pipe<>(new PipeConfig<>(schema, 8000));

            constructor.newInstance(gm, pipe);
            Appendable out = new PrintWriter(new ByteArrayOutputStream());
            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe, out );

            GraphManager.enableBatching(gm);
            //     MonitorConsoleStage.attach(gm);

            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.playNice=false;
            scheduler.startup();

            Thread.sleep(100);

            scheduler.shutdown();
            scheduler.awaitTermination(10, TimeUnit.SECONDS);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
*/

}