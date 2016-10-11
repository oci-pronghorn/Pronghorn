package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.*;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastDecoderStageGenerator;
import com.ociweb.pronghorn.stage.generator.PhastEncoderStageGenerator;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
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
import java.util.Random;
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
    //@Test
    public void runtimeWriterTest() throws IOException, ParserConfigurationException, SAXException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException {
        GraphManager gm = new GraphManager();
        FieldReferenceOffsetManager from = TemplateHandler.loadFrom("src/test/resources/SIUE_GroceryStore/groceryExample.xml");
        MessageSchemaDynamic messageSchema = new MessageSchemaDynamic(from);
        Pipe<RawDataSchema> inPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));
        inPipe.initBuffers();
        Pipe<MessageSchemaDynamic> sharedPipe = new Pipe<MessageSchemaDynamic>(new PipeConfig<MessageSchemaDynamic>(messageSchema));
        sharedPipe.initBuffers();
        Pipe<RawDataSchema> outPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance));
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
        Constructor econstructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), eTarget, PhastEncoderStageGenerator.class);


        //get decoder ready
        StringBuilder dTarget = new StringBuilder();
        PhastDecoderStageGenerator dw = new PhastDecoderStageGenerator(messageSchema, dTarget, false);
        try {
            dw.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        Constructor dconstructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), dTarget, PhastDecoderStageGenerator.class);

        //loading just two messages onto pipe.
        Random rnd = new Random();
        int random, storeID, amount, recordID;
        long date;
        String productName, units;
        for (int i = 0; i < 0; i++){
            //generate random numbers
            random = rnd.nextInt(20);
            storeID = (random < 18) ? 4 : random;
            date = (long)random * 1000;
            productName = "first string test " + Integer.toString(random);
            amount = random * 100;
            recordID = i;
            units = "second string test " + Integer.toString(random);

            //place them on the pipe
            Pipe.addIntValue(storeID,inPipe);
            Pipe.addLongValue(date,inPipe);
            Pipe.addASCII(productName,inPipe);
            Pipe.addIntValue(amount,inPipe);
            Pipe.addIntValue(recordID,inPipe);
            Pipe.addASCII(units,inPipe);
            Pipe.confirmLowLevelWrite(inPipe, 11/* fragment 0  size 8*/);
            Pipe.publishWrites(inPipe);
        }

        econstructor.newInstance(gm, inPipe, sharedPipe);
        ConsoleJSONDumpStage json = new ConsoleJSONDumpStage(gm, sharedPipe, System.out);

        //encoding data
        GraphManager.enableBatching(gm);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.playNice=false;
        scheduler.startup();
        Thread.sleep(300);
        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);

        //decoding data
        dconstructor.newInstance(gm, sharedPipe, outPipe);
        GraphManager.enableBatching(gm);
        scheduler.playNice=false;
        scheduler.startup();
        Thread.sleep(300);
        scheduler.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);

        StringBuilder strProuctName = new StringBuilder();
        StringBuilder strUniits = new StringBuilder();
        for (int i = 0; i < 0; i++) {
            storeID = Pipe.takeValue(outPipe);
            date = Pipe.takeLong(outPipe);
            strProuctName = Pipe.readASCII(outPipe, strProuctName, Pipe.takeRingByteMetaData(outPipe), Pipe.takeRingByteLen(outPipe));
            amount = Pipe.takeValue(outPipe);
            recordID = Pipe.takeValue(outPipe);
            Pipe.readOptionalASCII(outPipe, strUniits, Pipe.takeRingByteMetaData(outPipe), Pipe.takeRingByteLen(outPipe));
            Pipe.confirmLowLevelRead(outPipe, 11);

            System.out.println("storeID = " + storeID);
            System.out.println("date = " + date);
            System.out.println("ProductName = " + strProuctName.toString());
            System.out.println("amount = " + amount);
            System.out.println("record ID = " + recordID);
            System.out.println("Units = " + strUniits.toString());
        }



    }
}