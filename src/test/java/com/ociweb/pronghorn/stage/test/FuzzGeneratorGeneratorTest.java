package com.ociweb.pronghorn.stage.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class FuzzGeneratorGeneratorTest {

    
    @Test
    public void fuzzGeneratorBuildTest() {
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorBuildRunnableTest() {

        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target, true);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }        
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    
    @Test
    public void fuzzGeneratorBuildRunnable2Test() {
        MessageSchemaDynamic schema = sequenceExampleSchema();               
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(schema, target, true);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
       // System.out.println(target);
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorUsageTestPipeMonitor() {
        
        StringBuilder target = new StringBuilder();
        
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target);
        // //set the field to use for latency
        ew.setTimeFieldId(1); //use the MS field from the monitor schema to put the time into.
        
        int durationMS = 200;
        
        runtimeTestingOfFuzzGenerator(target, PipeMonitorSchema.instance, ew, durationMS,200);
    }
    
    @Test
    public void fuzzGeneratorUsageTestSequenceExample() {

            MessageSchemaDynamic schema = sequenceExampleSchema();
        
            
            //Fuzz time test,  what ranges are the best?
            
            
            StringBuilder target = new StringBuilder();
            
            FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(schema, target);
            ew.setMaxSequenceLengthInBits(9);
            
            int durationMS = 1000; 
            
            runtimeTestingOfFuzzGenerator(target, schema, ew, durationMS, 8000);

        
    }

    private MessageSchemaDynamic sequenceExampleSchema() {
        try {
            MessageSchemaDynamic schema;
            schema = new MessageSchemaDynamic(TemplateHandler.loadFrom("/template/sequenceExample.xml"));
            return schema;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            fail();
        } catch (SAXException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return null;
    }
    
    @Ignore
    public void fuzzGeneratorUsageTestRawData() {
        
        StringBuilder target = new StringBuilder();
                
        //TODO: Rewrite for ByteVector, DO NOT use ByteBuffer instead use the easier DataOutput object.
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(RawDataSchema.instance, target);
        
        
        int durationMS = 300;
        
        runtimeTestingOfFuzzGenerator(target, RawDataSchema.instance, ew, durationMS,1000);
    }

    private void runtimeTestingOfFuzzGenerator(StringBuilder target, MessageSchema schema, FuzzGeneratorGenerator ew, int durationMS, int pipeLength) {
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        
             
        
        try {
            Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, FuzzGeneratorGenerator.class);
            
            
            GraphManager gm = new GraphManager();
            
            //NOTE: Since the ConsoleSummaryStage usess the HighLevel API the pipe MUST be large enough to hold and entire message
            //      Would be nice to detect this failure, not sure how.
            Pipe<?> pipe = new Pipe<>(new PipeConfig<>(schema, pipeLength));           
            
            constructor.newInstance(gm, pipe);
            Appendable out = new PrintWriter(new ByteArrayOutputStream());
            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe, out );
            
            GraphManager.enableBatching(gm);
       //     MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.playNice=false;
            scheduler.startup();
            
            Thread.sleep(durationMS);
            
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
     
    
    
    private static void validateCleanCompile(String packageName, String className, StringBuilder target) {
        try {

        Class generateClass = LoaderUtil.generateClass(packageName, className, target, FuzzGeneratorGenerator.class);
        
        if (generateClass.isAssignableFrom(PronghornStage.class)) {
            Constructor constructor =  generateClass.getConstructor(GraphManager.class, Pipe.class);
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
    
    //TODO: methods below this point need to be moved to static helper class.

    
    
}
