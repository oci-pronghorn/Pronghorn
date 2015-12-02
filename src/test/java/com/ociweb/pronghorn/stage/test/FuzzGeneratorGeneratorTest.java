package com.ociweb.pronghorn.stage.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
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
            e.printStackTrace();
            fail();
        }
        
    //    System.out.println(target);
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    @Test
    public void fuzzGeneratorUsageTest() {
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new FuzzGeneratorGenerator(PipeMonitorSchema.instance, target);

        //set the field to use for latency
        ew.setTimeFieldId(1); //use the MS field from the monitor schema to put the time into.
        
        try {
            ew.processSchema();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
             
        
        try {
            Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, FuzzGeneratorGenerator.class);
            
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<PipeMonitorSchema> config = new PipeConfig<PipeMonitorSchema>(PipeMonitorSchema.instance, 1000);
            Pipe<PipeMonitorSchema> pipe = new Pipe<PipeMonitorSchema>(config);
            
            
            constructor.newInstance(gm, pipe);
            
            
            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe);
            
            GraphManager.enableBatching(gm);
       //     MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.playNice=false;
            scheduler.startup();
            
            Thread.sleep(300);
            
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

        Constructor constructor =  LoaderUtil.generateClassConstructor(packageName, className, target, FuzzGeneratorGenerator.class);
        assertNotNull(constructor);
        
        
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            e.printStackTrace();
            fail();
        }
        
    }
    
    //TODO: methods below this point need to be moved to static helper class.

    
    
}
