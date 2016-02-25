package com.ociweb.pronghorn.stage.generator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.junit.Test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PhastDecoderStageGeneratorTest {

    @Test
    public void generateDecoderCompileTest() {
        
        StringBuilder target = new StringBuilder();
        PhastDecoderStageGenerator ew = new PhastDecoderStageGenerator(PipeMonitorSchema.instance, target, "com.ociweb.test");

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        
        
        validateCleanCompile(ew.getPackageName(), ew.getClassName(), target);

    }
    
    private static void validateCleanCompile(String packageName, String className, StringBuilder target) {
        try {

            Class generateClass = LoaderUtil.generateClass(packageName, className, target, FuzzDataStageGenerator.class);
            
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
    
}
