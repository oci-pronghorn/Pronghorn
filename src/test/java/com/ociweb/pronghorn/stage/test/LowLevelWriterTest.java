package com.ociweb.pronghorn.stage.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.junit.Test;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.generator.FuzzDataStageGenerator;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.pipe.build.LowLevelWriter;

/**
 *
 * @author amatt
 */
public class LowLevelWriterTest{
        
    
        @Test
        public void lowLevelWriterStartupTest(){            
            //LowLevelWriter lw = new LowLevelWriter();
            
            //lw.startup();
        }
        
        @Test
        public void lowLevelWriterRunTest() {
            //StringBuilder target = new StringBuilder();
            //LowLevelWriter lw = new LowLevelWriter();
            
            //lw.run();
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
