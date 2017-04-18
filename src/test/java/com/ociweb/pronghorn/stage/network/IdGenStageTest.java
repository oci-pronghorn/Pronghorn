package com.ociweb.pronghorn.stage.network;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.code.GGSGenerator;
import com.ociweb.pronghorn.code.GVSValidator;
import com.ociweb.pronghorn.code.TestRunner;
import com.ociweb.pronghorn.network.mqtt.IdGenStage;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;

public class IdGenStageTest {
	
	public static final Logger log = LoggerFactory.getLogger(IdGenStageTest.class);
	public static final long SHUTDOWN_WINDOW = 500;//No shutdown should every take longer than this.
	
	
	@Test
    public void testIdGenExpectedUse() {
	    
	    long testDuration = 500; //keep short for now to save limited time on build server
	    
	    GVSValidator validator = IdGenStageBehavior.validator();
        GGSGenerator generator = IdGenStageBehavior.generator(testDuration);

        //to randomize from seed
        int generatorSeed = 42;
        Random random = new Random(generatorSeed);
                    
        Class targetStage = IdGenStage.class;
        PipeConfig[] inputConfigs = new PipeConfig[]{MQTTIdRangeSchema.instance.newPipeConfig(100)};
        PipeConfig[] outputConfigs = new PipeConfig[]{MQTTIdRangeSchema.instance.newPipeConfig(100)};
        try {
            TestRunner.runExpectedUseTest(targetStage, inputConfigs, outputConfigs, testDuration, validator, generator, random);   
        } catch (Exception e) {
            throw new RuntimeException(e);
         }
         
	}
	
	
	
	@Test
	public void testIdGenFuzz() {
	    
        GVSValidator validator = IdGenStageBehavior.validator();
        
        long testDuration = 500; //keep short for now to save limited time on build server
        GGSGenerator generator = IdGenStageBehavior.generator(testDuration);

        //to randomize from seed
        int generatorSeed = 42;
        Random random = new Random(generatorSeed);
                    
        Class targetStage = IdGenStage.class;
        PipeConfig[] inputConfigs = new PipeConfig[]{MQTTIdRangeSchema.instance.newPipeConfig(100)};
        PipeConfig[] outputConfigs = new PipeConfig[]{MQTTIdRangeSchema.instance.newPipeConfig(100)};
        try {
            TestRunner.runExpectedUseTest(targetStage, inputConfigs, outputConfigs, testDuration, validator, generator, random);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
	    
	}
	
	
}
