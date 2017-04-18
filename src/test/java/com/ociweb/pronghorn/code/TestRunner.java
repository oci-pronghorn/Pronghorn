package com.ociweb.pronghorn.code;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.network.IdGenStageTest;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class TestRunner {

    public static Pipe[] joinArrays(Pipe[] a, Pipe[] b) {
    	int len = a.length+b.length;
    	Pipe[] result = new Pipe[len];
    	int i = b.length;
    	while (--i>=0) {
    		result[--len] = b[i];
    	}
    	i = a.length;
    	while (--i>=0) {
    		result[--len] = a[i];
    	}	
    	return result;
    }

    public static Pipe[] buildRings(PipeConfig[] configs) {
    	int i = configs.length;
    	Pipe[] result = new Pipe[i];
    	while (--i>=0) {
    		result[i] = new Pipe(configs[i]);
    	}		
    	return result;
    }

    /**
     * General method for running each "expected use" tests
     * 
     * Specific details of each tested stage must be passed in.
     * 
     */
    public static void runExpectedUseTest(Class targetStage, PipeConfig[] inputConfigs,
    		PipeConfig[] outputConfigs, final long testDuration, GVSValidator validator, GGSGenerator generator,
    		Random random) throws NoSuchMethodException, InstantiationException, IllegalAccessException,
    				InvocationTargetException {
    	
    	IdGenStageTest.log.info("begin 'expected use' testing {}",targetStage);
    	
    	GraphManager gm = new GraphManager();
    	
    	//NOTE: Uses RingBufferConfig queue size so we only test for the case that is built and deployed.		
    	Pipe[] testedToValidate = buildRings(outputConfigs);
    	Pipe[] validateToGenerate = buildRings(outputConfigs);		
    	Pipe[] validateToTested = buildRings(inputConfigs);			
    	Pipe[] generateToValidate = buildRings(inputConfigs);
    	
    	Pipe[] validationInputs = joinArrays(testedToValidate, generateToValidate);
    	Pipe[] validationOutputs = joinArrays(validateToTested, validateToGenerate);
    	
    	Pipe[] generatorInputs = validateToGenerate;
    	Pipe[] generatorOutputs = generateToValidate;
    	
    	Constructor constructor = targetStage.getConstructor(gm.getClass(), validateToTested.getClass(), testedToValidate.getClass());
    
    	//all target test stages are market as producer for the duration of this test run
    	GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, (PronghornStage)constructor.newInstance(gm, validateToTested, testedToValidate));
    							
    	//validation shuts down when the producers on both end have already shut down.
    	ExpectedUseValidationStage valdiationStage = new ExpectedUseValidationStage(gm, validationInputs, validationOutputs, validator);
    
    	//generator is always a producer and must be marked as such.			
    	GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER,
    			                  new ExpectedUseGeneratorStage(gm, generatorInputs, generatorOutputs, random, generator));
    	
    	if (IdGenStageTest.log.isDebugEnabled()) {
    		MonitorConsoleStage.attach(gm);
    	}
    	 
    	//TODO: create new single threaded deterministic scheduler.
    	StageScheduler scheduler = new ThreadPerStageScheduler(gm);
    	
    	scheduler.startup();		
    	
    	if (!scheduler.awaitTermination(testDuration+IdGenStageTest.SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS) || valdiationStage.foundError()) {
    		for (Pipe ring: testedToValidate) {
    			IdGenStageTest.log.info("{}->Valdate {}",targetStage,ring);
    		}
    		for (Pipe ring: validateToTested) {
    			IdGenStageTest.log.info("Validate->{} {}",targetStage,ring);
    		}
    		for (Pipe ring: validateToGenerate) {
    			IdGenStageTest.log.info("Validate->Generate {}",targetStage,ring);
    		}
    		for (Pipe ring: generateToValidate) {
    			IdGenStageTest.log.info("Generate->Validate {}",targetStage,ring);
    		}			
    	}
    }

    public static void runFuzzTest(Class targetStage, PipeConfig[] inputConfigs, PipeConfig[] outputConfigs, long testDuration, Random random) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    	IdGenStageTest.log.info("begin 'fuzz' testing {}",targetStage);
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe[] inputRings = buildRings(inputConfigs);	
    	Pipe[] outputRings = buildRings(outputConfigs);	
    	
    	int i = inputRings.length;
    	while (--i>=0) {
    		GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, new FuzzGeneratorStage(gm, random, testDuration, inputRings[i]));
    	}
    	
    	//TODO: test must check for hang, needs special scheduler that tracks time and reports longest active actor
    	//TODO: test much check for throw, done by scheduler closing early, should still be running when we request shutdown
    	//TODO: test much check output rings for valid messages if any, done inside FuzzValidationStage
    	
    	int j = outputRings.length;
    	FuzzValidationStage[] valdiators = new FuzzValidationStage[j];
    	while (--j>=0) {
    		valdiators[j] = new FuzzValidationStage(gm, outputRings[j]);
    		
    	}
    		
    	//TODO: once complete determine how we will do this with multiple queues.
    	Constructor constructor = targetStage.getConstructor(gm.getClass(), inputRings.getClass(), outputRings.getClass());
    			
    	//all target test stages are market as producer for the duration of this test run
    	GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, (PronghornStage)constructor.newInstance(gm, inputRings, outputRings));	
    	
    	if (IdGenStageTest.log.isDebugEnabled()) {
    		MonitorConsoleStage.attach(gm);
    	}
    	 
    	//TODO: create new single threaded deterministic scheduler.
    	StageScheduler scheduler = new ThreadPerStageScheduler(gm);
    	
    	scheduler.startup();	
    					
    	boolean cleanTerminate = scheduler.awaitTermination(testDuration+IdGenStageTest.SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS);
    	
    	boolean foundError = false;
    	j = valdiators.length;
    	while (--j>=0) {
    		foundError |= valdiators[j].foundError();
    	}
    	
    	if (!cleanTerminate || foundError) {
    		for (Pipe ring: outputRings) {
    			IdGenStageTest.log.info("{}->Valdate {}",targetStage,ring);
    		}
    		for (Pipe ring: inputRings) {
    			IdGenStageTest.log.info("Generate->{} {}",targetStage,ring);
    		}			
    	}
    	
    }

}
