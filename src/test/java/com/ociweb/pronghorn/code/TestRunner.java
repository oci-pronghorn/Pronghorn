package com.ociweb.pronghorn.code;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.network.IdGenStageTest;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;

public class TestRunner {

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
    	Pipe[] testedToValidate = Pipe.buildPipes(outputConfigs);
    	Pipe[] validateToGenerate = Pipe.buildPipes(outputConfigs);		
    	Pipe[] validateToTested = Pipe.buildPipes(inputConfigs);			
    	Pipe[] generateToValidate = Pipe.buildPipes(inputConfigs);
    	
    	Pipe[] validationInputs = PronghornStage.join(testedToValidate,generateToValidate);
    	Pipe[] validationOutputs = PronghornStage.join(validateToTested,validateToGenerate);
    	
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

    	NonThreadScheduler scheduler = new NonThreadScheduler(gm);
    	
    	scheduler.startup();	
    					
    	long stopTime = testDuration+System.currentTimeMillis();
    	do {
    		scheduler.run();
    	} while (System.currentTimeMillis() < stopTime);
    	    	
    	scheduler.shutdown();
    	
    	
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

    public static void runFuzzTest(Class targetStage, long testDuration, int generatorSeed)  {

    	final int maxPipeLength = 4;
    	final int maxPipeVarArg = 128;

        final Random random = new Random(generatorSeed);
        
    	
    	////////////////////////////
    	//new test code to dynamically generate test configurations for a given stage
    	///////////////////////////
    	for(Constructor<?> con:targetStage.getConstructors()) {
    		
    		GraphManager localTestGM = new GraphManager();
    		
    		Parameter[] params = con.getParameters();
    		
    		Class[] constructorParameterTypes = new Class[params.length];
    		Object[] constructorParameterObjects = new Object[params.length];
    		int cIdx = 0;

    		for(Parameter p: params) {
    			
    			Class<?> clazz = p.getType();
    			String pipeText = p.getParameterizedType().getTypeName();
    			
    			if (clazz.equals(GraphManager.class)) {
    				
    				constructorParameterTypes[cIdx] = localTestGM.getClass();
    				constructorParameterObjects[cIdx++] = localTestGM;
    				
    			} else {
    			    	
	    			if (clazz.isAssignableFrom(Pipe.class)) {
		    			String schemaText = pipeText.substring(pipeText.indexOf('<')+1,pipeText.indexOf('>'));
		    			MessageSchema instance = null;
		    			try {
		    				Class<MessageSchema> schemaClass = (Class<MessageSchema>)Class.forName(schemaText);					
		    				instance = MessageSchema.findInstance(schemaClass);
		    			} catch (ClassNotFoundException e) {
		    				throw new RuntimeException(e);
		    			} 	
		    			
						Pipe<?> pipe = instance.newPipe(maxPipeLength, maxPipeVarArg);					
						constructorParameterTypes[cIdx] = pipe.getClass();
						constructorParameterObjects[cIdx++] = pipe;					    			   
	    			
	    			} else if (clazz.isAssignableFrom(Pipe[].class)) {
		    			String schemaText = pipeText.substring(pipeText.indexOf('<')+1,pipeText.indexOf('>'));
		    			MessageSchema instance = null;
		    			try {
		    				Class<MessageSchema> schemaClass = (Class<MessageSchema>)Class.forName(schemaText);					
		    				instance = MessageSchema.findInstance(schemaClass);
		    			} catch (ClassNotFoundException e) {
		    				throw new RuntimeException(e);
		    			} 	
		    			
	    				int groupSize = 1;
	    				Pipe[] pipeGroup = new Pipe[groupSize];    				
	    				int i = groupSize;
	    				while (--i>=0) {
	    					pipeGroup[i] = instance.newPipe(maxPipeLength, maxPipeVarArg);
	    				}
	    				
						constructorParameterTypes[cIdx] = pipeGroup.getClass();
						constructorParameterObjects[cIdx++] = pipeGroup;
	    				
	    			} else {
	    				
	    				throw new UnsupportedOperationException("Not yet implemented support for "+p);
	    				    				
	    			}    	
    			}
    		}
    		    		 
    		try {
	    		PronghornStage stageToTest = (PronghornStage)targetStage.getConstructor(constructorParameterTypes).newInstance(constructorParameterObjects);  
	    		    		
	    		fuzzTestStage(testDuration, random, localTestGM, stageToTest);
    		} catch (Exception ex) {
    			throw new RuntimeException(ex);
    		}
    	}

    }

	private static void fuzzTestStage(long testDuration, Random random, GraphManager gm, PronghornStage stageToTest) {
		int c = GraphManager.getOutputPipeCount(gm, stageToTest.stageId);
		Pipe[] outputPipes = new Pipe[c];
    	for(int i=1; i<=c; i++) {
    		outputPipes[i-1] = GraphManager.getOutputPipe(gm, stageToTest.stageId, i);
		}
    	c = GraphManager.getInputPipeCount(gm, stageToTest.stageId);
		Pipe[] inputPipes = new Pipe[c];
    	for(int i=1; i<=c; i++) {
    		inputPipes[i-1] = GraphManager.getInputPipe(gm, stageToTest.stageId, i);
		}
    	

    	//all target test stages are market as producer for the duration of this test run
		GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, stageToTest);	
		
		
		int i = inputPipes.length;
		while (--i>=0) {
			GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, new FuzzGeneratorStage(gm, random, testDuration, inputPipes[i]));
		}
		
		int j = outputPipes.length;
		FuzzValidationStage[] valdiators = new FuzzValidationStage[j];
		while (--j>=0) {
			valdiators[j] = new FuzzValidationStage(gm, outputPipes[j]);
		}
				
		
		if (IdGenStageTest.log.isDebugEnabled()) {
			MonitorConsoleStage.attach(gm);
		}
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		
		scheduler.startup();	
						
		long stopTime = testDuration+System.currentTimeMillis();
		do {
			scheduler.run();
		} while (System.currentTimeMillis() < stopTime);
		    	
		scheduler.shutdown();
		
		boolean cleanTerminate = scheduler.awaitTermination(testDuration+IdGenStageTest.SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS);
		
		boolean foundError = false;
		j = valdiators.length;
		while (--j>=0) {
			foundError |= valdiators[j].foundError();
		}
		
		if (!cleanTerminate || foundError) {
			for (Pipe ring: outputPipes) {
				IdGenStageTest.log.info("{}->Valdate {}",stageToTest,ring);
			}
			for (Pipe ring: inputPipes) {
				IdGenStageTest.log.info("Generate->{} {}",stageToTest,ring);
			}			
		}
	}

	private static void fuzzStageToTest(long testDuration, Random random, GraphManager gm, Pipe[] inputRings,
			Pipe[] outputRings, PronghornStage stageToTest) {
		//all target test stages are market as producer for the duration of this test run
		GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, stageToTest);	
    	
    	
    	int i = inputRings.length;
    	while (--i>=0) {
    		GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, new FuzzGeneratorStage(gm, random, testDuration, inputRings[i]));
    	}

    	int j = outputRings.length;
    	FuzzValidationStage[] valdiators = new FuzzValidationStage[j];
    	while (--j>=0) {
    		valdiators[j] = new FuzzValidationStage(gm, outputRings[j]);
    	}
    			
    	
    	if (IdGenStageTest.log.isDebugEnabled()) {
    		MonitorConsoleStage.attach(gm);
    	}
    	 
    	NonThreadScheduler scheduler = new NonThreadScheduler(gm);
    	
    	scheduler.startup();	
    					
    	long stopTime = testDuration+System.currentTimeMillis();
    	do {
    		scheduler.run();
    	} while (System.currentTimeMillis() < stopTime);
    	    	
    	scheduler.shutdown();
    	
    	boolean cleanTerminate = scheduler.awaitTermination(testDuration+IdGenStageTest.SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS);
    	
    	boolean foundError = false;
    	j = valdiators.length;
    	while (--j>=0) {
    		foundError |= valdiators[j].foundError();
    	}
    	
    	if (!cleanTerminate || foundError) {
    		for (Pipe ring: outputRings) {
    			IdGenStageTest.log.info("{}->Valdate {}",stageToTest,ring);
    		}
    		for (Pipe ring: inputRings) {
    			IdGenStageTest.log.info("Generate->{} {}",stageToTest,ring);
    		}			
    	}
	}

}
