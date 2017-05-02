package com.ociweb.pronghorn.code;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;

public class StageTester {

    interface Provider<T> {

		T get(int i);

	}

	private static final long SHUTDOWN_WINDOW = 500;
    private static final Logger log = LoggerFactory.getLogger(StageTester.class);

    static final int maxPipeLength = 4;
    static final int maxPipeVarArg = 1<<16;
    
	/**
     * General method for running each "expected use" tests
     * 
     * Specific details of each tested stage must be passed in.
     * 
     */
    public static <S extends PronghornStage> void runExpectedUseTest(Class<S> targetStage, PipeConfig[] inputConfigs,
    		PipeConfig[] outputConfigs, final long testDuration, GVSValidator validator, GGSGenerator generator,
    		Random random) throws NoSuchMethodException, InstantiationException, IllegalAccessException,
    				InvocationTargetException {
  
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
    	
    	if (log.isDebugEnabled()) {
    		MonitorConsoleStage.attach(gm);
    	}

    	NonThreadScheduler scheduler = new NonThreadScheduler(gm);
    	
    	scheduler.startup();	
    					
    	long stopTime = testDuration+System.currentTimeMillis();
    	do {
    		scheduler.run();
    	} while (System.currentTimeMillis() < stopTime);
    	    	
    	scheduler.shutdown();
    	
    	
    	if (!scheduler.awaitTermination(testDuration+SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS) || valdiationStage.foundError()) {
    		for (Pipe ring: testedToValidate) {
    			log.info("{}->Valdate {}",targetStage,ring);
    		}
    		for (Pipe ring: validateToTested) {
    			log.info("Validate->{} {}",targetStage,ring);
    		}
    		for (Pipe ring: validateToGenerate) {
    			log.info("Validate->Generate {}",targetStage,ring);
    		}
    		for (Pipe ring: generateToValidate) {
    			log.info("Generate->Validate {}",targetStage,ring);
    		}			
    	}
    }

    public static <S extends PronghornStage> boolean runFuzzTest(Class<S> targetStage, long testDuration, int generatorSeed)  {
    	return runFuzzTest(targetStage,testDuration,generatorSeed,  new MessageSchema[]{}, new Object[]{});
    }
    
    public static <S extends PronghornStage> boolean runFuzzTest(Class<S> targetStage, long testDuration, int generatorSeed, 
	          MessageSchema[] undefinedSchemas)  {
    	return runFuzzTest(targetStage, testDuration, generatorSeed, undefinedSchemas, new Object[]{});
    }

    public static <S extends PronghornStage> boolean runFuzzTest(Class<S> targetStage, long testDuration, int generatorSeed, 
	          Object[] undefinedArgs)  {
    	return runFuzzTest(targetStage,testDuration,generatorSeed, new MessageSchema[]{}, undefinedArgs);
    }

    
    public static <S extends PronghornStage> boolean runFuzzTest(Class<S> targetStage, long testDuration, int generatorSeed, 
    		          MessageSchema[] undefinedSchemas, Object[] undefinedArgs)  {


        final Random random = new Random(generatorSeed);
        
    	
    	////////////////////////////
    	//new test code to dynamically generate test configurations for a given stage
    	///////////////////////////
    	for(Constructor<?> con:targetStage.getConstructors()) {
    		
    		if (!con.isVarArgs()) {
    		
				PrintStream temp = System.out;
				PrintStream temp2 = System.err;
				ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
				try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					System.setOut(new PrintStream(baos));
					System.setErr(new PrintStream(errCapture));			
					
	    		testOneGraph(targetStage, testDuration, 
	    				      (idx)->{return undefinedSchemas[idx];}, 
	    				      (idx)->{return undefinedArgs[idx];}, 
	    				      maxPipeLength, maxPipeVarArg,
						      random, con);
	
				} finally {
					System.setOut(temp);
					System.setErr(temp2);
				}
				
				if (errCapture.size()>0) {
					System.err.println(new String(errCapture.toByteArray()));
					return false;
				}
				
    		} else {
    			log.info("var arg constructor discovered and skipped for testing");
    		}
    	}
    	return true;
    }

    public static <S extends PronghornStage> boolean runFuzzTest(Class<S> targetStage, long testDuration, int generatorSeed, 
	          MessageSchema undefinedSchema, Object ... undefinedArgs)  {


			final Random random = new Random(generatorSeed);
			
			
			////////////////////////////
			//new test code to dynamically generate test configurations for a given stage
			///////////////////////////
			for(Constructor<?> con:targetStage.getConstructors()) {
				
				if (!con.isVarArgs()) {
				
					PrintStream temp = System.out;
					PrintStream temp2 = System.err;
					ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
					try {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						System.setOut(new PrintStream(baos));
						System.setErr(new PrintStream(errCapture));			

						testOneGraph(targetStage, testDuration, 
							      (idx)->{return undefinedSchema;}, 
							      (idx)->{return undefinedArgs[idx];}, 
							      maxPipeLength, maxPipeVarArg,
							      random, con);
						
					} finally {
						System.setOut(temp);
						System.setErr(temp2);
					}
					
					if (errCapture.size()>0) {
						System.err.println(new String(errCapture.toByteArray()));
						return false;
					}
				} else {
					log.info("var arg constructor discovered and skipped for testing");
				}
			}
			return true;
	}
    
	private static <S extends PronghornStage> void testOneGraph(Class<S> targetStage, long testDuration,
			Provider<MessageSchema> undefinedSchemas, Provider<Object> undefinedArgs, final int maxPipeLength, final int maxPipeVarArg,
			final Random random, Constructor<?> con) {
		GraphManager localTestGM = new GraphManager();
		int schemaPos = 0;
		int argsPos = 0;
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
					MessageSchema instance = selectSchema(pipeText);
					if (null==instance) {//use passed in schemas since this stage can take any schema
						instance = undefinedSchemas.get(schemaPos++);
					}
					
					Pipe<?> pipe = instance.newPipe(maxPipeLength, maxPipeVarArg);					
					constructorParameterTypes[cIdx] = pipe.getClass();
					constructorParameterObjects[cIdx++] = pipe;					    			   
				
				} else if (clazz.isAssignableFrom(Pipe[].class)) {
					MessageSchema instance = selectSchema(pipeText);
					if (null==instance) {//use passed in schemas since this stage can take any schema
						instance = undefinedSchemas.get(schemaPos++);
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
					if (clazz.isAssignableFrom(Appendable.class) ) {
						constructorParameterTypes[cIdx] = clazz;
						constructorParameterObjects[cIdx++] = new StringBuilder();
					} else {
						if (null == undefinedArgs) {
								throw new UnsupportedOperationException("Not yet implemented support for "+p);
						} else {
							Object arg = undefinedArgs.get(argsPos++);
							
							if (arg instanceof Integer) {//changes to primitive
								constructorParameterTypes[cIdx] = Integer.TYPE;
							} else if (arg instanceof Long) {//changes to primitive
								constructorParameterTypes[cIdx] = Long.TYPE;
							} else if (arg instanceof Byte) {//changes to primitive
								constructorParameterTypes[cIdx] = Byte.TYPE;
							} else if (arg instanceof Boolean) {//changes to primitive
							    constructorParameterTypes[cIdx] = Boolean.TYPE;
					    	} else {
								constructorParameterTypes[cIdx] = arg.getClass();
							}
							
							constructorParameterObjects[cIdx++] = arg;
								    					
						}
					}
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

	private static MessageSchema selectSchema(String pipeText) {
		MessageSchema instance = null;
		{
			try {
				String schemaText = pipeText.substring(pipeText.indexOf('<')+1,pipeText.indexOf('>'));
				Class<MessageSchema> schemaClass = (Class<MessageSchema>)Class.forName(schemaText);					
				instance = MessageSchema.findInstance(schemaClass);
			} catch (Exception e) {
				return null;//can not extract schema
			} 	
		}
		return instance;
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
				
		
		if (log.isDebugEnabled()) {
			MonitorConsoleStage.attach(gm);
		}
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		
		scheduler.startup();	
						
		long stopTime = testDuration+System.currentTimeMillis();
		do {
			scheduler.run();
		} while (System.currentTimeMillis() < stopTime);
		    	
		scheduler.shutdown();
		
		boolean cleanTerminate = scheduler.awaitTermination(testDuration+SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS);
		
		boolean foundError = false;
		j = valdiators.length;
		while (--j>=0) {
			foundError |= valdiators[j].foundError();
		}
		
		if (!cleanTerminate || foundError) {
			for (Pipe ring: outputPipes) {
				log.info("{}->Valdate {}",stageToTest,ring);
			}
			for (Pipe ring: inputPipes) {
				log.info("Generate->{} {}",stageToTest,ring);
			}			
		}
	}

	private static <S extends PronghornStage> void fuzzStageToTest(long testDuration, Random random, GraphManager gm, Pipe[] inputRings,
			Pipe[] outputRings, S stageToTest) {
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
    			
    	
    	if (log.isDebugEnabled()) {
    		MonitorConsoleStage.attach(gm);
    	}
    	 
    	NonThreadScheduler scheduler = new NonThreadScheduler(gm);
    	
    	scheduler.startup();	
    					
    	long stopTime = testDuration+System.currentTimeMillis();
    	do {
    		scheduler.run();
    	} while (System.currentTimeMillis() < stopTime);
    	    	
    	scheduler.shutdown();
    	
    	boolean cleanTerminate = scheduler.awaitTermination(testDuration+SHUTDOWN_WINDOW, TimeUnit.MILLISECONDS);
    	
    	boolean foundError = false;
    	j = valdiators.length;
    	while (--j>=0) {
    		foundError |= valdiators[j].foundError();
    	}
    	
    	if (!cleanTerminate || foundError) {
    		for (Pipe ring: outputRings) {
    			log.info("{}->Valdate {}",stageToTest,ring);
    		}
    		for (Pipe ring: inputRings) {
    			log.info("Generate->{} {}",stageToTest,ring);
    		}			
    	}
	}

	public static boolean hasBadChar(CharSequence text) {
		int i = text.length();
		boolean found = false;
		while (--i>=0) {
			int c = text.charAt(i);
			found |= (0xFFFD == c);
		}
		return found;
	}

}
