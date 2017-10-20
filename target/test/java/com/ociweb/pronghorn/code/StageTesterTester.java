package com.ociweb.pronghorn.code;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Random;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class StageTesterTester {

	
	@Test
	public void testStageVal() {
		
		int testDuration = 100;
		int generatorSeed = 51;
		assertTrue(StageTester.runFuzzTest(FuzzValidationStage.class, testDuration, generatorSeed, 
				                new MessageSchema[]{RawDataSchema.instance}));
		
		assertTrue(StageTester.runFuzzTest(FuzzValidationStage.class, testDuration, generatorSeed, 
								new MessageSchema[]{ClientHTTPRequestSchema.instance}));
		
		assertTrue(StageTester.runFuzzTest(FuzzValidationStage.class, testDuration, generatorSeed, 
								new MessageSchema[]{PipeMonitorSchema.instance}));
	
		
	}
	
	@Test
	public void testStageGen() {
		
		int testDuration = 100;
		int generatorSeed = 51;
		assertTrue(StageTester.runFuzzTest(FuzzGeneratorStage.class, testDuration, generatorSeed, 
				                new MessageSchema[]{RawDataSchema.instance},
				                new Object[]{new Random(42),10L}));
		
		assertTrue(StageTester.runFuzzTest(FuzzGeneratorStage.class, testDuration, generatorSeed, 
								new MessageSchema[]{ClientHTTPRequestSchema.instance},
								new Object[]{new Random(42),10L}));
		
		assertTrue(StageTester.runFuzzTest(FuzzGeneratorStage.class, testDuration, generatorSeed, 
								new MessageSchema[]{PipeMonitorSchema.instance},
								new Object[]{new Random(42),10L}));
			
	}

	@Test
	public void testStageJSONDump() {
		
		int testDuration = 100;
		int generatorSeed = 13;
		
		assertTrue(StageTester.runFuzzTest(ConsoleJSONDumpStage.class, testDuration, generatorSeed++, 
							RawDataSchema.instance, false));
					       //never true above since RawDataSchema is NOT defined as UTF8
					
		assertTrue(StageTester.runFuzzTest(ConsoleJSONDumpStage.class, testDuration, generatorSeed++, 
							ClientHTTPRequestSchema.instance, false));
							//never true above since payload is NOT defined as UTF8
					
		assertTrue(StageTester.runFuzzTest(ConsoleJSONDumpStage.class, testDuration, generatorSeed++, 
							PipeMonitorSchema.instance, false));

		assertTrue(StageTester.runFuzzTest(ConsoleJSONDumpStage.class, testDuration, generatorSeed++, 
							PipeMonitorSchema.instance, true));

;
			
	}
}
