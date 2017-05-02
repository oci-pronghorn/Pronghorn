package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import com.ociweb.pronghorn.code.StageTester;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class PersistedBlobStageTest {

//	@Test
//	public void fuzzTest() {		
//		File rootFolder = tempDirectory();		
//		long testDuration = 300;
//		int generatorSeed = 42;;
//		StageTester.runFuzzTest(PersistedBlobStage.class, testDuration, generatorSeed, 
//				new Object[]{new Byte((byte)2),new Byte((byte)3),rootFolder});		
//	}

	private File tempDirectory() {
		File rootFolder = null;
		try {
			Path directory = Files.createTempDirectory("test");
			rootFolder = directory.toFile();
		} catch (IOException e) {
			fail();
		}
		return rootFolder;
	}
	
	@Test
	public void functionalTest() {	
		File rootFolder = tempDirectory();	
		byte fileSizeMultiplier = 2;
		byte maxIdValueBits = 6;
		
		
		GraphManager gm = new GraphManager();
		
		Pipe<PersistedBlobStoreSchema> storeRequests = PersistedBlobStoreSchema.instance.newPipe(10, 128);
		Pipe<PersistedBlobLoadSchema> loadResponses = PersistedBlobLoadSchema.instance.newPipe(10, 128);

		PersistedBlobStage.newInstance(gm, storeRequests, loadResponses, fileSizeMultiplier, maxIdValueBits, rootFolder);
		
		StringBuilder out = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, loadResponses, out);
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		
		scheduler.startup();
		
		publishBlock(storeRequests, (long) 31, "hello");
		
		scheduler.run();  //new block is consumed.
		
		assertFalse(storeRequests.toString(), PipeReader.hasContentToRead(storeRequests));
				
		PersistedBlobStoreSchema.publishRequestReplay(storeRequests);
		
		scheduler.run(); //ack returned
		scheduler.run(); //requested replay is consumed
		
		String results = out.toString();
		out.setLength(0);
		assertTrue(results, results.startsWith("{\"AckWrite\":"));
		assertTrue(results, results.contains("BeginReplay"));
		assertTrue(results, results.contains("{\"BlockId\":31}"));
		assertTrue(results, results.contains("FinishReplay"));
		
		//TODO: NEED TO SHOW REMOVAL OF BLOCK AND NON REPEAT
		publishBlock(storeRequests, (long) 33, "world");
		scheduler.run();  //new block is consumed.
		
		assertTrue(PersistedBlobStoreSchema.publishRelease(storeRequests, 31));
		scheduler.run();
		scheduler.run();
		results = out.toString();
		assertTrue(results, results.startsWith("{\"AckWrite\":"));
		assertTrue(results, results.contains("{\"AckRelease\":"));
		
		out.setLength(0);
		PersistedBlobStoreSchema.publishRequestReplay(storeRequests);
		scheduler.run();
		scheduler.run();
		results = out.toString();
		
		assertFalse(results, results.contains("{\"BlockId\":31}"));
		assertTrue(results, results.contains("{\"BlockId\":33}"));
		
		scheduler.shutdown();

	}

	private void publishBlock(Pipe<PersistedBlobStoreSchema> storeRequests, long fieldBlockId, String payload) {
		byte[] fieldByteArrayBacking = payload.getBytes();
		int fieldByteArrayPosition = 0;
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		assertTrue(PersistedBlobStoreSchema.publishBlock(storeRequests, 
				fieldBlockId, 
				fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength));
	}
	
}
