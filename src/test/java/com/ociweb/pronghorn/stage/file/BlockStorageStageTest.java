package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class BlockStorageStageTest {

	@Test
	public void fileWriteTest() {
		
		String fileName = null;
		try {
			File f = File.createTempFile("blockStorage", "test");
			fileName = f.getAbsolutePath();
		} catch (IOException e) {
			fail(e.getMessage());
		}
		/////////////////////////
		
		GraphManager gm = new GraphManager();
		
		Pipe<BlockStorageXmitSchema>[] input = 
				new Pipe[]{BlockStorageXmitSchema.instance.newPipe(10, 1000),
						   BlockStorageXmitSchema.instance.newPipe(10, 1000),
						   BlockStorageXmitSchema.instance.newPipe(10, 1000)};
		
		Pipe<BlockStorageReceiveSchema>[] output =
				new Pipe[]{BlockStorageReceiveSchema.instance.newPipe(10, 1000),
						   BlockStorageReceiveSchema.instance.newPipe(10, 1000),
						   BlockStorageReceiveSchema.instance.newPipe(10, 1000)};
		
		new BlockStorageStage(gm, fileName, input, output);
		
		int pipeIdx = 1;
		
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage<BlockStorageReceiveSchema> watchNo1 = new ConsoleJSONDumpStage<>(gm, output[0]);
		ConsoleJSONDumpStage<BlockStorageReceiveSchema> watch    = new ConsoleJSONDumpStage<>(gm, output[1], results, true);
		ConsoleJSONDumpStage<BlockStorageReceiveSchema> watchNo2 = new ConsoleJSONDumpStage<>(gm, output[2]);
		
		input[0].initBuffers();
		input[1].initBuffers();
		input[2].initBuffers();
		
		
		////////////////////////////
		
		long fieldPosition = 0;
		byte[] fieldPayloadBacking = "hello world".getBytes();
		int fieldPayloadPosition = 0;
		int fieldPayloadLength = fieldPayloadBacking.length;
		BlockStorageXmitSchema.publishWrite(input[pipeIdx], 
				fieldPosition, 
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength);
		
		BlockStorageXmitSchema.publishRead(input[pipeIdx], 0, fieldPayloadLength);
		
		PipeWriter.publishEOF(input[0]);
		PipeWriter.publishEOF(input[1]);		
		PipeWriter.publishEOF(input[2]);
		
		//////////////////////////////
		
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		
		scheduler.startup();
		
		while (!GraphManager.isStageTerminated(gm, watch.stageId)) {
				scheduler.run();		
		}
		
		scheduler.shutdown();
		
		String value = results.toString();
		assertTrue(value, value.indexOf("WriteAck")>=0);
		assertTrue(value, value.indexOf("hello world")>=0);

		
	}
	
	
}
