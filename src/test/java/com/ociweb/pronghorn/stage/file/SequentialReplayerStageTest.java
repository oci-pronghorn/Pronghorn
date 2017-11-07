package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class SequentialReplayerStageTest {

	
	@Test
	public void writeWithAckTest() {
		writeWithAckImpl(false, false);
	}
	
	@Test
	public void writeAndReadTest() {
		writeAndReadImpl(false, false);		
	}
	
	@Test
	public void emptyReplayTest() {
		emptyReplayImpl(false, false);
	}
	
	@Test
	public void writeReleaseAndReadTest() {
		writeReleaseAndReadImpl(false, false);
	}
		
	@Test
	public void encryptedWriteWithAckTest() {
		writeWithAckImpl(true, false);
	}

	@Test
	public void encryptedEmptyReplayTest() {
		emptyReplayImpl(true, false);
	}

	@Test
	public void encryptedWriteAndReadTest() {
		writeAndReadImpl(true, false);	
	}
	
	@Test
	public void encyptedWriteReleaseAndReadTest() {
		writeReleaseAndReadImpl(true, false);
	}
	

	private void writeWithAckImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreSchema> perStore = PersistedBlobStoreSchema.instance.newPipe(10, 1000);
		
		perStore.initBuffers();
		
		long fieldBlockId = 10;
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayPosition = 0;
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
		PipeWriter.writeBytes(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		PipeWriter.publishWrites(perStore);
		
		PipeWriter.publishEOF(perStore);
				
		String result = runGraph(perStore, encryption, telemetry);
			
		assertTrue(result, result.indexOf("AckWrite")>0);
		assertTrue(result, result.indexOf("{\"BlockId\":10}")>0);
	}


	private void writeAndReadImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreSchema> perStore = PersistedBlobStoreSchema.instance.newPipe(10, 1000);
		
		perStore.initBuffers();
		
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.writeBytes(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, 0, fieldByteArrayLength);
		PipeWriter.publishWrites(perStore);
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStore);
		
		PipeWriter.publishEOF(perStore); //ensure that the parts do not shut down before we are done
				
		String result = runGraph(perStore, encryption, telemetry);
					
		assertTrue(result, result.indexOf("AckWrite")>0);
		assertTrue(result, result.indexOf("{\"BlockId\":10}")>0);
		assertTrue(result, result.indexOf("0x68,0x65,0x6c,0x6c,0x6f")>0);
		assertTrue(result, result.indexOf("FinishReplay")>0);
	}

	
	private void emptyReplayImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreSchema> perStore = PersistedBlobStoreSchema.instance.newPipe(10, 1000);
		
		perStore.initBuffers();
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStore);
					
		PipeWriter.publishEOF(perStore);
				
		String result = runGraph(perStore, encryption, telemetry);

		assertTrue(result, result.indexOf("BeginReplay")>0);
		assertTrue(result, result.indexOf("FinishReplay")>0);
	}



	private void writeReleaseAndReadImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreSchema> perStore = PersistedBlobStoreSchema.instance.newPipe(10, 1000);
		perStore.initBuffers();
		
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.writeBytes(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, 0, fieldByteArrayLength);
		PipeWriter.publishWrites(perStore);
		
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_RELEASE_7);
		PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_RELEASE_7_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.publishWrites(perStore);
				
		PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStore);
						
		PipeWriter.publishEOF(perStore);
				
		String result = runGraph(perStore, encryption, telemetry);
		
		assertTrue(result, result.indexOf("AckWrite")>0);
		assertTrue(result, result.indexOf("{\"BlockId\":10}")>0);
		assertFalse(result, result.indexOf("0x68,0x65,0x6c,0x6c,0x6f")>0);
		assertTrue(result, result.indexOf("BeginReplay")>0);
		assertTrue(result, result.indexOf("FinishReplay")>0);
	}
	
//	@Ignore
//	public void encyptedWriteReleaseForeverTest() {
//		writeReleaseForeverImpl(true, true);
//	}
//	
//	private void writeReleaseForeverImpl(boolean encryption, boolean telemetry) {
//		final Pipe<PersistedBlobStoreSchema> perStore = PersistedBlobStoreSchema.instance.newPipe(10, 1000);
//		perStore.initBuffers();
//		
//	
//		new Thread(new Runnable() {
//
//			@Override
//			public void run() {
//				
//				byte[] fieldByteArrayBacking = "hello".getBytes();
//				int fieldByteArrayLength = fieldByteArrayBacking.length;
//
//				do {
//					if (Pipe.contentRemaining(perStore) == 0 ) {
//						System.err.println("write");
//						
//						PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
//						PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) 10);
//						PipeWriter.writeBytes(perStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, 0, fieldByteArrayLength);
//						PipeWriter.publishWrites(perStore);
//						
//						PipeWriter.presumeWriteFragment(perStore, PersistedBlobStoreSchema.MSG_RELEASE_7);
//						PipeWriter.writeLong(perStore,PersistedBlobStoreSchema.MSG_RELEASE_7_FIELD_BLOCKID_3, (long) 10);
//						PipeWriter.publishWrites(perStore);
//						
//					} else {
//						Thread.yield();
//					}
//				} while (true);
//			}			
//		}).start();
//		
//
//				
//		String result = runGraph(perStore, encryption, telemetry);
//	
//	}
	
	
	private String runGraph(Pipe<PersistedBlobStoreSchema> perStore, boolean encryption, boolean telemetry) {
		///////////////////////////////
		
		
		GraphManager gm = new GraphManager();
		
		if (telemetry) {
			gm.enableTelemetry(8089);
		}
		
		byte multi = 3;
		byte bits = 16;
		short inFlightCount = 20;
		int largestBlock = 1<<12;
		File dir=null;
	
		byte[] cypher = null;
		if (encryption) {
			cypher = new byte[16];
			new Random(123).nextBytes(cypher);
		}
		
		long rate = 2400;
		Pipe<PersistedBlobLoadSchema> perLoad = FileGraphBuilder.buildSequentialReplayer(gm, perStore, multi, bits, inFlightCount,
				largestBlock, dir, cypher,rate, null);
		
		StringBuilder result = new StringBuilder();
		ConsoleJSONDumpStage watch = ConsoleJSONDumpStage.newInstance(gm, perLoad, result);
		
		/////////////////////////////////////////
		/////////////////////////////////////////
		
		NonThreadScheduler scheduler = new NonThreadScheduler(gm);
		scheduler.startup();
		
		while (!GraphManager.isStageTerminated(gm, watch.stageId) ) {
			scheduler.run();
			Thread.yield();
		}
		
		while (telemetry) {
			scheduler.run();
		}
		
		scheduler.shutdown();
		
		
		///////////////////////
		return result.toString();
	}
	
	
	
	
}
