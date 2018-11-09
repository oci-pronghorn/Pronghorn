package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.SecureRandom;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.encrypt.NoiseProducer;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
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
		Pipe<PersistedBlobStoreProducerSchema> perStoreProducer = PersistedBlobStoreProducerSchema.instance.newPipe(10, 1000);
		Pipe<PersistedBlobStoreConsumerSchema> perStoreConsumer = PersistedBlobStoreConsumerSchema.instance.newPipe(10, 1000);
		
		perStoreProducer.initBuffers();
		perStoreConsumer.initBuffers();
		
		
		long fieldBlockId = 10;
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayPosition = 0;
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStoreProducer, PersistedBlobStoreProducerSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
		PipeWriter.writeBytes(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		PipeWriter.publishWrites(perStoreProducer);
		
		PipeWriter.publishEOF(perStoreConsumer);
				
		String result = runGraph(perStoreProducer, perStoreConsumer, encryption, telemetry);
			
		assertTrue(result, result.indexOf("AckWrite")>0);
		assertTrue(result, result.indexOf("{\"BlockId\":10}")>0);
	}


	private void writeAndReadImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreProducerSchema> perStoreProducer = PersistedBlobStoreProducerSchema.instance.newPipe(10, 1000);
		Pipe<PersistedBlobStoreConsumerSchema> perStoreConsumer = PersistedBlobStoreConsumerSchema.instance.newPipe(10, 1000);
		
		perStoreProducer.initBuffers();
		perStoreConsumer.initBuffers();
		
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStoreProducer, PersistedBlobStoreProducerSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.writeBytes(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, 0, fieldByteArrayLength);
		PipeWriter.publishWrites(perStoreProducer);
		
		PipeWriter.presumeWriteFragment(perStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStoreConsumer);
		
		PipeWriter.publishEOF(perStoreConsumer); //ensure that the parts do not shut down before we are done
		PipeWriter.publishEOF(perStoreProducer);
				
		String result = runGraph(perStoreProducer, perStoreConsumer, encryption, telemetry);
					
		assertTrue(result, result.indexOf("AckWrite")>0);
		assertTrue(result, result.indexOf("{\"BlockId\":10}")>0);
		assertTrue(result, result.indexOf("0x68,0x65,0x6c,0x6c,0x6f")>0);
		assertTrue(result, result.indexOf("FinishReplay")>0);
	}

	
	private void emptyReplayImpl(boolean encryption, boolean telemetry) {
		Pipe<PersistedBlobStoreProducerSchema> perStoreProducer = PersistedBlobStoreProducerSchema.instance.newPipe(10, 1000);
		Pipe<PersistedBlobStoreConsumerSchema> perStoreConsumer = PersistedBlobStoreConsumerSchema.instance.newPipe(10, 1000);
		
		perStoreProducer.initBuffers();
		perStoreConsumer.initBuffers();
		
		PipeWriter.presumeWriteFragment(perStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStoreConsumer);
					
		PipeWriter.publishEOF(perStoreConsumer);
				
		String result = runGraph(perStoreProducer, perStoreConsumer, encryption, telemetry);

		assertTrue(result, result.indexOf("BeginReplay")>0);
		assertTrue(result, result.indexOf("FinishReplay")>0);
	}



	private void writeReleaseAndReadImpl(boolean encryption, boolean telemetry) {
		
		
		Pipe<PersistedBlobStoreProducerSchema> perStoreProducer = PersistedBlobStoreProducerSchema.instance.newPipe(10, 1000);
		Pipe<PersistedBlobStoreConsumerSchema> perStoreConsumer = PersistedBlobStoreConsumerSchema.instance.newPipe(10, 1000);
		
		perStoreProducer.initBuffers();
		perStoreConsumer.initBuffers();
		
		byte[] fieldByteArrayBacking = "hello".getBytes();
		int fieldByteArrayLength = fieldByteArrayBacking.length;
		
		PipeWriter.presumeWriteFragment(perStoreProducer, PersistedBlobStoreProducerSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.writeBytes(perStoreProducer,PersistedBlobStoreProducerSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, 0, fieldByteArrayLength);
		PipeWriter.publishWrites(perStoreProducer);
		
		PipeWriter.presumeWriteFragment(perStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_RELEASE_7);
		PipeWriter.writeLong(perStoreConsumer,PersistedBlobStoreConsumerSchema.MSG_RELEASE_7_FIELD_BLOCKID_3, (long) 10);
		PipeWriter.publishWrites(perStoreConsumer);
				
		PipeWriter.presumeWriteFragment(perStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(perStoreConsumer);
						
		PipeWriter.publishEOF(perStoreConsumer);
				
		String result = runGraph(perStoreProducer, perStoreConsumer, encryption, telemetry);
		
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
	
	
	private String runGraph(Pipe<PersistedBlobStoreProducerSchema> perStoreProducer,
							Pipe<PersistedBlobStoreConsumerSchema> perStoreConsumer,
			                boolean encryption, boolean telemetry) {
		///////////////////////////////
		
		
		GraphManager gm = new GraphManager();
		
		if (telemetry) {
			gm.enableTelemetry(8089);
		}
		
		short inFlightCount = 20;
		int largestBlock = 1<<12;
		
		File dir=null;
	
		NoiseProducer np = null;
		
		if (encryption) {
			np = new NoiseProducer(new SecureRandom("seed".getBytes()));
		}
		
		final long rate = 2400;
		
		Pipe<PersistedBlobLoadReleaseSchema>  perLoadRelease  = PersistedBlobLoadReleaseSchema.instance.newPipe(inFlightCount, largestBlock);
		Pipe<PersistedBlobLoadConsumerSchema> perLoadConsumer = PersistedBlobLoadConsumerSchema.instance.newPipe(inFlightCount, largestBlock);
		Pipe<PersistedBlobLoadProducerSchema> perLoadProducer = PersistedBlobLoadProducerSchema.instance.newPipe(inFlightCount, largestBlock);
				
		PronghornStageProcessor proc = new PronghornStageProcessor() {

			@Override
			public void process(GraphManager gm, PronghornStage stage) {
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, stage);				
			}
			
		};
		FileGraphBuilder.buildSequentialReplayer(gm, 
				perLoadRelease, perLoadConsumer, perLoadProducer, 
				perStoreConsumer, perStoreProducer,
				inFlightCount, largestBlock, dir, np, proc);
	
		StringBuilder result0 = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, perLoadRelease, result0);		
		StringBuilder result1 = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, perLoadProducer, result1);
		StringBuilder result2 = new StringBuilder();
		ConsoleJSONDumpStage watch = ConsoleJSONDumpStage.newInstance(gm, perLoadConsumer, result2);
		
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
		return result0.toString()+result1.toString()+result2.toString();
	}
	
	
	
	
}
