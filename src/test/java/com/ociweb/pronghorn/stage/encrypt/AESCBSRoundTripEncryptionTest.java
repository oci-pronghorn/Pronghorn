package com.ociweb.pronghorn.stage.encrypt;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Ignore;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.BlockStorageStage;
import com.ociweb.pronghorn.stage.file.FileBlobReadStage;
import com.ociweb.pronghorn.stage.file.FileBlobWriteStage;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class AESCBSRoundTripEncryptionTest {

	@Ignore
	public void roundTripAES() {
		//tests in parts and as a full file in one go.
		
		///////
		///fake password
		///////
		byte[] pass = new byte[16];
		Random r= new Random(123);
		r.nextBytes(pass);
				
		
		String blockFilePath = null;
		File tempFile = null;
		try {
			tempFile = File.createTempFile("aes", "test");
			blockFilePath = File.createTempFile("aesDoFinal", "test").getAbsolutePath();
		} catch (IOException e) {			
			e.printStackTrace();
		}
		
			
		////////////
		//build graph
		//data -> enrypt -> decrypt -> test
		//////////
		Pipe<RawDataSchema> testDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		testDataPipe.initBuffers();
		
		Pipe<RawDataSchema> encryptedDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		Pipe<RawDataSchema> encryptedDataPipeA = RawDataSchema.instance.newPipe(20, 2000);
		Pipe<RawDataSchema> encryptedDataPipeB = RawDataSchema.instance.newPipe(20, 2000);
		Pipe<RawDataSchema> resultDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		
		GraphManager gm = new GraphManager();
		StringBuilder results = new StringBuilder();
		
		Pipe<BlockStorageReceiveSchema> doFinalInput1 = BlockStorageReceiveSchema.instance.newPipe(10, 1000);
		Pipe<BlockStorageXmitSchema> doFinalOutput1 = BlockStorageXmitSchema.instance.newPipe(10, 1000);
		
		Pipe<BlockStorageReceiveSchema> doFinalInput2 = BlockStorageReceiveSchema.instance.newPipe(10, 1000);
		Pipe<BlockStorageXmitSchema> doFinalOutput2 = BlockStorageXmitSchema.instance.newPipe(10, 1000);
		
		BlockStorageStage.newInstance(gm, blockFilePath, 
				                     new Pipe[]{doFinalOutput1,doFinalOutput2},
				                     new Pipe[]{doFinalInput1,doFinalInput2});
		
		RawDataCryptAESCBCPKCS5Stage encrpt = new  RawDataCryptAESCBCPKCS5Stage(gm, pass, true, 
				                                                                testDataPipe, 
				                                                                encryptedDataPipe,
				                                                                doFinalInput2,
				                                                                doFinalOutput2);
	
		ReplicatorStage.newInstance(gm, encryptedDataPipe, encryptedDataPipeA, encryptedDataPipeB);
		

		FileBlobWriteStage write = new FileBlobWriteStage(gm,encryptedDataPipeA,false, tempFile.getAbsolutePath());
						
		
		RawDataCryptAESCBCPKCS5Stage decrypt = new  RawDataCryptAESCBCPKCS5Stage(gm, pass, false, 
				                                                                 encryptedDataPipeB, 
				                                                                 resultDataPipe,
				                                                                 doFinalInput1,
				                                                                 doFinalOutput1);
		
		ConsoleJSONDumpStage lastStage = ConsoleJSONDumpStage.newInstance(gm, resultDataPipe, results, true);
		
		/////////////////////
		///test data
		///////////////////////
		int j = 1;
		while (--j>=0) {
			
			//exactly 16 in size, same as block
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("ABCDEFGHIJKLMNOP".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("qrstuvwxyz12345678".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			

			//something longer than block
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("This is a long message which takes up multiple blocks.".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);

			//something shorter than block, will exercise the padding logic
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("helloworld".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			
			
			//reset the encryption
			//flush the writes so far, eg end of stream, close and finalize the data.
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addNullByteArray(testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
//			
//			Pipe.addMsgIdx(testDataPipe, 0);
//			Pipe.addByteArray("after reset".getBytes(), testDataPipe);
//			Pipe.confirmLowLevelWrite(testDataPipe);
//			Pipe.publishWrites(testDataPipe);
			
		}
		Pipe.publishEOF(testDataPipe); //this is the Shutdown signal
		
		///////
		//run
		///////
		
		boolean debug = false;
		if (debug) {
			MonitorConsoleStage.attach(gm);
		}
		results.setLength(0);
		results.append("individual messages: ");
		{
			NonThreadScheduler s = new NonThreadScheduler(gm);		
			
			s.startup();
			long timeout = System.currentTimeMillis()+10_000;
			while (!GraphManager.isStageTerminated(gm, lastStage.stageId) && System.currentTimeMillis()<timeout) {
					s.run();
			}
			
			s.shutdown();		
						
			System.err.println(results);
			
			//TODO: add back in once complete.
		//	assertTrue(results.toString(),results.indexOf("This is a long message which takes up multiple blocks.")!=-1);
		//	assertTrue(results.toString(),results.indexOf("ABCDEFGHIJKLMNOP")!=-1);
			
			
			
		}
		
		///////////
		///new graph to read the file, the file allows for all the data to come in as 1 chunk.
		///////////
		GraphManager gm2 = new GraphManager();
		
		Pipe<RawDataSchema> encryptedDataPipe2 = RawDataSchema.instance.newPipe(10, 1000);
		Pipe<RawDataSchema> resultDataPipe2 = RawDataSchema.instance.newPipe(10, 1000);
				
		Pipe<BlockStorageReceiveSchema> doFinalInput3 = BlockStorageReceiveSchema.instance.newPipe(10, 1000);
		Pipe<BlockStorageXmitSchema> doFinalOutput3 = BlockStorageXmitSchema.instance.newPipe(10, 1000);
		
		BlockStorageStage.newInstance(gm2, blockFilePath, doFinalOutput3, doFinalInput3);
				
		results.setLength(0);
		results.append("single large message: ");
		
		FileBlobReadStage read= new FileBlobReadStage(gm2, encryptedDataPipe2, tempFile.getAbsolutePath());
		RawDataCryptAESCBCPKCS5Stage decrypt2 = new  RawDataCryptAESCBCPKCS5Stage(gm2, pass, false,
				                                                                  encryptedDataPipe2, resultDataPipe2,
				                                                                  doFinalInput3, doFinalOutput3
															);
		
		ConsoleJSONDumpStage lastStage2 = ConsoleJSONDumpStage.newInstance(gm2, resultDataPipe2, results, true);
	
		{
			NonThreadScheduler s = new NonThreadScheduler(gm2);		
			
			s.startup();
			long timeout = System.currentTimeMillis()+10_000;
			while (!GraphManager.isStageTerminated(gm2, lastStage2.stageId) && System.currentTimeMillis()<timeout) {
					s.run();
			}
			
			s.shutdown();		
						
			System.err.println(results);
			
			assertTrue(results.toString(),results.indexOf("This is a long message which takes up multiple blocks.")!=-1);
			assertTrue(results.toString(),results.indexOf("ABCDEFGHIJKLMNOP")!=-1);
			
			
			
		}
		
		
	}
	
	
}
