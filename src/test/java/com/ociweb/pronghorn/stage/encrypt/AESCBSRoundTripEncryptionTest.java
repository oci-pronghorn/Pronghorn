package com.ociweb.pronghorn.stage.encrypt;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class AESCBSRoundTripEncryptionTest {

	@Test
	public void simpleTest() {
		
		///////
		///fake password
		///////
		byte[] pass = new byte[16];
		Random r= new Random(123);
		r.nextBytes(pass);
		
		////////////
		//build graph
		//data -> enrypt -> decrypt -> test
		//////////
		Pipe<RawDataSchema> testDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		testDataPipe.initBuffers();
		
		Pipe<RawDataSchema> encryptedDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		Pipe<RawDataSchema> resultDataPipe = RawDataSchema.instance.newPipe(10, 1000);
		
		GraphManager gm = new GraphManager();
		StringBuilder results = new StringBuilder();
		
		RawDataCryptAESCBCPKCS5Stage encrpt = new  RawDataCryptAESCBCPKCS5Stage(gm, pass, true, testDataPipe, encryptedDataPipe);
		RawDataCryptAESCBCPKCS5Stage decrypt = new  RawDataCryptAESCBCPKCS5Stage(gm, pass, false, encryptedDataPipe, resultDataPipe);
		ConsoleJSONDumpStage lastStage = ConsoleJSONDumpStage.newInstance(gm, resultDataPipe, results);
		
		/////////////////////
		///test data
		///////////////////////
		int j = 2;
		while (--j>=0) {
			
			//exactly 16 in size, same as block
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray(new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			
			//something shorter than block, will exercise the padding logic
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("helloworld".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			
			//something longer than block
			Pipe.addMsgIdx(testDataPipe, 0);
			Pipe.addByteArray("This is a long message which takes up multiple blocks.".getBytes(), testDataPipe);
			Pipe.confirmLowLevelWrite(testDataPipe);
			Pipe.publishWrites(testDataPipe);
			
		}
		Pipe.spinBlockForRoom(testDataPipe, Pipe.EOF_SIZE);
		Pipe.publishEOF(testDataPipe);
		///////
		//run
		////////
		
		boolean debug = false;
		if (debug) {
			MonitorConsoleStage.attach(gm);
		}
		
		NonThreadScheduler s = new NonThreadScheduler(gm);
			
		
		s.startup();
		long timeout = System.currentTimeMillis()+10_000;
		while (!GraphManager.isStageTerminated(gm, lastStage.stageId) && System.currentTimeMillis()<timeout) {
			
			s.run();
		}
		
		s.shutdown();		
		
		assertTrue(results.toString(),results.indexOf("0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f")!=-1);
		assertTrue(results.toString(),results.indexOf("0x68,0x65,0x6c,0x6c,0x6f,0x77,0x6f,0x72,0x6c,0x64")!=-1);
	
		
	}
	
	
}
