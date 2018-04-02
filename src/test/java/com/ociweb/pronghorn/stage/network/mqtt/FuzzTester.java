package com.ociweb.pronghorn.stage.network.mqtt;

import com.ociweb.pronghorn.code.StageTester;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.mqtt.IdGenStage;
import com.ociweb.pronghorn.network.mqtt.MQTTClientResponseStage;
import com.ociweb.pronghorn.network.mqtt.MQTTClientToServerEncodeStage;
import com.ociweb.pronghorn.struct.BStructSchema;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FuzzTester {

	long testDuration = 200; //keep short for now to save limited time on build server
	int generatorSeed = 101;
	
	@Test
	public void testMQTTClientToServerEncodeStage() {
		assertTrue(
				StageTester.runFuzzTest(MQTTClientToServerEncodeStage.class, testDuration, generatorSeed,
						new Object[]{new ClientCoordinator(3,3, TLSCertificates.defaultCerts, new BStructSchema()),1,1}
						)
        );
	}
	
	@Ignore
	public void testMQTTClientResponseStage() {
		assertTrue(
				StageTester.runFuzzTest(MQTTClientResponseStage.class, testDuration, generatorSeed,
						new Object[]{new ClientCoordinator(3,3,TLSCertificates.defaultCerts, new BStructSchema()),1,1}
						)
        );
	}
	
	@Ignore
	public void testIdGenStage() {
		assertTrue(
				StageTester.runFuzzTest(IdGenStage.class, testDuration, generatorSeed)
        );
	}	
	
//	@Test
//	public void testMQTTClient() {
//		assertTrue(
//				StageTester.runFuzzTest(MQTTClient.class, testDuration, generatorSeed)
//        );
//	}	
	
}
