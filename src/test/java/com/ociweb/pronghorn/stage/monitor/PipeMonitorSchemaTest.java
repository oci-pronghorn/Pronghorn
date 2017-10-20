package com.ociweb.pronghorn.stage.monitor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class PipeMonitorSchemaTest {
	
	@Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/ringMonitor.xml", PipeMonitorSchema.class));
	}

}
