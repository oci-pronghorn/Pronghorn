package com.ociweb.pronghorn.stage.monitor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class PipeMonitorSchemaTest {
	
	@Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.testForMatchingFROMs("/ringMonitor.xml", "FROM", PipeMonitorSchema.FROM));
	};
	
	@Test
	public void testConstantFields() {
	    
	    assertTrue(FROMValidation.testForMatchingLocators(PipeMonitorSchema.instance));
	}
	

}
