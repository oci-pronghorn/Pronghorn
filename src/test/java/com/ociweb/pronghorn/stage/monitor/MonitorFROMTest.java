package com.ociweb.pronghorn.stage.monitor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class MonitorFROMTest {
	
	@Test
	public void testFROMMatchesXML() {
		String templateFile = "/ringMonitor.xml";
		String varName = "monitorFROM";	
		assertTrue(FROMValidation.testForMatchingFROMs(templateFile, varName, PipeMonitorSchema.FROM));
	};
	
	@Test
	public void testConstantFields() {
	    
	    assertTrue(FROMValidation.testForMatchingLocators(PipeMonitorSchema.instance));
	}
	

}
