package com.ociweb.pronghorn.stage.blocking;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class BlockingSupportTest {

	
	@Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/BlockingWorkInProgress.xml", BlockingWorkInProgressSchema.class));
	}
	
}
