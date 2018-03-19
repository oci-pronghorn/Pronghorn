package com.ociweb.pronghorn.stage.scheduling;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ElapsedTimeRecorderTest {

	@Test
	public void simpleTest() {
		
		ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
		
		etr.record(etr, 8);
		etr.record(etr, 8);
		
		etr.record(etr, 32);
		etr.record(etr, 32);
		etr.record(etr, 32);
		etr.record(etr, 32);
		
		//this is an estimate so this value is ok.
		assertEquals(40,etr.elapsedAtPercentile(etr, .5f));
				
	}
	
	@Test
	public void simpleEmpty() {
		
		ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
				
		assertEquals(0, etr.elapsedAtPercentile(etr, .5f));
				
	}
	
	@Test
	public void recordZero() {
		
		ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
	
		etr.record(etr, 0);
			
		assertEquals(0, etr.elapsedAtPercentile(etr, .5f));
				
	}
	
	
	
	@Test
	public void simpleFull() {
		
		ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
	
		etr.record(etr, 32);
		etr.record(etr, 32);
	
		//this is an estimate so this value is ok
		assertEquals(64, etr.elapsedAtPercentile(etr, 1f));
				
	}
	
}
