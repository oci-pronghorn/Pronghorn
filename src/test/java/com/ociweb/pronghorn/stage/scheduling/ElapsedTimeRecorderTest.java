package com.ociweb.pronghorn.stage.scheduling;

import static org.junit.Assert.*;

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
		
		assertEquals(20,etr.elapsedAtPercentile(etr, .5f));
				
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
	
		
		assertEquals(32, etr.elapsedAtPercentile(etr, 1f));
				
	}
	
}
