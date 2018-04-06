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
		
			
		assertEquals(32,etr.elapsedAtPercentile(etr, .5f));
		//this is an estimate so this value is ok.
		assertEquals(16,etr.elapsedAtPercentile(etr, .16f));	
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
		assertEquals(32, etr.elapsedAtPercentile(etr, 1f));
				
	}
	
	
	
	@Test
	public void nominalTest() {
		
		ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
	
		etr.record(etr, 100);
		etr.record(etr, 100);
		etr.record(etr, 120);
		etr.record(etr, 120);
//should be 120 for 50%
		etr.record(etr, 130);
		etr.record(etr, 130);
		etr.record(etr, 400);
		etr.record(etr, 400);
	
		//this is an estimate so this value is ok
		assertEquals(128, ElapsedTimeRecorder.elapsedAtPercentile(etr, .5f));
				
	}
	
	
}
