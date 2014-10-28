package com.ociweb.jfast.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestBranchless {
	
	@Test
	public void int32test() {
		int j = -1000;
		while (j<1000) {
			
			int expected = j>=0 ? j+1 : j;
			assertEquals(expected,incWhenPositive32(j));
			
			j++;
		}
		
	}
	
	private int incWhenPositive32(int value) {
		return (1+(value + (value >> 31)));
	}
	
	@Test
	public void int64test() {
		int j = -1000;
		while (j<1000) {
			
			int expected = j>=0 ? j+1 : j;
			assertEquals(expected,incWhenPositive64(j));
			
			j++;
		}
		
	}
	
	private int incWhenPositive64(int value) {
		return (1+(value + (value >> 63)));
	}
	
}
