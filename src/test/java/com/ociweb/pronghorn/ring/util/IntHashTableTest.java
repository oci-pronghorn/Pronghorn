package com.ociweb.pronghorn.ring.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.ring.util.hash.IntHashTable;

public class IntHashTableTest {
	
	@Test
	public void addToHashTable() {
		
		int testBits = 9;
		int extra = (1<<testBits)+1;
		
		IntHashTable ht = new IntHashTable(testBits);
		
		int j = (1<<testBits);
		while (--j>0) {			
			assertTrue(IntHashTable.setItem(ht, j, j*7));
		}
		assertFalse(IntHashTable.setItem(ht, extra, extra*7));
		
		j = (1<<testBits);
		while (--j>0) {	
			assertEquals("at position "+j,
					j*7, 
					IntHashTable.getItem(ht, j));			
		}
	}
	
}
