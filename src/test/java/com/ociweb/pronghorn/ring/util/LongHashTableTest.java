package com.ociweb.pronghorn.ring.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.ring.util.hash.IntHashTable;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;

public class LongHashTableTest {
	
	@Test
	public void addToHashTable() {
		
		int testBits = 9;
		int extra = (1<<testBits)+1;
		
		LongHashTable ht = new LongHashTable(testBits);
		
		int j = (1<<testBits);
		while (--j>0) {			
			assertTrue(LongHashTable.setItem(ht, j, j*7));
		}
		assertFalse(LongHashTable.setItem(ht, extra, extra*7));
		
		j = (1<<testBits);
		while (--j>0) {	
			assertEquals("at position "+j,
					j*7, 
					LongHashTable.getItem(ht, j));			
		}
	}
	
}
