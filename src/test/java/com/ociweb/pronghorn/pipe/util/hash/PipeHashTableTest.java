package com.ociweb.pronghorn.pipe.util.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.hash.PipeHashTable;

public class PipeHashTableTest {
	
	@Test
	public void addToHashTable() {
		
		int testBits = 9;
		int extra = (1<<testBits)+1;
		
		PipeHashTable ht = new PipeHashTable(testBits);
		
		int j = (1<<testBits);
		while (--j>0) {			
			assertTrue(PipeHashTable.setItem(ht, j, j*7));
		}
		assertFalse(PipeHashTable.setItem(ht, extra, extra*7));
		
		j = (1<<testBits);
		while (--j>0) {	
			assertEquals("at position "+j,
					j*7, 
					PipeHashTable.getItem(ht, j));			
		}
	}
	
}
