package com.ociweb.pronghorn.pipe.util.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.hash.LongHashTable;

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
		    assertTrue(LongHashTable.hasItem(ht, j));            
            assertTrue(0!=LongHashTable.getItem(ht, j));
			assertEquals("at position "+j,
					j*7, 
					LongHashTable.getItem(ht, j));			
		}
	}
	
    @Test
    public void visitorTest() {
        
        int testBits = 9;
        int extra = (1<<testBits)+1;
        
        LongHashTable ht = new LongHashTable(testBits);
        
        int j = (1<<testBits);
        while (--j>0) {         
            assertTrue(LongHashTable.setItem(ht, j, j*7));
        }
        assertFalse(LongHashTable.setItem(ht, extra, extra*7));
        
        //keep array to know if every key gets visited
        final boolean[] foundValues = new boolean[(1<<testBits)-1];
        LongHashTableVisitor visitor = new LongHashTableVisitor(){

            @Override
            public void visit(long key, int value) {
                //check that the right value was found with this key
                assertEquals(key*7, value);
                //check that we only visit each key once
                assertFalse(foundValues[(int)key-1]);
                foundValues[(int)key-1]=true;
            }};
            
        ht.visit(ht, visitor );
        
        //error if we find any key that was not visited
        int i = foundValues.length;
        while (--i>=0) {
            if (!foundValues[i]) {
                fail("Did not visit key "+(i+1));
            }
        }
    }
    
    
    @Test
    public void addToHashTableThenReplace() {
        
        int testBits = 9;
        int testSize = (1<<testBits);
        int extra = testSize+1;
        
        LongHashTable ht = new LongHashTable(testBits);
        
        int j = testSize;
        while (--j>0) {         
            assertTrue(LongHashTable.setItem(ht, j, j*7));
        }
        //out of space
        assertFalse(LongHashTable.setItem(ht, extra, extra*7));
        
        
        j = testSize;
        while (--j>0) {         
            assertTrue(LongHashTable.replaceItem(ht, j, j*13));
        }
        
        j = testSize;
        while (--j>0) { 
            assertTrue(LongHashTable.hasItem(ht, j));            
            assertTrue(0!=LongHashTable.getItem(ht, j));
            assertEquals("at position "+j,
                    j*13, 
                    LongHashTable.getItem(ht, j));           
        }
    }
    
}
