package com.ociweb.pronghorn.pipe.util.hash;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;

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
	        assertTrue(IntHashTable.hasItem(ht, j));            
	        assertTrue(0!=IntHashTable.getItem(ht, j));
			assertEquals("at position "+j,
					j*7, 
					IntHashTable.getItem(ht, j));			
		}
	}
	
    @Test
    public void visitorTest() {
        
        int testBits = 9;
        int extra = (1<<testBits)+1;
        
        IntHashTable ht = new IntHashTable(testBits);
        
        int j = (1<<testBits);
        while (--j>0) {         
            assertTrue(IntHashTable.setItem(ht, j, j*7));
        }
        //out of space
        assertFalse(IntHashTable.setItem(ht, extra, extra*7));
        
        //keep array to know if every key gets visited
        final boolean[] foundValues = new boolean[(1<<testBits)-1];
        IntHashTableVisitor visitor = new IntHashTableVisitor(){

            @Override
            public void visit(int key, int value) {
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
        
        IntHashTable ht = new IntHashTable(testBits);
        
        int j = testSize;
        while (--j>0) {         
            assertTrue(IntHashTable.setItem(ht, j, j*7));
        }
        //out of space
        assertFalse(IntHashTable.setItem(ht, extra, extra*7));
        
        
        j = testSize;
        while (--j>0) {         
            assertTrue(IntHashTable.replaceItem(ht, j, j*13));
        }
        
        j = testSize;
        while (--j>0) { 
            assertTrue(IntHashTable.hasItem(ht, j));            
            assertTrue(0!=IntHashTable.getItem(ht, j));
            assertEquals("at position "+j,
                    j*13, 
                    IntHashTable.getItem(ht, j));           
        }
    }
    
    
}
