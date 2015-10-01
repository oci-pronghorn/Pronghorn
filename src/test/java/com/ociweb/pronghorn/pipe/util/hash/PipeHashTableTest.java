package com.ociweb.pronghorn.pipe.util.hash;

import static org.junit.Assert.*;

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
		    assertTrue(PipeHashTable.hasItem(ht, j));		    
		    assertTrue(0!=PipeHashTable.getItem(ht, j));
			assertEquals("at position "+j,
					j*7, 
					PipeHashTable.getItem(ht, j));	
			
		}
	}
	
	   @Test
	    public void addToHashTableBounded() {
	        
	        int testBits = 9;
	        int testSize = (1<<testBits);
	        int extra = testSize+1;
	        
	        PipeHashTable ht = new PipeHashTable(testBits);
	        
	        
	        int j = testSize;
	        while (--j>0) {         
	            assertTrue(PipeHashTable.setItem(ht, j, j*7));
	        }
	        assertFalse(PipeHashTable.setItem(ht, extra, extra*7));

	        final int lowBoundKey = testSize/2;
	        final int lowBound = lowBoundKey*7;
	        PipeHashTable.setLowerBounds(ht,lowBound);
	        
	        j = testSize;
	        while (--j>0) { 
	            if (j>=lowBoundKey) {	            
	                assertTrue(0!=PipeHashTable.getItem(ht, j));
    	            assertTrue(PipeHashTable.hasItem(ht, j));           
    	            
    	            assertEquals("at position "+j,
    	                    j*7, 
    	                    PipeHashTable.getItem(ht, j));  
	            } else {
	                   assertFalse(PipeHashTable.hasItem(ht, j));           
	                   assertFalse(0!=PipeHashTable.getItem(ht, j));
	            }
	            
	        }
	    }
	
    @Test
    public void visitorTest() {
        
        int testBits = 9;
        int extra = (1<<testBits)+1;
        
        PipeHashTable ht = new PipeHashTable(testBits);
        
        int j = (1<<testBits);
        while (--j>0) {         
            assertTrue(PipeHashTable.setItem(ht, j, j*7));
        }
        assertFalse(PipeHashTable.setItem(ht, extra, extra*7));
        
        //keep array to know if every key gets visited
        final boolean[] foundValues = new boolean[(1<<testBits)-1];
        PipeHashTableVisitor visitor = new PipeHashTableVisitor(){

            @Override
            public void visit(long key, long value) {
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
    public void visitorBoundedTest() {
        
        int testBits = 9;
        int extra = (1<<testBits)+1;
        
        PipeHashTable ht = new PipeHashTable(testBits);
        
        int j = (1<<testBits);
        while (--j>0) {         
            assertTrue(PipeHashTable.setItem(ht, j, j*7));
        }
        assertFalse(PipeHashTable.setItem(ht, extra, extra*7));
        
        
        final int lowBoundKey = (1<<testBits)/2;
        final int lowBound = lowBoundKey*7;
        
        PipeHashTable.setLowerBounds(ht,lowBound);
                
        
        //keep array to know if every key gets visited
        final boolean[] foundValues = new boolean[(1<<testBits)-1];
        PipeHashTableVisitor visitor = new PipeHashTableVisitor(){

            @Override
            public void visit(long key, long value) {
                //check that nothing is visited below the low bound
                assertTrue(value>=lowBound);
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
            if (i+1>=lowBoundKey) {
                assertTrue(foundValues[i]);
            } else {
                assertFalse(foundValues[i]);
            }
        }
    }
	
    
    @Test
    public void addToHashTableThenReplace() {
        
        int testBits = 9;
        int testSize = (1<<testBits);
        int extra = testSize+1;
        
        PipeHashTable ht = new PipeHashTable(testBits);
        
        int j = testSize;
        while (--j>0) {         
            assertTrue(PipeHashTable.setItem(ht, j, j*7));
        }
        //out of space
        assertFalse(PipeHashTable.setItem(ht, extra, extra*7));
        
        
        j = testSize;
        while (--j>0) {         
            assertTrue(PipeHashTable.replaceItem(ht, j, j*13));
        }
        
        j = testSize;
        while (--j>0) { 
            assertTrue(PipeHashTable.hasItem(ht, j));            
            assertTrue(0!=PipeHashTable.getItem(ht, j));
            assertEquals("at position "+j,
                    j*13, 
                    PipeHashTable.getItem(ht, j));           
        }
    }
	
    
    @Test
    public void addToHashTableBoundedValuesThenReplace() {
        
        int testBits = 9;
        int testSize = (1<<testBits);
        int extra = testSize+1;
        
        PipeHashTable ht = new PipeHashTable(testBits);
        
        int j = testSize;
        while (--j>0) {         
            assertTrue(PipeHashTable.setItem(ht, j, j*7));
        }
        //out of space
        assertFalse(PipeHashTable.setItem(ht, extra, extra*7));
        
        
        final int lowBoundKey = (1<<testBits)/2;
        final int lowBound = lowBoundKey*7;
        
        PipeHashTable.setLowerBounds(ht,lowBound);
        
        j = testSize;
        while (--j>0) { 
            if (j>=lowBoundKey) {               
                assertTrue(0!=PipeHashTable.getItem(ht, j));
                assertTrue(PipeHashTable.hasItem(ht, j));           
                
                assertEquals("at position "+j,
                        j*7, 
                        PipeHashTable.getItem(ht, j));  
            } else {
                   assertFalse(PipeHashTable.hasItem(ht, j));           
                   assertFalse(0!=PipeHashTable.getItem(ht, j));
            }            
        }        
        
        
        j = testSize;
        while (--j>0) {         
            assertTrue(PipeHashTable.replaceItem(ht, j, lowBound + (j*13) ));
        }
        
        j = testSize;
        while (--j>0) { 
            assertTrue(PipeHashTable.hasItem(ht, j));            
            assertTrue(0!=PipeHashTable.getItem(ht, j));
            assertEquals("at position "+j,
                    lowBound+(j*13), 
                    PipeHashTable.getItem(ht, j));           
        }
    }
	
}
