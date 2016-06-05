package com.ociweb.pronghorn.util;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

public class PoolTest {

    @Test
    public void testPool() {
        Random r = new Random(123);
       
        //build up test data
        int size = 10;
        
        
        // add <size> keys and StringBuffer members
        StringBuilder[] members = new StringBuilder[size];
        int i = size;
        long[] keys = new long[size];
        while ( --i >= 0 ) {
            members[i] = new StringBuilder(); //initial StringBuilder instance to be borrowed and returned when done.
            keys[i] = r.nextLong();
        }
                
        Pool<StringBuilder> p = new Pool<StringBuilder>(members);
                
        int j = size;
        while (--j>=0) {
            
            StringBuilder b = p.get(keys[j]);
            assertNotNull(b); //this key must not be empty
            b.append(Long.toString(keys[j]));//insert numeric value to this existing string builder
            
        }
        
        //is no more room and this next value should not have been found
        assertNull(p.get(r.nextLong()));
        
        j = size;
        while (--j>=0) {
            
            StringBuilder b = p.get(keys[j]);
            assertNotNull(b);
            assertEquals(Long.toString(keys[j]), b.toString()); //confirm we got the same one back with the same answer.
                               
        }
                
        assertEquals(size, p.locks());
        
        assertNull(p.get(r.nextLong()));
        
                
        j = size;
        while (--j>=0) {     //release all the keys       
            p.release(keys[j]);
            assertEquals(j, p.locks()); //should be one less lock for each iteration.
        }

        assertEquals(0, p.locks()); //all the locks have been released
        
               
    }
    
    
}
