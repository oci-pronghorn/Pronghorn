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
        StringBuilder[] members = new StringBuilder[size];
        int i = size;
        long[] keys = new long[size];
        while ( --i >= 0 ) {
            members[i] = new StringBuilder();
            keys[i] = r.nextLong();
        }
                
        Pool<StringBuilder> p = new Pool<StringBuilder>(members);
                
        int j = size;
        while (--j>=0) {
            
            StringBuilder b = p.get(keys[j]);
            assertNotNull(b);
            b.append(Long.toString(keys[j]));
            
        }
        
        //is no more room
        assertNull(p.get(r.nextLong()));
        
        j = size;
        while (--j>=0) {
            
            StringBuilder b = p.get(keys[j]);
            assertNotNull(b);
            assertEquals(Long.toString(keys[j]), b.toString());
       
            
        }
        
        p.release(keys[0]);        
        assertNotNull(p.get(r.nextLong()));
               
    }
    
    
}
