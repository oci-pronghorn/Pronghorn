package com.ociweb.pronghorn.stage.filter;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

public class BloomFilterTest {

    String[] testMessages1 = new String[] {"Moe","Larry","Curley"};
    String[] testMessages2 = new String[] {"Shemp","Buster"};
    
    
    
    @Test
    public void testAddAndFetch() {
        
        BloomFilter filter = new BloomFilter(1000, .00000001);
        
        //build up the filter with the known values.
        int i = testMessages1.length;
        while (--i>=0) {
            filter.addValue(testMessages1[i]);
        }
        
        //confirm expected        
        assertTrue(filter.mayContain(testMessages1[0]));
        assertTrue(filter.mayContain(testMessages1[1]));
        assertTrue(filter.mayContain(testMessages1[2]));
        assertFalse(filter.mayContain(testMessages2[0]));
        assertFalse(filter.mayContain(testMessages2[1]));
                        
    }
    
    @Ignore
    public void testSize() {
        
        double x = .1;
        int c = 21;
        while (--c>=0) {
        
            BloomFilter filter = new BloomFilter(100L*1000L*1000L, x);
            
            long bytes = filter.estimatedSize();
            
            System.out.println((bytes/(1024*1024))+"m  "+x);
        
            x = x/10d;
        }
    }
    
    
}
