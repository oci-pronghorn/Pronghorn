package com.ociweb.pronghorn.stage.filter;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.BloomFilter;

public class BloomFilterTest {

    String[] testMessages1 = new String[] {"Moe","Larry","Curley"};
    String[] testMessages2 = new String[] {"Shemp","Buster"};
    
    @Test
    public void testSizes() {
        
        //30_000 items at .001 err 64K  2bytes each
        
        for(int items = 3; items<=300_000; items=items*10) {
            for(int j = 1; j<=100_000 ; j=j*10) {
                float rate = 1f/((float)j);
                BloomFilter filter = new BloomFilter(items,rate);
                
                int x = items;
                while (--x>=0) {
                    filter.addValue(Integer.toHexString(x));
                }
                
                float pctConsumed = filter.pctConsumed();
                
                //10_000_000  .00001 -> 32MB
                System.out.println(items+"  "+rate+"   "+filter.estimatedSize() +" per "+(filter.estimatedSize()/items)+" pct consumed "+pctConsumed);       
          //      System.out.println("            compressed length "+length);
                
            }
        }        
    }
    
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
