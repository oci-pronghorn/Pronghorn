package com.ociweb.pronghorn.stage.filter;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.RollingBloomFilter;

public class RollingBloomFilterTest {

    String[] testMessages1 = new String[] {"Moe","Larry","Curley"};
    String[] testMessages2 = new String[] {"Shemp","Buster"};
    
    @Ignore
    public void testSizes() {
        
        //30_000 items at .001 err 64K  2bytes each
        
        for(int items = 3; items<=300_000; items=items*10) {
            for(int j = 10; j<=1_000 ; j=j*10) {
                float rate = 1f/((float)j);
                RollingBloomFilter filter = new RollingBloomFilter(items,rate);
                
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
        
    	RollingBloomFilter filter = new RollingBloomFilter(1000, .00000001);
        
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
    
    @Test
    public void testRollingTest() {
    	
    	String[] testValues = new String[] {"00","01","02","03","04","05","06","07","08","09","10"};
    	
    	BloomFilter filter = new RollingBloomFilter(10, .00000001);
    	
    	
    	int i = testValues.length;
    	while (--i>=0) {
    		filter.addValue(testValues[i]);
    		assertTrue(filter.mayContain(testValues[i]));
    	}
    	
//    	 i = testValues.length;
//     	while (--i>=0) {     		
//     		System.out.println("  "+filter.mayContain(testValues[i]));
//     	}
    	
    	assertTrue(filter.mayContain(testValues[0]));
    	assertFalse(filter.mayContain(testValues[9]));
    	
    	
    	
    	
    }

    
    
}
