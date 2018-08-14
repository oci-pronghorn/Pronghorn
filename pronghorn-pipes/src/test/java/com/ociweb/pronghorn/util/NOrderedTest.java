package com.ociweb.pronghorn.util;

import java.util.Random;

import org.junit.Test;

public class NOrderedTest {

    
    @Test
    public void testOrder() {
        
        int size = 3;//only show top 3
        int testSize = 1000;
        
        NOrdered n = new NOrdered(size);
                
        Random r = new Random();
        
        String[] data = {"first","second","third","fourth","fifth","sixth","seventh","eighth"};
        
        int j = testSize;
        while (--j>=0) {
            
            int idx = r.nextInt()&0x07;
            
            n.sample(data[idx]);
            
        }
    
        StringBuilder target = new StringBuilder();
        n.writeTo(target);
        //System.out.println(target);
    
        
        
    }
    
}
