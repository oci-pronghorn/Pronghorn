package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class MAAvgRollerLongTest {

    @Test
    public void avgTest() {
        
        MAvgRollerLong roller = new MAvgRollerLong(4);
        
        MAvgRollerLong.roll(roller, 2);
        MAvgRollerLong.roll(roller, 2);
        MAvgRollerLong.roll(roller, 2);
        MAvgRollerLong.roll(roller, 2);
                
        assertEquals(2, (int)Math.rint(MAvgRollerLong.mean(roller)));
                
    }
    
    
    @Test
    public void callTooEarlyTest() {
        
       int maSpan = 10; 
       int count = 100;       
       
       MAvgRollerLong roller = new MAvgRollerLong(maSpan);
               
       int j = count;
       int k = count;
       while (--j>=0) {
           
           double removedValue = MAvgRollerLong.roll(roller, j);
           
           if (j<90) {
               assertEquals(--k, (int)Math.rint(removedValue));
               double movingAverage = MAvgRollerLong.mean(roller);
               
           } else {               
               if (j>90) {
                   try {
                       MAvgRollerLong.mean(roller);
                       fail("should have thrown");
                   } catch (Exception e) {
                       //ignored and expected.
                   }
               }
           }
           
       }
        
    }
    
}
