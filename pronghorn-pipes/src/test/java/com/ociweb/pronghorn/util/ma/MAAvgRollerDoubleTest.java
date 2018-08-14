package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.*;

import org.junit.Test;

public class MAAvgRollerDoubleTest {

    @Test
    public void movingAverageTest() {
        
       int maSpan = 10; 
       int count = 100;       
       
       MAvgRollerDouble roller = new MAvgRollerDouble(maSpan);
               
       int j = count;
       int k = count;
       while (--j>=0) {
           
           double removedValue = MAvgRollerDouble.roll(roller, j);
           
           if (j<90) {
               assertEquals(--k, (int)Math.rint(removedValue));
               double movingAverage = MAvgRollerDouble.mean(roller);
           } else {               
               if (j>90) {
                   try {
                       MAvgRollerDouble.mean(roller);
                       fail("should have thrown");
                   } catch (Exception e) {
                       //ignored and expected.
                   }
               }
           }
           
       }
        
    }
    
}
