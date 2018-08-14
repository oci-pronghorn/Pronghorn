package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.fail;

import org.junit.Test;

public class BucketMAAvgRollerDoubleTest {

    @Test
    public void movingAverageTest() {
        
       int maSpan = 10; 
       int granularity = 2;
       int count = 100;       
       
       BucketMAvgRollerDouble roller = new BucketMAvgRollerDouble(granularity, maSpan);
               
       double j = count;
       double k = count;
       while (--j>=0) {
           
           BucketMAvgRollerDouble.roll(roller, (double)j);
           
           if (j<90) {
               --k;
               double movingAverage = BucketMAvgRollerDouble.mean(roller);
           } else {               
               if (j>90) {
                  // try {
                       BucketMAvgRollerDouble.mean(roller); //TODO: should throw because
//                       fail("should have thrown");
//                   } catch (Exception e) {
//                       //ignored and expected.
//                   }
               }
           }
           
       }
        
    }
    
}
