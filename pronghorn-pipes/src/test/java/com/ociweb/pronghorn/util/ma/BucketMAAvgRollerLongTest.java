package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.fail;

import org.junit.Test;

public class BucketMAAvgRollerLongTest {

    @Test
    public void movingAverageTest() {
        
       int maSpan = 10; 
       int granularity = 2;
       int count = 100;       
       
       BucketMAvgRollerLong roller = new BucketMAvgRollerLong(granularity, maSpan);
               
       int j = count;
       int k = count;
       while (--j>=0) {
           
           BucketMAvgRollerLong.sample(roller, j);
           
           if (j<90) {
               --k;
               double movingAverage = BucketMAvgRollerLong.mean(roller);
           } else {               
               if (j>90) {
                  // try {
                       BucketMAvgRollerLong.mean(roller); //TODO: should throw because
//                       fail("should have thrown");
//                   } catch (Exception e) {
//                       //ignored and expected.
//                   }
               }
           }
           
       }
        
    }
    
}
