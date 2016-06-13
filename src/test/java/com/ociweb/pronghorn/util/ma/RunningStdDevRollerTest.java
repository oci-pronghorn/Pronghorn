package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RunningStdDevRollerTest {

    @Test
    public void testRollingBuckets() {
        
        int bucketsInBits = 3;//3 bits gives us 8 buckets
        int samplesPerBucket = 100;// count of values accumulated in bucket
        
        RunningStdDevRoller roller = new RunningStdDevRoller(bucketsInBits, samplesPerBucket);
        
        int loops = 3;
        
        int m = loops;
        while (--m>=0) {//loop over buckets multiple times        
            int j = (1<<bucketsInBits);//buckets
            while (--j>=0) {
                int i = samplesPerBucket;
                while (--i>=0) {
                    
                    if (0==j) {
                        roller.sample(i); //only first bucket gets these values
                    } else {
                        roller.sample(m); //the rest get this same value
                    }
                    
                }
            }
        }
        
        assertEquals(1,(int)Math.rint(roller.mean(0)));
        assertEquals(50,(int)Math.rint(roller.mean(1)));
        assertEquals(1,(int)Math.rint(roller.mean(2)));
        assertEquals(1,(int)Math.rint(roller.mean(3)));
        assertEquals(1,(int)Math.rint(roller.mean(4)));
        assertEquals(1,(int)Math.rint(roller.mean(5)));
        assertEquals(1,(int)Math.rint(roller.mean(6)));
        assertEquals(1,(int)Math.rint(roller.mean(7)));

        
    }
    
    
}
