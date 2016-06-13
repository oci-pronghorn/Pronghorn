package com.ociweb.pronghorn.util.ma;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RunningStdDevTest {

    @Test
    public void testSimpleSlopeStdDev() {
        
       RunningStdDev r = new RunningStdDev(); 
       
       long start = System.currentTimeMillis();
       int count = 1_000_001;
       int i = count;
       while (--i>=0) {           
           r.sample(r, i);
       }
       long duration = System.currentTimeMillis()-start;
       
       int mean = 500000;
       assertEquals(mean, (int)Math.rint(RunningStdDev.mean(r)));
      
       //confirm answer with long hand compute here 
       int j = count;
       long sum = 0;
       while (--j>=0) {           
           long temp = j-mean;
           long square = temp*temp;
           sum +=square;
       }
       long variance = sum/(count-1);
       assertEquals(variance,(long)Math.rint(RunningStdDev.variance(r)));
       
       long stdDev = (long)Math.rint(Math.sqrt(variance));
       assertEquals(stdDev, (long)Math.rint(RunningStdDev.stdDeviation(r)));

       //System.out.println("duration: "+duration+" forCount: "+count+" per ms "+(((float)count)/(float)duration) );
        
        
    }
    
    
    
}
