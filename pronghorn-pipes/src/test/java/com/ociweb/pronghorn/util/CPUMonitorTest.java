package com.ociweb.pronghorn.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Ignore;

public class CPUMonitorTest {

    @Ignore
    public void exmapleUsageTest() {
       
        
        CPUMonitor monitor = new CPUMonitor(200); //5 per second
        
        monitor.start();
        
        try {
            Thread.sleep(900);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
           return;
        }
                
        SmallFootprintHistogram histogram = monitor.stop();
        assertTrue(null!=histogram);
        
        if (SmallFootprintHistogram.totalCount(histogram)>0) {//some platforms do not support this monitor.
            assertEquals(5, SmallFootprintHistogram.totalCount(histogram));
        }
        PrintStream printStream = new PrintStream(new ByteArrayOutputStream());
        
        
        histogram.report(printStream);
        //System.out.println(histogram);
        
    }
    
}
