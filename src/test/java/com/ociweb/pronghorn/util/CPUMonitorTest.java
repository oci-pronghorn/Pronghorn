package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.HdrHistogram.Histogram;
import org.junit.Test;

public class CPUMonitorTest {

    @Test
    public void exmapleUsageTest() {
       
        
        CPUMonitor monitor = new CPUMonitor(200); //5 per second
        
        monitor.start();
        
        //do you work here
        try {
            Thread.sleep(900);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
           return;
        }
                
        Histogram histogram = monitor.stop();
        assertTrue(null!=histogram);
        if (histogram.getTotalCount()>0) {//some platforms do not support this monitor.
            assertEquals(5, histogram.getTotalCount());
        }
        PrintStream printStream = new PrintStream(new ByteArrayOutputStream());
        
        histogram.outputPercentileDistribution(printStream, CPUMonitor.UNIT_SCALING_RATIO);
        
        
    }
    
}
