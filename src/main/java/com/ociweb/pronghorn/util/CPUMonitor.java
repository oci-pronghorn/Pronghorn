package com.ociweb.pronghorn.util;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * Simple to use CPU Usage Monitor  
 *  1. Create instance and optionally pass in period in ms;
 *  2. Call Start to begin recording CPU usage;
 *  3. Call Stop to end recording and get the histogram.
 *  4. Take the histogram and export the results. 
 *     Use CPUMonitor.UNIT_SCALING_RATIO for the outputPercentile methods
 * 
 * Nathan Tippy
 */
public class CPUMonitor {
    
    private final static Logger log = LoggerFactory.getLogger(CPUMonitor.class);
    
    private Histogram histogram;
    private final ScheduledExecutorService scheduledExecutor;
    private final long period;
    private final boolean monitorEntireSystem;
    
    private static final long ONE_HUNDRED_PERCENT = 1000000000; 
    public static final double UNIT_SCALING_RATIO = CPUMonitor.ONE_HUNDRED_PERCENT/100d;
    
    public CPUMonitor() {
        this(100);
    }
    
    public CPUMonitor(long periodInMS) {
        this(periodInMS, false);
        if (periodInMS<100) {
            log.warn("This period may be so fast that the CPU results can not be captured accuratly, slow it down to a value 100 or greater.");
        }
        if (periodInMS<20) {
            throw new UnsupportedOperationException("Period should be 100 or greater and must be 20 or greater.");
        }
    }
    
    public CPUMonitor(long periodInMS, boolean monitorEntireSystem) {
        this.scheduledExecutor = Executors.newScheduledThreadPool(1);
        this.period = periodInMS;
        this.monitorEntireSystem = monitorEntireSystem;
    }
    
    public void start() {       
       scheduledExecutor.scheduleAtFixedRate(new Watcher(this), 0, period, TimeUnit.MILLISECONDS);
    }
    
    public Histogram stop() {
        scheduledExecutor.shutdownNow();
        histogram.setEndTimeStamp(System.currentTimeMillis());
        return histogram;
    }
    
    private static class Watcher implements Runnable {
        
        private String[] id;
        private MBeanServer mbs;
        private ObjectName os;
        private CPUMonitor that;
        
        
        Watcher(CPUMonitor that) {
            this.that = that;
        }

        private void init() {
            that.histogram = new Histogram(ONE_HUNDRED_PERCENT, 4);  //TODO: Bug do not set this to 6 or it will hang.              
            this.mbs    = ManagementFactory.getPlatformMBeanServer();
            
            this.id = (that.monitorEntireSystem ? new String[]{ "SystemCpuLoad" } : new String[]{ "ProcessCpuLoad" });
            
            try {
                this.os = ObjectName.getInstance("java.lang:type=OperatingSystem");
            } catch (MalformedObjectNameException e) {
                log.warn("looking up os", e);
                this.os = null;
            } catch (NullPointerException e) {
                log.warn("looking up os", e);
                this.os = null;
            }
            that.histogram.setStartTimeStamp(System.currentTimeMillis());
        }
        
        @Override
        public void run() {            
            if (null==id) {
                init();
            }
            getCPULoad(id);
            
        }
        
        private void getCPULoad(String[] attrib) {
            try {
                if (null==os) {
                    return;
                }
                
                AttributeList list = mbs.getAttributes(os, attrib);            
                if (list.isEmpty()) {
                    return;
                }
            
                Double value  = (Double)((Attribute)list.get(0)).getValue();                
                if (value == -1.0) {
                    return;  // usually takes a couple of seconds before we get real values
                } else {
                    long longValue = (long)(ONE_HUNDRED_PERCENT * value.doubleValue());
                    that.histogram.recordValue(longValue);   
                }
            } catch (Exception e) {
                log.warn("unable to fetch CPU usage", e);
                return;
            }
        }
        
    }

    
    
}
