package com.ociweb.pronghorn.util;

public class RunningStdDevRoller {

    public final RunningStdDev[] buckets;
    public final int samplesPerBucket;
    
    private int activeIdx;
    private int activeSampleCount;
    
    private final int bucketCount;
    private final int bucketMask;
    
    public RunningStdDevRoller(int bucketsInBits, int samplesPerBucket) {
               
        this.bucketCount = 1<<(bucketsInBits);
        this.bucketMask = bucketCount-1;
        
        this.buckets = new RunningStdDev[bucketCount];
        int i = bucketCount;
        while (--i>=0) {
            this.buckets[i] = new RunningStdDev();
        }
        
        this.samplesPerBucket = samplesPerBucket;
        this.activeSampleCount = 0;
        
    }
        
    public void sample(double value) {
      
        RunningStdDev.sample( buckets[activeIdx], value);
      
        if (++activeSampleCount == samplesPerBucket) {  
            
            activeIdx = ++activeIdx & bucketMask;
            activeSampleCount = 0;
                        
        }
    }
        
    public double mean(int bucketsBack) {
        return RunningStdDev.mean(buckets[bucketMask&(bucketCount+activeIdx-bucketsBack)]);
    }
   
    public double stdDeviation(int bucketsBack) {
        return RunningStdDev.stdDeviation(buckets[bucketMask&(bucketCount+activeIdx-bucketsBack)]);
    }
    
    public double maxSample(int bucketsBack) {
        return RunningStdDev.maxSample(buckets[bucketMask&(bucketCount+activeIdx-bucketsBack)]);
    }
    
    public double minSample(int bucketsBack) {
        return RunningStdDev.minSample(buckets[bucketMask&(bucketCount+activeIdx-bucketsBack)]);
    }
    
    public double probabilityDensity(int bucketsBack, double x) {
        return RunningStdDev.probabilityDensity(buckets[bucketMask&(bucketCount+activeIdx-bucketsBack)], x);
    }
    
    
}
