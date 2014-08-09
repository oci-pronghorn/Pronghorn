package com.ociweb.jfast.util;

public class Stats {

    private final long[] buckets;
    private final long COMPACT_LIMIT = Long.MAX_VALUE>>1;//room for the double up process 
    
    //These are set to sane limits and should not be used for restricting the results
    //because they will invalidate the results at both ends of the spectrum.
    private final long hardMin; //These hard limits are here to protect against unbounded 
    private final long hardMax; //growth which may lead to memory issues.
    
    
    private long min;
    private long max;
    private long step;
    private long total;
    private int compactions;
    private long accum;
    private long maxValue = Long.MIN_VALUE;
    private long maxValueIdx;
    private long minValue = Long.MAX_VALUE;
    private long minValueIdx;
    
    /**
     * 
     * @param bucketBits Fixed buckets for recording the data.
     * @param lowEst May grow lower with additional data
     * @param highEst May grow larger with additional data
     */
    public Stats(int bucketsCount, long expectedAvg, long hardMin, long hardMax) {
        this.hardMin = hardMin;
        this.hardMax = hardMax;
        this.buckets = new long[bucketsCount<<1];//must be divisible by two
        this.step = 1;
        this.min = expectedAvg - (bucketsCount>>1);
        if (this.min < hardMin) {
            this.min = hardMin;
        }
        //max is allowed be larger than hardMax
        this.max = this.min + (this.step*this.buckets.length);        
    }
    
    public void sample(long value) {
        
        if (value>maxValue) {
            maxValue = value;
            maxValueIdx = total;
        }
        if (value<minValue) {
            minValue = value;
            minValueIdx = total;
        }
        
        accum +=value;
        total++;
        
        int bIdx;
        if (value>hardMax) {
            //do not grow but do count this one
            //values too large will end up pooling in the last bucket
            bIdx = buckets.length-1;            
        } else {
            if (value<hardMin) {
                //do not grow but do count this one
                //values too small will end up pooling in the first bucket
                bIdx = 0;
            } else {
                while (value>max) {
                    //grow up
                    newMax();
                }
                while (value<min) {
                    //grow down
                    newMin();
                }
                bIdx = (int)((value-min)/step);
            }
        }
        
        if (++buckets[bIdx]>COMPACT_LIMIT) {
            //compact
            compact();
        }
        
        
    }

    private void newMax() {        
      //double up everything to make it fit.
        int i = 0;
        int limit = buckets.length >> 1;
        while (i<limit) {
            int twoI =i<<1;
            buckets[i++] = buckets[twoI]+buckets[twoI+1];            
        }
        step = step<<1;
        max = min+(step*buckets.length);
    }

    private void newMin() {
        //double up everything to make it fit.        
        int i = 0;
        int limit = buckets.length >> 1;
        int top = buckets.length-1;
        while (i<limit) {
            int twoI =i<<1;            
            buckets[top-i] = buckets[top-twoI]+buckets[top-(twoI+1)];
            i++;
        }
                
        step = step<<1;
        min = max-(step*buckets.length);
        
    }

    private void compact() {
        //halve all the values in order to keep rolling with the data.
        
        total = total>>1;
        int i = buckets.length;
        while (--i>=0) {
            buckets[i] >>= 1;
        }
        
        compactions++;
    }
    
    public int compactions() {
        return compactions;
    }
    
    public long total() {
        return total;
    }
    
    public long valueAtPercent(double pct) {
        long topDownTarget = total-(long)(pct*total);
        int i = buckets.length;
        long sum = 0;
        int lastValidBucket = -1;
        while (((lastValidBucket<0) ||
                (sum<topDownTarget)) && --i>=0) {
            long count = buckets[i];
            if (count>0) {
                lastValidBucket=i;
            }
            sum += count;
        }
        return min+(step*lastValidBucket);        
    }
    
    public String toString() {
        double avg = (accum/(float)total);
        return "25%["+valueAtPercent(.25)+"] "
                + "50%["+valueAtPercent(.5)+"] "
                + "60%["+valueAtPercent(.6)+"] "
                + "70%["+valueAtPercent(.7)+"] "
                + "80%["+valueAtPercent(.99)+"] "
                + "99%["+valueAtPercent(.99)+"] "
                + "99.9%["+valueAtPercent(.999)+"] "
                + "99.99%["+valueAtPercent(.9999)+"] "
                        + " avg:"+avg+" "
                + "Min:"+minValue+" @ "+minValueIdx+" "
                + "Max:"+maxValue+" @ "+maxValueIdx+" ";
    }
    
    
    
    //TODO: do this in a read only copy of the data.
    //TODO: given a raw value return pct "Near"
    //TODO: given a raw value return pct above and/or below.
    
}
