package com.ociweb.jfast.util;

public class Stats {

    private final long[] buckets;
    private final long COMPACT_LIMIT = Long.MAX_VALUE>>1;//room for the double up process 
    
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
    public Stats(int bucketsCount, long expectedAvg) {//TODO: A, add hard limits, record max and min.
        buckets = new long[bucketsCount<<1];//must be divisible by two
        step = 1;
        min = expectedAvg - (bucketsCount>>1);
        max = min + (step*buckets.length);
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
        
        //System.err.println("sample :"+value);
        total++;
        while (value>max) {
            //grow up
            newMax();
        }
        while (value<min) {
            //grow down
            newMin();
        }
        int bIdx = (int)((value-min)/step);
        
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
        while (sum<topDownTarget && --i>=0) {
            sum += buckets[i];
        }
        return min+(step*i);        
    }
    
    public String toString() {
        double avg = (accum/(float)total);
        return "50%["+valueAtPercent(.5)+"] "
                + "80%["+valueAtPercent(.99)+"] "
                + "99%["+valueAtPercent(.99)+"] "
                + "99.9%["+valueAtPercent(.999)+"] "
                + "99.99%["+valueAtPercent(.9999)+"] "
                        + " avg:"+avg+" "
                + "Max:"+maxValue+"@"+maxValueIdx+" "
                + "Min:"+minValue+"@"+minValueIdx;
    }
    
    
    
    //TODO: do this in a read only copy of the data.
    //TODO: given a raw value return pct "Near"
    //TODO: given a raw value return pct above and/or below.
    
}
