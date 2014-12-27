package com.ociweb.pronghorn.ring.util;


//Needs work to clean it up and make it more effecient but it gets the job done in general.

public class Histogram {

    private final long[] buckets;
    private final long COMPACT_LIMIT = Long.MAX_VALUE>>1;//room for the double up process 
    
    //These are set to sane limits and should not be used for restricting the results
    //because they will invalidate the results at both ends of the spectrum.
    private final long hardMin; //These hard limits are here to protect against unbounded 
    private final long hardMax; //growth which may lead to memory issues.
    
    
    private long min;
    private long max;
    private long step;
    private long sampleCount;
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
    public Histogram(int bucketsCount, long expectedAvg, long hardMin, long hardMax) {
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
    
    public static long accumulatedTotal(Histogram me) {
    	return me.accum;
    }
    
    public static long sampleCount(Histogram me) {
        return me.sampleCount;
    }
    
    public static void sample(long value, Histogram me) {
		//TODO: B, rewite smaller/faster
    	
		me.accum +=value;
		me.sampleCount++;

		if (value>me.maxValue) {
			me.maxValue = value;
			me.maxValueIdx = me.sampleCount;
        }
        if (value<me.minValue) {
        	me.minValue = value;
        	me.minValueIdx = me.sampleCount;
        }
                
        int bIdx;
        if (value>me.hardMax) {
            //do not grow but do count this one
            //values too large will end up pooling in the last bucket
            bIdx = me.buckets.length-1;            
        } else {
            if (value<me.hardMin) {
                //do not grow but do count this one
                //values too small will end up pooling in the first bucket
                bIdx = 0;
            } else {
                while (value>me.max) {
                    //grow up
                	me.newMax();
                }
                while (value<me.min) {
                    //grow down
                	me.newMin();
                }
                bIdx = (int)((value-me.min)/me.step);
            }
        }
        
        if (++me.buckets[bIdx]>me.COMPACT_LIMIT) {
            //compact
        	compact(me);
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

    private static void compact(Histogram me) {
        //halve all the values in order to keep rolling with the data.
        
        me.sampleCount = me.sampleCount>>1;
        int i = me.buckets.length;
        while (--i>=0) {
        	me.buckets[i] >>= 1;
        }
        
        me.compactions++;
    }
    
    public int compactions() {
        return compactions;
    }
   
    
    public long valueAtPercent(double pct) {
        long topDownTarget = sampleCount-(long)(pct*sampleCount);
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
        double avg = (accum/(float)sampleCount);
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
    
        
}
