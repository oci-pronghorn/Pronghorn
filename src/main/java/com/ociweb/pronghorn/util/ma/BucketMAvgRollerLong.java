package com.ociweb.pronghorn.util.ma;

//TODO: well used but still requires unit testing.

/**
 * For very large moving average windows
 * 
 * @author Nathan Tippy
 *
 */
public class BucketMAvgRollerLong {
    
    private final long[] buckets;
    private final int span;
    private final int samplesPerBucket;
    
    private long accumulator;
    private int initAccumulatorCount;
    private long bucketToSubtract;
    
    private int position;
    private int initPositionCount;
    
    private long result;
    
    /**
     * 
     * @param granularity samples per bucket in internal implementation
     * @param span total size of moving average
     */
    public BucketMAvgRollerLong(int granularity, int span) {
        //granularity is samplesPerBucket
        int bucketCount = (int)Math.ceil(span/(double)granularity);
        assert(bucketCount*granularity>=span);
        assert((bucketCount-1)*granularity<=span);
        
        this.buckets = new long[bucketCount];
        this.samplesPerBucket = granularity;
        this.span = span;
        
        this.initAccumulatorCount = span;
        this.initPositionCount = granularity;
        
    }

    public static void sample(BucketMAvgRollerLong roller, long value) {
        
        roller.accumulator+=value;
        if (--roller.initAccumulatorCount==0) {
        	roller.accumulator -= roller.bucketToSubtract;
        	roller.result = roller.accumulator;//keep this value for anyone calling
            //do this again after filling one more bucket.
            //at the same point within the next bucket
        	roller.initAccumulatorCount = roller.samplesPerBucket;
        }
        
        roller.buckets[roller.position]+=value;
        if (--roller.initPositionCount==0) {//bucket is full 
            //move position and set up to accumulate more data
        	roller.position = (0==roller.position ? roller.buckets.length: roller.position) -1;
        	roller.bucketToSubtract = roller.buckets[roller.position];
        	roller.buckets[roller.position]=0;
        	roller.initPositionCount = roller.samplesPerBucket;
        }
    }
    
    public static double mean(BucketMAvgRollerLong roller) { //probably don't want to use, passing accumulator would be more accurate.
        return roller.result/(double)roller.span;
    }
    
    public static long result(BucketMAvgRollerLong roller) {
        return roller.result;
    }
    

}
