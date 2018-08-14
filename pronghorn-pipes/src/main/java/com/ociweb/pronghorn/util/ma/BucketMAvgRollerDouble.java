package com.ociweb.pronghorn.util.ma;


/**
 * For very large moving average windows
 * 
 * @author Nathan Tippy
 *
 */
public class BucketMAvgRollerDouble {
    
    private final double[] buckets;
    private int bucketCount;
    private boolean reduceRing;
    
    private final int span;
    private final int alignment;
    private final int samplesPerBucket;
    private final int eccWait;
    private final int initalAlignmentBucket;
    
    private double eccAccumulator;
    private int initEccCount;
    private int initEccWait;
    
    private double accumulator;
    private int accumulatorCountDown;
    private int accumulatorSize;
    private double bucketToSubtract;
    
    private int position;
    private int initPositionCount;
    
    private double result=0;
    
    //TODO: well used but still requires unit testing.
    
    public BucketMAvgRollerDouble(int granularity, int span) {
        this(granularity,span,0);
    }
    
    /**
     * 
     * @param granularity samples per bucket internal implementation
     * @param span size of moving average
     * @param alignment initial count in first bucket
     */
    public BucketMAvgRollerDouble(int granularity, int span, int alignment) {
        //granularity is samplesPerBucket
        int remainder = span%granularity;
        this.reduceRing = alignment>0 && (remainder>=alignment || remainder==0);
        this.bucketCount = (reduceRing ? 1 : 0 )
                            +(int)Math.ceil(span/(double)granularity);
        
        //must start at index one less than last so that when first span
        //is completed we end up at the very last valid index.  This is
        //required in order to reduce the ring if needed at that time.
        this.position = bucketCount-2;
        
        this.buckets = new double[bucketCount];
        this.samplesPerBucket = granularity;
        this.span = span;
        this.alignment = alignment;
        
        this.accumulatorCountDown = span;
        this.accumulatorSize = span;
        
        assert(alignment<=samplesPerBucket);
        this.initalAlignmentBucket = alignment==0 ? samplesPerBucket : alignment;//must be smaller than samples per bucket
        assert(initalAlignmentBucket>=0) : "initial alignment must be >= zero, samplesPerBucket "+samplesPerBucket+" alignment "+alignment;
        
        this.initPositionCount = initalAlignmentBucket;// samplesPerBucket;
     
        this.initEccCount = span;
        this.eccWait = granularity - (span%granularity);
        this.initEccWait = initalAlignmentBucket + span + eccWait;//first one must be after full span
        
    }

    public static int span(BucketMAvgRollerDouble roller) {
        return roller.span;
    }
    
    public static int bucketsRepeatAfter(BucketMAvgRollerDouble roller) {
        return roller.span+roller.alignment;
    }

    public static void roll(BucketMAvgRollerDouble roller, double value) {

        
        //NOTE: both bucketToSubtract and accumulator MUST have same count of samples however.
        //the 1th bucket of each can be a smaller number of samples than the rest to ensure
        //the alignment with the smaller moving averages. The zero bucket must be span length.

        boolean first=false;
        roller.accumulator+=value;
        if (--roller.accumulatorCountDown==0) {
            //do this again after filling one more bucket.
            //at the same point within the next bucket
            first = 0==roller.result && 0==roller.bucketToSubtract;
            roller.accumulatorCountDown = (first ? roller.initalAlignmentBucket : roller.samplesPerBucket);
            roller.accumulatorSize = roller.accumulatorCountDown;
            
            roller.result = roller.result + roller.accumulator - roller.bucketToSubtract;
            //mark this as used for both safety check and accumulation on ringReduction
            roller.bucketToSubtract = 0;
            //reset accumulator
            roller.accumulator = 0;
        }

        //error correction, a total starting from zero will replace result every span+eccWait
        //to minimize any possible drift from the ongoing value changes.
        if (--roller.initEccWait<0) {
            //System.err.println("add:"+value);
        	roller.eccAccumulator+=value;
            if (--roller.initEccCount==0) {
            	roller.result = roller.eccAccumulator; //this may not line up with time cycle above!!
            	roller.eccAccumulator = 0;
            	roller.initEccCount = roller.span;
                //don't start accumulating until here that way it lines up with bucket size boundary.
            	roller.initEccWait = roller.eccWait;
            }
        } 
        
        //if this is the first bucket with offset; that small one takes up 
        //a full spot that was alloted to a full bucket so we may run out of buckets.
        //to solve this we need one extra bucket first time around then shrink
        //to achieve this the bucketCount will be decremented on first use.
        
        roller.buckets[roller.position] += value;

        if (--roller.initPositionCount==0) {//bucket is full 
            //move position and set up to accumulate more data
        	roller.position = (0==roller.position ? roller.bucketCount: roller.position) -1;
            
            //oldest bucket we are about to write over, keep for subtract
        	roller.bucketToSubtract = roller.buckets[roller.position];
        	roller.buckets[roller.position] = 0;//clear to begin accumulation here.
            
        	roller.initPositionCount = roller.samplesPerBucket;
        }
        
        if (roller.reduceRing && first) {
            //accumulated so far
            double curTotal = roller.buckets[roller.position];
            //shorten the length of the ring
            roller.bucketCount--;
            //move down to next position
            roller.position = roller.bucketCount-1;//(0==position ? bucketCount: position) -1;
            //keep value here as next to subtract
            //Usually already zero unless we exactly hit the most recent intPositionCount==0
            roller.bucketToSubtract += roller.buckets[roller.position];
            //put accumulated so far into this index as we count down
            roller.buckets[roller.position] = curTotal;//continue totaling from here
            //never do this again
            roller.reduceRing = false;
        }

    }
    
    public static double mean(BucketMAvgRollerDouble roller) { //probably don't want to use, passing accumulator would be more accurate.
        return roller.result/(double)roller.span;
    }
    
    public static double result(BucketMAvgRollerDouble roller) {
        return roller.result;
    }
    
    /**
     * only returns new means or NaN
     */
    public static double newMean(BucketMAvgRollerDouble roller) {
        if (0==roller.accumulator){
            return mean(roller);
        } else {
            return Double.NaN;
        }
        
    }
    
    public static int bucketFill(BucketMAvgRollerDouble roller) {
        return roller.accumulatorSize - roller.accumulatorCountDown;
    }
    
    public static int bucketSize(BucketMAvgRollerDouble roller) {
        return roller.accumulatorSize;
    }
    

}
