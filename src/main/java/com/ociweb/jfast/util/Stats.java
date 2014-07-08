package com.ociweb.jfast.util;

public class Stats {

    private final long[] buckets;
    private final long COMPACT_LIMIT = Long.MAX_VALUE>>1;//room for the double up process 
    
    private long min;
    private long max;
    private long step;
    private long total;
    
    /**
     * 
     * @param bucketBits Fixed buckets for recording the data.
     * @param lowEst May grow lower with additional data
     * @param highEst May grow larger with additional data
     */
    Stats(int bucketBits, long lowEst, long highEst) {
        buckets = new long[1<<bucketBits];
        min = lowEst;
        max = highEst;
        step = (max-min)/buckets.length;
    }
    
    public void sample(long value) {
        total++;
        if (value>max) {
            //grow up
            newMax(value);
        }
        if (value<min) {
            //grow down
            newMin(value);
        }
        int bIdx = (int)((value-min)/step);
        if (++buckets[bIdx]>COMPACT_LIMIT) {
            //compact
            compact();
        }
    }

    private void newMax(long value) {
        //double up everything to make it fit.
        
        // TODO Auto-generated method stub
        
    }

    private void newMin(long value) {
        //double up everything to make it fit.
        
        
        // TODO Auto-generated method stub
        
    }

    private void compact() {
        // TODO Auto-generated method stub
        
    }
    
    //TODO: method to return percentile value.
        
    //TODO: toString method to return 50% 80% 96% 99.2% 99.84% 99.968%
    
    //TODO: given a raw value return pct "Near"
    //TODO: given a raw value return pct above and/or below.
    
}
