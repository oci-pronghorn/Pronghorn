package com.ociweb.pronghorn.util.ma;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Keeps N values and a running mean at all times for doubles.  As new samples are added 
 * the oldest values is thrown out and returned to the caller.
 * 
 * @author Nathan Tippy
 *
 */
public class MAvgRollerLong {

    public long[] elements;
    public int      position;
    public long     distance; //public to avoid method call
    private double  runningMean;

    private final int     size;
    private final double factor;
    
    final static Logger logger = LoggerFactory.getLogger(MAvgRollerDouble.class);
    
    public MAvgRollerLong(int size) {
        this.size = size;
        this.factor = 1/(double)size;
        this.elements = new long[size];
        this.position = 0;
        this.distance = 0-size;//start negative size its ready for use when its zero or greater
        this.runningMean = 0;
        assert(distance<0);
    }
    
    public void reset() {
        Arrays.fill(this.elements,0L);
        this.position = 0;
        this.distance = 0-size;//start negative size its ready for use when its zero or greater
        this.runningMean = 0;
    }
    
    
    /**
     * zero returns the last item added.
     * one returns the item before that
     * 
     * @param index
     * @return
     */
    public static double getElementAt(MAvgRollerLong roller, int index) {
        index++;//need one more because roll is already pointing at the next spot
        return roller.elements[index <= roller.position ? roller.position-index : roller.size+(roller.position-index) ];
    }
    
    public static int getCount(MAvgRollerLong roller) {
        return (int) (roller.distance<0 ?  roller.size+roller.distance : roller.size);
    }
    
    public static double peek(MAvgRollerLong roller){
        return roller.elements[roller.position];//next one tossed out
    }
    
    /*
     * Take this new sample and return the one that has been thrown out of the set
     */
    public static final long roll(MAvgRollerLong roller, long curValue) {
        if (roller.size == 0 ) {
            return curValue;
        }
        long result = roller.elements[roller.position];
        roller.elements[roller.position] = curValue;
        if (++roller.position==roller.size) {
        	roller.position=0;
        }
        
        if (++roller.distance >= 0) {
            roller.runningMean += ((curValue-result)*roller.factor);
        } 

        return result;
    }
    

    public static void print(MAvgRollerLong roller) {
        int i = 0;
        while (i<roller.elements.length) {
            boolean isPos = roller.position==i;
            
            System.out.println(roller.elements[i++]+"  "+(isPos?"pos":""));
            
        }
        
    }

    public static final double mean(MAvgRollerLong roller) {
        if (roller.distance<0) {
            throw new UnsupportedOperationException("Can not be called before moving average data is fully accumulated. Needs:"+roller.size+" Loaded:"+(roller.size+roller.distance));
        }
        return roller.runningMean;
    }
}
