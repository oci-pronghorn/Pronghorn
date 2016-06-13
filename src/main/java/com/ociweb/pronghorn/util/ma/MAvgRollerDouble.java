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
public class MAvgRollerDouble {

    public double[] elements;
    public int      position;
    public long     distance; //public to avoid method call
    private double  runningMean;
    public double   cleanMean;
    public int      cleanMeanCountDown;

    private final int     size;
    private final double factor;
    
    final static Logger logger = LoggerFactory.getLogger(MAvgRollerDouble.class);
    
    public MAvgRollerDouble(int size) {
        this.size = size;
        this.factor = 1/(double)size;
        this.elements = new double[size];
        this.position = 0;
        this.distance = 0-size;//start negative size its ready for use when its zero or greater
        this.runningMean = 0;
        this.cleanMean = 0;
        this.cleanMeanCountDown = size;
        assert(distance<0);
    }
    
    public void reset() {
        Arrays.fill(this.elements,0d);
        this.position = 0;
        this.distance = 0-size;//start negative size its ready for use when its zero or greater
        this.runningMean = 0;
        this.cleanMean = 0;
        this.cleanMeanCountDown = size;
    }
    
    
    /**
     * zero returns the last item added.
     * one returns the item before that
     * 
     * @param index
     * @return
     */
    public static double getElementAt(MAvgRollerDouble roller, int index) {
        index++;//need one more because roll is already pointing at the next spot
        return roller.elements[index <= roller.position ? roller.position-index : roller.size+(roller.position-index) ];
    }
    
    public static int getCount(MAvgRollerDouble roller) {
        return (int) (roller.distance<0 ?  roller.size+roller.distance : roller.size);
    }
    
    public static double peek(MAvgRollerDouble roller){
        return roller.elements[roller.position];//next one tossed out
    }
    /*
     * Take this new sample and return the one that has been thrown out of the set
     */
    public static final double roll(MAvgRollerDouble roller, double curValue) {
        if (roller.size == 0 ) {
            return curValue;
        }
        assert(!Double.isNaN(curValue));
        double result = roller.elements[roller.position];
        //set both working values
        assert(!Double.isNaN(result)); //this could not happen because it would be caught when passed in on a previous call
        //run both threads and wait for them to finish.
        
        roller.elements[roller.position] = curValue;
        if (++roller.position==roller.size) {
        	roller.position=0;
        }
        
        if (++roller.distance >= 0) {
            roller.runningMean += ((curValue-result)*roller.factor);
        } 
        
        roller.cleanMean += curValue*roller.factor;

        if (--roller.cleanMeanCountDown==0) {
            //this is the same length as all the data in elements
            //so we can not replace the working sum with this cleaned value!
        	roller.runningMean = roller.cleanMean;
        	roller.cleanMean = 0d;
        	roller.cleanMeanCountDown = roller.size;            
        }
        return result;
    }
    

    public static void print(MAvgRollerDouble roller) {
        int i = 0;
        while (i<roller.elements.length) {
            boolean isPos = roller.position==i;
            
            System.out.println(roller.elements[i++]+"  "+(isPos?"pos":""));
            
        }
        
    }

    public static final double mean(MAvgRollerDouble roller) {
        if (roller.distance<0) {
            throw new UnsupportedOperationException("Can not be called before moving average data is fully accumulated. Needs:"+roller.size+" Loaded:"+(roller.size+roller.distance));
        }
        return roller.runningMean;
    }
}
