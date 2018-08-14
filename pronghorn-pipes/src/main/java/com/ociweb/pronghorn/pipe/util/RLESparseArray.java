package com.ociweb.pronghorn.pipe.util;

public class RLESparseArray {

    public static long[] rlEncodeSparseArray(long[] data) {
        //countTransitions
        int transitionCount = 1; //the first one counts
        int i = data.length;
        long lastValue = data[i-1];
        while (--i>=0) {
            long thisValue = data[i];
            if (thisValue!=lastValue) {
                transitionCount++;
            }
            lastValue = thisValue;
        }
        //copy the data
        int length = (transitionCount * 2) + 1;
        long[] result = new long[length];
        int resultPos = 0;
        result[resultPos++] = data.length;
        int runLength = 0;
        lastValue = data[0];
        for(int j = 0; j<data.length; j++) {
            if (data[j]==lastValue) {
                runLength++;
            } else {
                //end of run
                result[resultPos++] = runLength;
                result[resultPos++] = lastValue;
                runLength = 1;
                lastValue=data[j];
            }
        }
        result[resultPos++] = runLength;
        result[resultPos++] = lastValue;
        return result;
    }    

    public static long[] rlDecodeSparseArray(long[] data) {
        
        long[] result = new long[(int)data[0]];
        int resultPos = 0;
        for(int i = 1; i<data.length; i+=2){
            long count = data[i];
            long value = data[i+1];
            long j = count;
            while (--j>=0) {
                result[resultPos++] = value;
            }
        }
        return result;
        
    }
    

    public static int[] rlEncodeSparseArray(int[] data) {
        //countTransitions
        int transitionCount = 1; //the first one counts
        int i = data.length;
        int lastValue = data[i-1];
        while (--i>=0) {
            int thisValue = data[i];
            if (thisValue!=lastValue) {
                transitionCount++;
            }
            lastValue = thisValue;
        }
        //copy the data
        int length = (transitionCount * 2) + 1; //one for leading full count
        int[] result = new int[length];
        int resultPos = 0;
        result[resultPos++] = data.length;
        int runLength = 0;
        lastValue = data[0];
        for(int j = 0; j<data.length; j++) {
            if (data[j]==lastValue) {
                runLength++;
            } else {
                //end of run
                result[resultPos++] = runLength;
                result[resultPos++] = lastValue;
                runLength = 1;
                lastValue=data[j];
            }
        }
        result[resultPos++] = runLength;
        result[resultPos++] = lastValue;
        return result;
    }

    public static int[] rlDecodeSparseArray(int[] data) {
        
        int[] result = new int[data[0]];
        int resultPos = 0;
        for(int i = 1; i<data.length; i+=2){
            int count = data[i];
            int value = data[i+1];
            int j = count;
            while (--j>=0) {
                result[resultPos++] = value;
            }
        }
        return result;
        
    }
    
    
    
    
}
