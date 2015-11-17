package com.ociweb.pronghorn.pipe.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class RLESparseArrayTest {

    private static final int testSize = 100000;
    
    @Test
    public void testLongRLE() {
       Random r  = new Random(42); 
       
       int j = testSize;
       while (--j>=0) {
       
           long[] inputArray = generateSparseLongArray(r, 100);
            
           long[] encoded = RLESparseArray.rlEncodeSparseArray(inputArray);
                  
           assertFalse(Arrays.equals(inputArray, encoded));
           
           long[] decoded = RLESparseArray.rlDecodeSparseArray(encoded);
                  
           boolean equals = Arrays.equals(decoded, inputArray);
           if (!equals) {
               System.out.println(Arrays.toString(inputArray));
               System.out.println(Arrays.toString(encoded));
               System.out.println(Arrays.toString(decoded));
           }
          assertTrue(equals);
       }
        
    }

    private long[] generateSparseLongArray(Random r, int len) {
       long[] result = new long[len];
       
       int c = len/10;//this is sparse and mostly zeros
       while (--c>=0) {
           result[r.nextInt(len)] = r.nextInt();
       }
       
       return result;
    }
 
    @Test
    public void testIntRLE() {
       Random r  = new Random(42); 
       
       int j = testSize;
       while (--j>=0) {
       
           int[] inputArray = generateSparseIntArray(r, 100);
            
           int[] encoded = RLESparseArray.rlEncodeSparseArray(inputArray);
                  
           assertFalse(Arrays.equals(inputArray, encoded));
           
           int[] decoded = RLESparseArray.rlDecodeSparseArray(encoded);
                  
           boolean equals = Arrays.equals(decoded, inputArray);
           if (!equals) {
               System.out.println(Arrays.toString(inputArray));
               System.out.println(Arrays.toString(encoded));
               System.out.println(Arrays.toString(decoded));
           }
          assertTrue(equals);
       }
        
    }

    private int[] generateSparseIntArray(Random r, int len) {
       int[] result = new int[len];
       
       int c = len/10;//this is sparse and mostly zeros
       while (--c>=0) {
           result[r.nextInt(len)] = r.nextInt();
       }
       
       return result;
    }
    
    
    
    
    
}
