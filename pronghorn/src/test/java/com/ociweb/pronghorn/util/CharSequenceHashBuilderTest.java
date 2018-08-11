package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class CharSequenceHashBuilderTest {

    
  @Test
    public void simpleTextTest() {
       
        CharSequence[] testValues = new CharSequence[] {
                                                         "a",
                                                         "ab",
                                                         "abc",
                                                         "abcd",
                                                         "abcde",                                                         
                                                         "abcdef",
                                                         "abcdefg",
                                                         "abcdefgh",
                                                         "abcdefgc",
                                                         "abcdefgq",
                                                         "abcdefgai",
                                                         "abcdefgbij",
                                                        };
        
        
        CharSequenceHashBuilder obj = new CharSequenceHashBuilder();
        
        GeneratedHashRules result = obj.perfectCharSequenceHashBuilder(testValues);
        
       // result.report();
        
        assertEquals(false, result.needsMoreSplitting());
        assertEquals(0xF, result.lengthMask());
        
        
        //if its a power of 2 then split more is false
                
    }
    

    @Test
    public void integerPerfectHashTest() {
        
        int maxValues = 2000;
        int j = maxValues;
        while (--j >= 0) {
            int[] testValues = new int[j];
            int i = j;
            while (--i >= 0) {
                testValues[i] = (i*1111)+12345;           
            }
            
          //  System.out.println("count "+j);
          //  System.out.println(Arrays.toString(testValues));
           
            CharSequenceHashBuilder obj = new CharSequenceHashBuilder();
            GeneratedHashRules result = obj.perfectIntegerHashBuilder(testValues);
            
            //Must be within 1 power of two of the length
            
            int spaceSize = (1<<Integer.bitCount(result.lengthMask()));
            
            
            assertTrue("space size "+spaceSize+" vs "+j, spaceSize >= j);
            assertTrue( spaceSize <= (1+(j*4)));
            assertFalse(result.needsMoreSplitting());
         
            
        }
    }
    
    
}
