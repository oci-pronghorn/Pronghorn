package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class BranchlessTest {

    private static final int FIZZ = 0x81;
    private static final int BUZZ = 0x82;
      
    @Test
    public void simpleFizzBuzzTest() {
       
        for(int i = 1;i<10000;i++) {
            
            assertEquals(fizzbuzz(i), fizzbuzzBranchless(i));
            assertEquals(fizzbuzz(i), fizzbuzzBranchlessInline(i));            
            
        }
        
        
    }
    
    
    
    public int fizzbuzz(int value) {
        
        if (0 != (5%value)) {
            if (0 != (3%value)) {
                return value;
            } else {
                return FIZZ;
            }
        } else {
            if (0 != (3%value)) {
                return BUZZ;
            } else {
                return FIZZ|BUZZ;
            }            
        }        
    }
    
    
    
    public int fizzbuzzBranchless(int value) {
        
        return Branchless.ifZero((5%value), Branchless.ifZero((3%value), FIZZ|BUZZ, BUZZ), Branchless.ifZero((3%value), FIZZ, value));
     
    }
    
    public int fizzbuzzBranchlessInline(int value) {
        
        int mod3 = (3%value);
        int mod3tmp = (mod3-1)>>31;
        int mod3msk = ((mod3>>31)^mod3tmp)&mod3tmp;
        int mod5 = (5%value);
        int mod5tmp = (mod5-1)>>31;
        int mod5msk = ((mod5>>31)^mod5tmp)&mod5tmp;
        return ((((FIZZ|BUZZ)&mod3msk)|(BUZZ&(~mod3msk)))&mod5msk)|(((FIZZ&mod3msk)|(value&(~mod3msk)))&(~mod5msk));
     
    }
    
}
