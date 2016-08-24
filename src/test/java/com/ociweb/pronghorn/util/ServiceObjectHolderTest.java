package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;

public class ServiceObjectHolderTest {

    int iterations = 100000;
    
    private boolean expireRule(String serviceObject) {
        return serviceObject.charAt(0)=='1';
    }

    @Test
    public void lookup() {
        
     ServiceObjectValidator<String> validator = new ServiceObjectValidator<String>() {

        @Override
        public boolean isValid(String serviceObject) {            
            return ! expireRule(serviceObject);
        }

        @Override
        public void dispose(String t) {
            //nothing to do.            
        }
         
     };
       
     ServiceObjectHolder<String> holder = new ServiceObjectHolder<String>(String.class, validator, true /*grows */);
     
     Random r = new Random(42);
     
     int i = iterations;
     while (--i>=0) {
         String value = Long.toHexString(r.nextLong());
         
         long key = holder.add(value);
         
         String value2 = holder.getValid(key);
         
         if (expireRule(value)) {
             assertNull(value2);
         } else {
             assertEquals(value,value2);
         }
     }
             
             
    }
    
}
