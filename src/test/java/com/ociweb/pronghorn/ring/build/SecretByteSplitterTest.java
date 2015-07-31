package com.ociweb.pronghorn.ring.build;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.ring.util.build.SecretByteSplitter;

public class SecretByteSplitterTest {

    @Test
    public void testRoundTrip() {
        String value = "hello world";
                
        StringBuilder scrambled = (StringBuilder)SecretByteSplitter.scramble(value, new StringBuilder());
        
        assertTrue(scrambled.length() == value.length()*2);
        
        StringBuilder assembled = (StringBuilder)SecretByteSplitter.assemble(scrambled, new StringBuilder());
        
        assertEquals(assembled.toString(),value);
        
    }
    
    
}
