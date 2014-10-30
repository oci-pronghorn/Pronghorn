package com.ociweb.jfast.ring;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.ring.FASTRingBufferReader;

public class FASTRingBufferReaderTest {

    
    @Test
    public void powCenters() {
        
        assertEquals(1d,FASTRingBufferReader.powd[64],.000001d);
        assertEquals(1f,FASTRingBufferReader.powf[64],.00001f);
        
    }
    
}
