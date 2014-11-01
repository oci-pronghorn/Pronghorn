package com.ociweb.jfast.ring;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.ring.RingReader;

public class FASTRingBufferReaderTest {

    
    @Test
    public void powCenters() {
        
        assertEquals(1d,RingReader.powd[64],.000001d);
        assertEquals(1f,RingReader.powf[64],.00001f);
        
    }
    
}
