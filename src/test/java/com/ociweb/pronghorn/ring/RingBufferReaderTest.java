package com.ociweb.pronghorn.ring;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.pronghorn.ring.RingReader;

public class RingBufferReaderTest {

    
    @Test
    public void powCenters() {
        
        assertEquals(1d,RingReader.powdi[64],.000001d);
        assertEquals(1f,RingReader.powfi[64],.00001f);
        
    }

    
}
