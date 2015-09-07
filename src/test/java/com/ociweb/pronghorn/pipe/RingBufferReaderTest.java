package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.PipeReader;

public class RingBufferReaderTest {

    
    @Test
    public void powCenters() {
        
        assertEquals(1d,PipeReader.powdi[64],.000001d);
        assertEquals(1f,PipeReader.powfi[64],.00001f);
        
    }

    
}
