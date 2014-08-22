package com.ociweb.jfast.stream;

import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

public class FASTRingBufferWriter {

    
    //TODO: A, X fill this out based on the work done in the writer,  Build Test example where this is used to write into ring buffer.
    
    public static void writeLong(int[] buffer, int mask, long pos, long value) {
        
        ///off mask  
        // add pos on to the 
        
        buffer[mask & (int)pos] = (int)value >>> 32;
        buffer[mask & (int)(pos+1)] = (int)value & 0xFFFFFFFF;
        
                
    }
    
}
