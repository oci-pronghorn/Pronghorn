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

    public static void writeInt(FASTRingBuffer rb, int value) {
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);        
    }
    
    public static void writeLong(FASTRingBuffer rb, long value) {
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int)value >>> 32, (int)value & 0xFFFFFFFF );    
    }

    public static void writeDecimal(FASTRingBuffer rb, int exponent, long mantissa) {
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, exponent);   
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int)mantissa >>> 32, (int)mantissa & 0xFFFFFFFF );    
    }

    public static void writeString(FASTRingBuffer queue, CharSequence charSequence) {
        // TODO Auto-generated method stub
        
    }

    public static void writeBytes(FASTRingBuffer ringBuffer, byte[] catBytes) {
        // TODO Auto-generated method stub
        
    }
    
}
