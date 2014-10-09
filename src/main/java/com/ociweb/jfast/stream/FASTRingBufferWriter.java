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


    
    //TODO: need helper method to append text as we go so that logic remains here, temp bytes write
    
    //Because the stream neeeds to be safe and write the bytes ahead to the buffer we need 
    //to set the new byte pos, pos/len ints as a separate call
    public static void finishWriteBytes(FASTRingBuffer rb, int length) {
            	
        int p = rb.addByteWorkingHeadPos.value;
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, p);
        FASTRingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, length);
   //     System.err.println("write length "+length+" at pos "+rb.workingHeadPos.value);
        rb.addByteWorkingHeadPos.value = p + length;
        
    }

    public static void writeBytes(FASTRingBuffer rb, byte[] source) {
        FASTRingBuffer.addByteArray(source, 0, source.length, rb);
    }
    
}
