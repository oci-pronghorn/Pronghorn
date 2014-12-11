package com.ociweb.pronghorn.ring;

import java.nio.ByteBuffer;



public class RingWriter {

	
	
    public static void writeLong(int[] buffer, int mask, long pos, long value) {
        
        ///off mask  
        // add pos on to the 
        
        buffer[mask & (int)pos] = (int)value >>> 32;
        buffer[mask & (int)(pos+1)] = (int)value & 0xFFFFFFFF;
        
                
    }
    
//long i = ring.workingHeadPos.value + (OFF_MASK&idx);  
    
    public static void writeInt(RingBuffer rb, int value) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);        
    }
    
    public static void writeLong(RingBuffer rb, long value) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int)(value >>> 32), (int)value & 0xFFFFFFFF );    
    }

    public static void writeDecimal(RingBuffer rb, int exponent, long mantissa) {
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, exponent);   
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, (int) (mantissa >>> 32), (int)mantissa & 0xFFFFFFFF );    
    }

    
    //Because the stream neeeds to be safe and write the bytes ahead to the buffer we need 
    //to set the new byte pos, pos/len ints as a separate call
    public static void finishWriteBytes(RingBuffer rb, int p, int length) {
    	rb.validateVarLength(length);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, p);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, length);

        rb.byteWorkingHeadPos.value = p + length;
        
    }

    public static void writeBytes(RingBuffer rb, byte[] source) {
    	rb.validateVarLength(source.length);
        RingBuffer.addByteArray(source, 0, source.length, rb);
    }
    
	
    public static void writeBytes(RingBuffer rb, ByteBuffer source, int position, int length) {
    	rb.validateVarLength(length);
    	if ((position&rb.byteMask) > ((position+length)&rb.byteMask)) {
    		int temp = 1 + rb.mask - (position & rb.mask);
    		source.get(rb.byteBuffer, position & rb.byteMask, temp);
    		source.get(rb.byteBuffer, 0, length - temp);					    		
    	} else {					    	
    		source.get(rb.byteBuffer, position&rb.byteMask, length);
    	}
    	finishWriteBytes(rb, position, length);
    }
    
}
