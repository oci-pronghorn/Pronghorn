package com.ociweb.jfast.ring;



public class RingWriter {

	
	
    public static void writeLong(int[] buffer, int mask, long pos, long value) {
        
        ///off mask  
        // add pos on to the 
        
        buffer[mask & (int)pos] = (int)value >>> 32;
        buffer[mask & (int)(pos+1)] = (int)value & 0xFFFFFFFF;
        
                
    }

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


    
    //TODO: need helper method to append text as we go so that logic remains here, temp bytes write
    
    //Because the stream neeeds to be safe and write the bytes ahead to the buffer we need 
    //to set the new byte pos, pos/len ints as a separate call
    public static void finishWriteBytes(RingBuffer rb, int p, int length) {
            	
//    	        
//        String val = new String(Arrays.copyOfRange(rb.byteBuffer, p, p+length));
//        System.err.println("write string:"+val);
//
//        
        
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, p);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, length);
   //     System.err.println("write length "+length+" at pos "+rb.workingHeadPos.value);
        rb.byteWorkingHeadPos.value = p + length;
        
    }

    public static void writeBytes(RingBuffer rb, byte[] source) {
        RingBuffer.addByteArray(source, 0, source.length, rb);
    }
    
}
