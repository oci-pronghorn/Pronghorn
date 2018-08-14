package com.ociweb.pronghorn.pipe.util;

public class ArrayBacked {

    /*
     * New test code to determine the speed of using an array of ints
     * 
     *  TODO: There are no proper unit tests
     *  TODO: The speed test indicates that there is something wrong with both int and long implementations.
     */
    
    public static long readLong(int[] buffer, int mask, long byteIdx) {
        assert(0==(7&byteIdx)); 
        int intIdx = (int)(byteIdx>>2);
        return (((long)buffer[mask & intIdx])<<32) | (0xFFFFFFFFl&(long)buffer[mask & (1+intIdx)]);
    }

    
    public static void writeLong(int[] buffer, int mask, long byteIdx, long value) {
     
        int intIdx = (int)(byteIdx>>2);
        buffer[mask & intIdx] = (int)(value>>32);
        buffer[mask & (1+intIdx)] = (int)(value);
                
    }
    
    
    public static int readInt(int[] buffer, int mask, long byteIdx) {  
       assert(0==(3&byteIdx));    
       return buffer[mask & (int)(byteIdx>>2)];
    }

    public static void writeInt(int[] buffer, int mask, long byteIdx, int value ) {        
        buffer[mask & (int)(byteIdx>>2)] = value;
     }
    
        
    public static short readShort(int[] buffer, int mask, long byteIdx) {
        assert(0==(1&byteIdx));        
        return  (short)(buffer[mask & (int)(byteIdx>>2)]>>(((int)(~byteIdx)&0x2)<<3)); 

    }

    public static void writeShort(int[] buffer, int mask, long byteIdx, short value) {
        int intIdx = mask & (int)(byteIdx>>2);
        int shiftDown = ((int)(~byteIdx)&0x2)<<3;       
        buffer[intIdx]     = (buffer[intIdx] & (~(0xFFFF << shiftDown))) |  ((0xFFFF&value)<<shiftDown);
  
    }
    
    
    public static byte readByte(int[] buffer, int mask, long byteIdx) {
        return (byte)(buffer[mask & (int)byteIdx>>2]>> ( ( (~(int)byteIdx)&0x3)<<3 )
                 );
    }

    
    public static void writeByte(int[] buffer, int mask, long byteIdx, byte value) {
        int i = (~(int)byteIdx)&0x3;
        int shiftLeft =   i<<3;       
        int intIdx = mask & (int)byteIdx>>2;
        
        int temp = (0xFF&(int)value)<<shiftLeft;        
      //  buffer[intIdx] ^=  ((((int)0xFF)<<shiftLeft) & (buffer[intIdx]^temp));
        
        buffer[intIdx] = (buffer[intIdx] & (~ (((int)0xFF)<<shiftLeft))    ) |   temp;
  
    }
    
    
    
}
