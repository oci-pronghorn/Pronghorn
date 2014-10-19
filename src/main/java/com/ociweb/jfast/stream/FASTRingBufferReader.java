package com.ociweb.jfast.stream;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

/**
 * Public interface for applications desiring to consume data from a FAST feed.
 * @author Nathan Tippy
 *
 */
public class FASTRingBufferReader {//TODO: B, build another static reader that does auto convert to the requested type.
    
    
    public final static int OFF_MASK  =   0xFFFFFFF;
    public final static int BASE_SHFT =   28;
    
    static double[] powd = new double[] {1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
        1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
        1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
        1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
        1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};
    
    static float[] powf = new float[] {Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,
        1.0E-45f,1.0E-44f,1.0E-43f,1.0E-42f,1.0E-41f,1.0E-40f,1.0E-39f,1.0E-38f,1.0E-37f,1.0E-36f,1.0E-35f,1.0E-34f,1.0E-33f,1.0E-32f,1.0E-31f,1.0E-30f,1.0E-29f,1.0E-28f,1.0E-27f,1.0E-26f,1.0E-25f,1.0E-24f,1.0E-23f,1.0E-22f,
        1.0E-21f,1.0E-20f,1.0E-19f,1.0E-18f,1.0E-17f,1.0E-16f,1.0E-15f,1.0E-14f,1.0E-13f,1.0E-12f,1.0E-11f,1.0E-10f,1.0E-9f,1.0E-8f,1.0E-7f,1.0E-6f,1.0E-5f,1.0E-4f,0.001f,0.01f,0.1f,1.0f,10.0f,100.0f,1000.0f,10000.0f,100000.0f,1000000.0f,
        1.0E7f,1.0E8f,1.0E9f,1.0E10f,1.0E11f,1.0E12f,1.0E13f,1.0E14f,1.0E15f,1.0E16f,1.0E17f,1.0E18f,1.0E19f,1.0E20f,1.0E21f,1.0E22f,1.0E23f,1.0E24f,1.0E25f,1.0E26f,1.0E27f,1.0E28f,1.0E29f,1.0E30f,1.0E31f,1.0E32f,1.0E33f,1.0E34f,1.0E35f,
        1.0E36f,1.0E37f,1.0E38f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN};
    
    
    public static int readInt(int[] buffer, int mask, PaddedLong pos, int idx) {
          return buffer[mask & (int)(pos.value + (OFF_MASK&idx))];
    }

    public static int readInt(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
    }

    //int[] rbB, int rbMask, PaddedLong rbPos
    public static long readLong(int[] buffer, int mask, PaddedLong pos, int idx) {
        long i = pos.value + (OFF_MASK&idx);        
        return (((long) buffer[mask & (int)i]) << 32) | (((long) buffer[mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }

    public static long readLong(FASTRingBuffer ring, int idx) {
        long i = ring.workingTailPos.value + (OFF_MASK&idx);        
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }

    public static double readDouble(FASTRingBuffer ring, int idx) {
        return ((double)readDecimalMantissa(ring,(OFF_MASK&idx)))/powd[64+readDecimalExponent(ring,(OFF_MASK&idx))]; //TODO: D, swap table around for performance boost
    }

    public static float readFloat(FASTRingBuffer ring, int idx) {
        return ((float)readDecimalMantissa(ring,(OFF_MASK&idx)))/powf[64*readDecimalExponent(ring,(OFF_MASK&idx))];
    }
    
    public static int readDecimalExponent(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
    }
    
    public static long readDecimalMantissa(FASTRingBuffer ring, int idx) {
        long i = ring.workingTailPos.value + (OFF_MASK&idx) + 1; //plus one to skip over exponent
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }
    

    public static int readDataLength(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx) + 1)];// second int is always the length
    }

    @Deprecated
    public static Appendable readText(FASTRingBuffer ring, int idx, Appendable target) {
        
        return readASCII(ring, idx, target);

    }
    
    public static Appendable readASCII(FASTRingBuffer ring, int idx, Appendable target) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = FASTRingBufferReader.readDataLength(ring, idx);

        if (pos < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
            return readASCIIConst(ring,len,target,0x7FFFFFFF & pos);
        } else {
            return readASCIIRing(ring,len,target,pos);
        }
    }
    
    
    private static Appendable readASCIIConst(FASTRingBuffer ring, int len, Appendable target, int pos) {
        try {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target.append((char)buffer[pos++]);
            }
        } catch (IOException e) {
           throw new FASTException(e);
        }
        return target;
    }
    
    
    private static Appendable readASCIIRing(FASTRingBuffer ring, int len, Appendable target, int pos) {
        try {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target.append((char)buffer[mask & pos++]);
            }
        } catch (IOException e) {
           throw new FASTException(e);
        }
        return target;
    }
    
    public static void readText(FASTRingBuffer ring, int idx, char[] target, int targetOffset) {
        //if ascii
        readASCII(ring,idx,target,targetOffset);
        //else
        //readUTF8(ring,idx,target,targetOffset);
    }
    
    public static void readASCII(FASTRingBuffer ring, int idx, char[] target, int targetOffset) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = FASTRingBufferReader.readDataLength(ring, idx);
        if (pos < 0) {
            try {
                readASCIIConst(ring,len,target, targetOffset, 0x7FFFFFFF & pos);
            } catch (Exception e) {
                
                e.printStackTrace();
                System.err.println("pos now :"+(0x7FFFFFFF & pos)+" len "+len);                
                System.exit(0);
                
            }
        } else {
            readASCIIRing(ring,len,target, targetOffset,pos);
        }
    }
    

    private static void readASCIIConst(FASTRingBuffer ring, int len, char[] target, int targetIdx, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            char c = (char)buffer[pos++];
            target[targetIdx++] = c;
        };
    }
    
//    private static void readUTF8Const(FASTRingBuffer ring, int bytesLen, char[] target, int targetIdx, int ringPos) {
//        
//        long charAndPos = ((long)ringPos)<<32;
//        
//        int i = targetIdx;
//        int chars = target.length;
//        while (--chars>=0) {
//            
//            charAndPos = decodeUTF8Fast(ring.constByteBuffer, charAndPos, ring.byteMask);            
//            target[i++] = (char)charAndPos;
//  
//        }
//               
//    }
    
    private static void readASCIIRing(FASTRingBuffer ring, int len, char[] target, int targetIdx, int pos) {
        byte[] buffer = ring.byteBuffer;
        int mask = ring.byteMask;
        while (--len >= 0) {
            target[targetIdx]=(char)buffer[mask & pos++];
        }
    }
    
//    private static void readUTF8Ring(FASTRingBuffer ring, int len, char[] target, int targetIdx, int pos) {
//        
//        
////        byte[] buffer = ring.byteBuffer;
////        int mask = ring.byteMask;
////        while (--len >= 0) {
////            target[targetIdx]=(char)buffer[mask & pos++];
////        }
//    }

    
  /**
   * Convert bytes into chars using UTF-8.
   * 
   *  High 32   BytePosition
   *  Low  32   Char (caller can cast response to char to get the decoded value)  
   * 
   */
  public static long decodeUTF8Fast(byte[] source, long posAndChar, int mask) { //pass in long of last position?
      
    int sourcePos = (int)(posAndChar >> 32); 
      
    byte b;   
    if ((b = source[mask&sourcePos++]) >= 0) {
        // code point 7
        return (((long)sourcePos)<<32) | b;
    } 
    
    int result;
    if (((byte) (0xFF & (b << 2))) >= 0) {
        if ((b & 0x40) == 0) {
            ++sourcePos;
            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
        }
        // code point 11
        result = (b & 0x1F);
    } else {
        if (((byte) (0xFF & (b << 3))) >= 0) {
            // code point 16
            result = (b & 0x0F);
        } else {
            if (((byte) (0xFF & (b << 4))) >= 0) {
                // code point 21
                result = (b & 0x07);
            } else {
                if (((byte) (0xFF & (b << 5))) >= 0) {
                    // code point 26
                    result = (b & 0x03);
                } else {
                    if (((byte) (0xFF & (b << 6))) >= 0) {
                        // code point 31
                        result = (b & 0x01);
                    } else {
                        // System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
                        // the high bit should never be set
                        sourcePos += 5;
                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                    }

                    if ((source[sourcePos] & 0xC0) != 0x80) {
                        sourcePos += 5;
                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                    }
                    result = (result << 6) | (source[sourcePos++] & 0x3F);
                }
                if ((source[sourcePos] & 0xC0) != 0x80) {
                    sourcePos += 4;
                    return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                }
                result = (result << 6) | (source[sourcePos++] & 0x3F);
            }
            if ((source[sourcePos] & 0xC0) != 0x80) {
                sourcePos += 3;
                return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
            }
            result = (result << 6) | (source[sourcePos++] & 0x3F);
        }
        if ((source[sourcePos] & 0xC0) != 0x80) {
            sourcePos += 2;
            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
        }
        result = (result << 6) | (source[sourcePos++] & 0x3F);
    }
    if ((source[sourcePos] & 0xC0) != 0x80) {
        sourcePos += 1;
        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
    }
    int chr = ((result << 6) | (source[sourcePos++] & 0x3F));
    return (((long)sourcePos)<<32) | chr;
  }
    
    
    public static boolean eqUTF8(FASTRingBuffer ring, int idx, CharSequence seq) {
        int len = FASTRingBufferReader.readDataLength(ring, idx);
        if (0==len && seq.length()==0) {
            return true;
        }
        //char count is not comparable to byte count for UTF8 of length greater than zero.
        //must convert one to the other before comparison.
        
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        if (pos < 0) {
            return eqUTF8Const(ring,len,seq,0x7FFFFFFF & pos);
        } else {
            return eqUTF8Ring(ring,len,seq,pos);
        }
    }
    
    
    public static boolean eqASCII(FASTRingBuffer ring, int idx, CharSequence seq) {
        int len = FASTRingBufferReader.readDataLength(ring, idx);
        if (len!=seq.length()) {
            return false;
        }
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        if (pos < 0) {
            return eqASCIIConst(ring,len,seq,0x7FFFFFFF & pos);
        } else {
            return eqASCIIRing(ring,len,seq,pos);
        }
    }

    private static boolean eqASCIIConst(FASTRingBuffer ring, int len, CharSequence seq, int pos) {
        byte[] buffer = ring.constByteBuffer;
        int i = 0;
        while (--len >= 0) {
            if (seq.charAt(i++)!=buffer[pos++]) {
                return false;
            }
        }
        return true;
    }
    
    
    /**
     * checks equals without moving buffer cursor.
     * 
     * @param ring
     * @param bytesLen
     * @param seq
     * @param ringPos
     * @return
     */
    private static boolean eqUTF8Const(FASTRingBuffer ring, int bytesLen, CharSequence seq, int ringPos) {
        
        long charAndPos = ((long)ringPos)<<32;
        
        int i = 0;
        int chars = seq.length();
        while (--chars>=0) {
            
            charAndPos = decodeUTF8Fast(ring.constByteBuffer, charAndPos, Integer.MAX_VALUE);
            
            if (seq.charAt(i++) != (char)charAndPos) {
                return false;
            }
            
        }
                
        return true;
    }
    
    @Deprecated
    private static boolean eqTextRing(FASTRingBuffer ring, int len, CharSequence seq, int pos) {
            
        return eqASCIIRing(ring,len,seq,pos);
        
    }
    
    private static boolean eqASCIIRing(FASTRingBuffer ring, int len, CharSequence seq, int pos) {
        
        byte[] buffer = ring.byteBuffer;
        
        int mask = ring.byteMask;
        int i = 0;
        while (--len >= 0) {
            if (seq.charAt(i++)!=buffer[mask & pos++]) {
                //System.err.println("text match failure on:"+seq.charAt(i-1)+" pos "+pos+" mask "+mask);
                return false;
            }
        }
        return true;
    }
    
    private static boolean eqUTF8Ring(FASTRingBuffer ring, int len, CharSequence seq, int ringPos) {
        
        
        long charAndPos = ((long)ringPos)<<32;
        int mask = ring.byteMask;
        int i = 0;
        int chars = seq.length();
        while (--chars>=0) {
            
            charAndPos = decodeUTF8Fast(ring.byteBuffer, charAndPos, mask);
            
            if (seq.charAt(i++) != (char)charAndPos) {
                return false;
            }
            
        }
                
        return true;
        
        
    }   
    
    
    //Bytes
    
    public static int readBytesLength(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx) + 1)];// second int is always the length
    }

    public static ByteBuffer readBytes(FASTRingBuffer ring, int idx, ByteBuffer target) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + idx)];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            return readBytesConst(ring,len,target,0x7FFFFFFF & pos);
        } else {
            return readBytesRing(ring,len,target,pos);
        }
    }
    
    private static ByteBuffer readBytesConst(FASTRingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            target.put(buffer[pos++]);
        }
        return target;
    }

    private static ByteBuffer readBytesRing(FASTRingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.byteBuffer;
        int mask = ring.byteMask;
        while (--len >= 0) {
            target.put(buffer[mask & pos++]);
        }
        return target;
    }
    
    public static void readBytes(FASTRingBuffer ring, int idx, byte[] target, int targetOffset) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,0x7FFFFFFF & pos);
        } else {
            readBytesRing(ring,len,target, targetOffset,pos);
        }
    }
    
    private static void readBytesConst(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetIdx++]=buffer[pos++];
            };
    }

    private static void readBytesRing(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target[targetIdx++]=buffer[mask & pos++];
            }
    }
    
    public static void readBytes(FASTRingBuffer ring, int idx, byte[] target, int targetOffset, int targetMask) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,targetMask, 0x7FFFFFFF & pos);
        } else {
            readBytesRing(ring,len,target, targetOffset,targetMask, pos);
        }
    }
    
    private static void readBytesConst(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int targetMask, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetMask & targetIdx++]=buffer[pos++];
            };
    }

    private static void readBytesRing(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int targetMask, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target[targetMask & targetIdx]=buffer[mask & pos++];
            }
    }


    public static int encodeSingleChar(int c, byte[] buffer, int pos) {
        if (c <= 0x007F) {
            // code point 7
            buffer[pos++] = (byte) c;
        } else {
            if (c <= 0x07FF) {
                // code point 11
                buffer[pos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
            } else {
                if (c <= 0xFFFF) {
                    // code point 16
                    buffer[pos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                } else {
                    if (c < 0x1FFFFF) {
                        // code point 21
                        buffer[pos++] = (byte) (0xF0 | ((c >> 18) & 0x07));
                    } else {
                        if (c < 0x3FFFFFF) {
                            // code point 26
                            buffer[pos++] = (byte) (0xF8 | ((c >> 24) & 0x03));
                        } else {
                            if (c < 0x7FFFFFFF) {
                                // code point 31
                                buffer[pos++] = (byte) (0xFC | ((c >> 30) & 0x01));
                            } else {
                                throw new UnsupportedOperationException("can not encode char with value: " + c);
                            }
                            buffer[pos++] = (byte) (0x80 | ((c >> 24) & 0x3F));
                        }
                        buffer[pos++] = (byte) (0x80 | ((c >> 18) & 0x3F));
                    }
                    buffer[pos++] = (byte) (0x80 | ((c >> 12) & 0x3F));
                }
                buffer[pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
            }
            buffer[pos++] = (byte) (0x80 | ((c) & 0x3F));
        }
        return pos;
    }

	public static boolean isNewMessage(FASTRingBuffer rb) {
		return rb.consumerData.isNewMessage();
	}



}
