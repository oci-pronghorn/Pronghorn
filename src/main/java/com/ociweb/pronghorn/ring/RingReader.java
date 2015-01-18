package com.ociweb.pronghorn.ring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;

/**
 * Public interface for applications desiring to consume data from a FAST feed.
 * @author Nathan Tippy
 *
 */
public class RingReader {//TODO: B, build another static reader that does auto convert to the requested type.
    
    
    public final static int OFF_MASK  =   0xFFFFFFF;
    public final static int BASE_SHFT =   28;
    public final static int POS_CONST_MASK = 0x7FFFFFFF;
    

    public final static double[] powdi = new double[]{
    	1.0E64,1.0E63,1.0E62,1.0E61,1.0E60,1.0E59,1.0E58,1.0E57,1.0E56,1.0E55,1.0E54,1.0E53,1.0E52,1.0E51,1.0E50,1.0E49,1.0E48,1.0E47,1.0E46,1.0E45,1.0E44,1.0E43,1.0E42,1.0E41,1.0E40,1.0E39,1.0E38,1.0E37,1.0E36,1.0E35,1.0E34,1.0E33,
    	1.0E32,1.0E31,1.0E30,1.0E29,1.0E28,1.0E27,1.0E26,1.0E25,1.0E24,1.0E23,1.0E22,1.0E21,1.0E20,1.0E19,1.0E18,1.0E17,1.0E16,1.0E15,1.0E14,1.0E13,1.0E12,1.0E11,1.0E10,1.0E9,1.0E8,1.0E7,1000000.0,100000.0,10000.0,1000.0,100.0,10.0,
    	1.0,0.1,0.01,0.001,1.0E-4,1.0E-5,1.0E-6,1.0E-7,1.0E-8,1.0E-9,1.0E-10,1.0E-11,1.0E-12,1.0E-13,1.0E-14,1.0E-15,1.0E-16,1.0E-17,1.0E-18,1.0E-19,1.0E-20,1.0E-21,1.0E-22,1.0E-23,1.0E-24,1.0E-25,1.0E-26,1.0E-27,1.0E-28,1.0E-29,1.0E-30,1.0E-31,
    	0E-32,1.0E-33,1.0E-34,1.0E-35,1.0E-36,1.0E-37,1.0E-38,1.0E-39,1.0E-40,1.0E-41,0E-42,1.0E-43,1.0E-44,1.0E-45,1.0E-46,1.0E-47,1.0E-48,1.0E-49,1.0E-50,1.0E-51,1.0E-52,1.0E-53,1.0E-54,1.0E-55,1.0E-56,1.0E-57,1.0E-58,1.0E-59,1.0E-60,1.0E-61,1.0E-62,1.0E-63,1.0E-64
    };
    
    public final static float[] powfi = new float[]{
    	Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,1.0E38f,1.0E37f,1.0E36f,1.0E35f,1.0E34f,1.0E33f,
    	1.0E32f,1.0E31f,1.0E30f,1.0E29f,1.0E28f,1.0E27f,1.0E26f,1.0E25f,1.0E24f,1.0E23f,1.0E22f,1.0E21f,1.0E20f,1.0E19f,1.0E18f,1.0E17f,1.0E16f,1.0E15f,1.0E14f,1.0E13f,1.0E12f,1.0E11f,1.0E10f,1.0E9f,1.0E8f,1.0E7f,1000000.0f,100000.0f,10000.0f,1000.0f,100.0f,10.0f,
    	1.0f,0.1f,0.01f,0.001f,1.0E-4f,1.0E-5f,1.0E-6f,1.0E-7f,1.0E-8f,1.0E-9f,1.0E-10f,1.0E-11f,1.0E-12f,1.0E-13f,1.0E-14f,1.0E-15f,1.0E-16f,1.0E-17f,1.0E-18f,1.0E-19f,1.0E-20f,1.0E-21f,1.0E-22f,1.0E-23f,1.0E-24f,1.0E-25f,1.0E-26f,1.0E-27f,1.0E-28f,1.0E-29f,1.0E-30f,1.0E-31f,
    	0E-32f,1.0E-33f,1.0E-34f,1.0E-35f,1.0E-36f,1.0E-37f,1.0E-38f,1.0E-39f,1.0E-40f,1.0E-41f,0E-42f,1.0E-43f,1.0E-44f,1.0E-45f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN
    };
    
    public static int readInt(int[] buffer, int mask, PaddedLong pos, int idx) {
          return buffer[mask & (int)(pos.value + (OFF_MASK&idx))];
    }

    public static int readInt(RingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
    }

    //int[] rbB, int rbMask, PaddedLong rbPos
    public static long readLong(int[] buffer, int mask, PaddedLong pos, int idx) {
        long i = pos.value + (OFF_MASK&idx);        
        return (((long) buffer[mask & (int)i]) << 32) | (((long) buffer[mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }

    public static long readLong(RingBuffer ring, int idx) {
        long i = ring.workingTailPos.value + (OFF_MASK&idx);        
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }

    public static double readDouble(RingBuffer ring, int idx) {
        return ((double)readDecimalMantissa(ring,(OFF_MASK&idx)))*powdi[64 + readDecimalExponent(ring,(OFF_MASK&idx))];
    }

    public static double readLongBitsToDouble(RingBuffer ring, int idx) {
        return Double.longBitsToDouble(readLong(ring,idx));
    }    
    
    public static float readFloat(RingBuffer ring, int idx) {
        return ((float)readDecimalMantissa(ring,(OFF_MASK&idx)))*powfi[64 + readDecimalExponent(ring,(OFF_MASK&idx))];
    }
    
    public static float readIntBitsToFloat(RingBuffer ring, int idx) {
        return Float.intBitsToFloat(readInt(ring,idx));
    }    
    
    public static int readDecimalExponent(RingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
    }
    
    public static long readDecimalMantissa(RingBuffer ring, int idx) {
        long i = ring.workingTailPos.value + (OFF_MASK&idx) + 1; //plus one to skip over exponent
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }
    

    public static int readDataLength(RingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx) + 1)];// second int is always the length
    }
    
    public static Appendable readASCII(RingBuffer ring, int idx, Appendable target) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = RingReader.readDataLength(ring, idx);

        if (pos < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
            return readASCIIConst(ring,len,target,POS_CONST_MASK & pos);
        } else {
            return readASCIIRing(ring,len,target,restorePosition(ring,pos));
        }
    }
    
    public static Appendable readUTF8(RingBuffer ring, int idx, Appendable target) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int len = RingReader.readDataLength(ring, idx);

        if (pos < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
            return readUTF8Const(ring,len,target,POS_CONST_MASK & pos);
        } else {
            return readUTF8Ring(ring,len,target,restorePosition(ring,pos));
        }
    }
   
	private static Appendable readUTF8Const(RingBuffer ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;
			  
			  while (charAndPos<limit) {		      
			      charAndPos = decodeUTF8Fast(ring.constByteBuffer, charAndPos, 0xFFFFFFFF); //constants do not wrap            
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;       
	}
	
	private static Appendable readUTF8Ring(RingBuffer ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;
			  
			  while (charAndPos<limit) {		      
			      charAndPos = decodeUTF8Fast(ring.byteBuffer, charAndPos, ring.byteMask);            
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;       
	}
	
    public static int readUTF8(RingBuffer ring, int idx, char[] target, int targetOffset) {
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        int bytesLength = RingReader.readDataLength(ring, idx);
        if (pos < 0) {
            return readUTF8Const(ring,bytesLength,target, targetOffset, POS_CONST_MASK & pos);
        } else {
            return readUTF8Ring(ring,bytesLength,target, targetOffset,restorePosition(ring,pos));
        }
    }
    
	private static int readUTF8Const(RingBuffer ring, int bytesLen, char[] target, int targetIdx, int ringPos) {
	  
	  long charAndPos = ((long)ringPos)<<32;
	  long limit = ((long)ringPos+bytesLen)<<32;
			  
	  int i = targetIdx;
	  while (charAndPos<limit) {
	      
	      charAndPos = decodeUTF8Fast(ring.constByteBuffer, charAndPos, 0xFFFFFFFF);//constants never loop back            
	      target[i++] = (char)charAndPos;
	
	  }
	  return i - targetIdx;    
	}
    
	private static int readUTF8Ring(RingBuffer ring, int bytesLen, char[] target, int targetIdx, int ringPos) {
		  
		  long charAndPos = ((long)ringPos)<<32;
		  long limit = ((long)(ringPos+bytesLen))<<32;
						  
		  int i = targetIdx;
		  while (charAndPos<limit) {
		      
		      charAndPos = decodeUTF8Fast(ring.byteBuffer, charAndPos, ring.byteMask);    
		      target[i++] = (char)charAndPos;
		
		  }
		  return i - targetIdx;
		         
	}
	
	
    private static Appendable readASCIIConst(RingBuffer ring, int len, Appendable target, int pos) {
        try {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target.append((char)buffer[pos++]);
            }
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        return target;
    }
    
    
    private static Appendable readASCIIRing(RingBuffer ring, int len, Appendable target, int pos) {
        try {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target.append((char)buffer[mask & pos++]);
            }
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        return target;
    }
       
    public static int readASCII(RingBuffer ring, int idx, char[] target, int targetOffset) {
        long tmp = ring.workingTailPos.value + (OFF_MASK&idx);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
        if (pos < 0) {
            try {
                readASCIIConst(ring,len,target, targetOffset, POS_CONST_MASK & pos);
            } catch (Exception e) {
                
                e.printStackTrace();
                System.err.println("pos now :"+(POS_CONST_MASK & pos)+" len "+len);                
                System.exit(0);
                
            }
        } else {
            readASCIIRing(ring,len,target, targetOffset,restorePosition(ring,pos));
        }
        return len;
    }
    

    private static void readASCIIConst(RingBuffer ring, int len, char[] target, int targetIdx, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            char c = (char)buffer[pos++];
            target[targetIdx++] = c;
        }
        
    }
    

    
    private static void readASCIIRing(RingBuffer ring, int len, char[] target, int targetIdx, int pos) {
        byte[] buffer = ring.byteBuffer;
        int mask = ring.byteMask;
        while (--len >= 0) {
            target[targetIdx++]=(char)buffer[mask & pos++];
        }
    }
   
    
  /**
   * Convert bytes into chars using UTF-8.
   * 
   *  High 32   BytePosition
   *  Low  32   Char (caller can cast response to char to get the decoded value)  
   * 
   */
  public static long decodeUTF8Fast(byte[] source, long posAndChar, int mask) { //pass in long of last position?
      //TODO: these masks appear to be wrong.
	  
	  // 7  //high bit zero all others its 1
	  // 5 6
	  // 4 6 6
	  // 3 6 6 6
	  // 2 6 6 6 6
	  // 1 6 6 6 6 6
	  
    int sourcePos = (int)(posAndChar >> 32); 
    
    byte b;   
    if ((b = source[mask&sourcePos++]) >= 0) {
        // code point 7
        return (((long)sourcePos)<<32) | (long)b; //1 byte result of 7 bits with high zero
    } 
    
    int result;
    if (((byte) (0xFF & (b << 2))) >= 0) {
        if ((b & 0x40) == 0) {        	
            ++sourcePos;
            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
        }
        // code point 11
        result = (b & 0x1F); //5 bits
    } else {
        if (((byte) (0xFF & (b << 3))) >= 0) {
            // code point 16
            result = (b & 0x0F); //4 bits
        } else {
            if (((byte) (0xFF & (b << 4))) >= 0) {
                // code point 21
                result = (b & 0x07); //3 bits
            } else {
                if (((byte) (0xFF & (b << 5))) >= 0) {
                    // code point 26
                    result = (b & 0x03); // 2 bits
                } else {
                    if (((byte) (0xFF & (b << 6))) >= 0) {
                        // code point 31
                        result = (b & 0x01); // 1 bit
                    } else {
                        // the high bit should never be set
                        sourcePos += 5;
                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                    }

                    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
                        sourcePos += 5;
                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                    }
                    result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
                }
                if ((source[mask&sourcePos] & 0xC0) != 0x80) {
                    sourcePos += 4;
                    return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
                }
                result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
            }
            if ((source[mask&sourcePos] & 0xC0) != 0x80) {
                sourcePos += 3;
                return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
            }
            result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
        }
        if ((source[mask&sourcePos] & 0xC0) != 0x80) {
            sourcePos += 2;
            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
        }
        result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
    }
    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
       System.err.println("Invalid encoding, low byte must have bits of 10xxxxxx but we find "+Integer.toBinaryString(source[mask&sourcePos]));
       sourcePos += 1;
       return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
    }
    long chr = ((result << 6) | (int)(source[mask&sourcePos++] & 0x3F)); //6 bits
    return (((long)sourcePos)<<32) | chr;
  }
    
    
    public static boolean eqUTF8(RingBuffer ring, int idx, CharSequence seq) {
        int len = RingReader.readDataLength(ring, idx);
        if (0==len && seq.length()==0) {
            return true;
        }
        //char count is not comparable to byte count for UTF8 of length greater than zero.
        //must convert one to the other before comparison.
        
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        if (pos < 0) {
            return eqUTF8Const(ring,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqUTF8Ring(ring,len,seq,restorePosition(ring,pos));
        }
    }
    
    
    public static boolean eqASCII(RingBuffer ring, int idx, CharSequence seq) {
        int len = RingReader.readDataLength(ring, idx);
        if (len!=seq.length()) {
            return false;
        }
        int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&idx))];
        if (pos < 0) {
            return eqASCIIConst(ring,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqASCIIRing(ring,len,seq,restorePosition(ring,pos));
        }
    }

    private static boolean eqASCIIConst(RingBuffer ring, int len, CharSequence seq, int pos) {
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
    private static boolean eqUTF8Const(RingBuffer ring, int bytesLen, CharSequence seq, int ringPos) {
        
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
    
    
    private static boolean eqASCIIRing(RingBuffer ring, int len, CharSequence seq, int pos) {
        
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
    
    private static boolean eqUTF8Ring(RingBuffer ring, int lenInBytes, CharSequence seq, int ringPos) {
        
        
        long charAndPos = ((long)ringPos)<<32;
        long limit = ((long)ringPos+lenInBytes)<<32;
        
        
        int mask = ring.byteMask;
        int i = 0;
        int chars = seq.length();
        while (--chars>=0 && charAndPos<limit) {
            
            charAndPos = decodeUTF8Fast(ring.byteBuffer, charAndPos, mask);
            
            if (seq.charAt(i++) != (char)charAndPos) {
                return false;
            }
            
        }
        if (chars >= 0 || charAndPos<limit) {
        	return false;
        }
                
        return true;
        
        
    }   
    
    private static int restorePosition(RingBuffer ring, int pos) {
    	
    	//TODO: AAA, must add ring.bytesHeadPosition.long()+pos;
    	//return ring.bytesHeadPos.get()+pos;
    	return pos;
    }
    
    
    //Bytes
    
    public static int readBytesLength(RingBuffer ring, int loc) {
        return ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&loc) + 1)];// second int is always the length
    }
    
    public static int readBytesPosition(RingBuffer ring, int loc) {
        int tmp = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&loc) )];
		return tmp<0 ? POS_CONST_MASK & tmp : restorePosition(ring,tmp);// first int is always the length
    }

    public static byte[] readBytesBackingArray(RingBuffer ring, int loc) {
    	 int pos = ring.buffer[ring.mask & (int)(ring.workingTailPos.value + (OFF_MASK&loc))];
    	 return pos<0 ? ring.constByteBuffer :  ring.byteBuffer;
    }
    
    public static ByteBuffer readBytes(RingBuffer ring, int loc, ByteBuffer target) {
        long tmp = ring.workingTailPos.value + (OFF_MASK&loc);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
        if (pos < 0) {
            return readBytesConst(ring,len,target,POS_CONST_MASK & pos);
        } else {
            return readBytesRing(ring,len,target,restorePosition(ring,pos));
        }
    }
    
    private static ByteBuffer readBytesConst(RingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            target.put(buffer[pos++]);
        }
        return target;
    }

    private static ByteBuffer readBytesRing(RingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.byteBuffer;
        int mask = ring.byteMask;
        while (--len >= 0) {
            target.put(buffer[mask & pos++]);
        }
        return target;
    }
    
    public static int readBytes(RingBuffer ring, int idx, byte[] target, int targetOffset) {
        long tmp = ring.workingTailPos.value + (OFF_MASK&idx);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,POS_CONST_MASK & pos);
        } else {
            readBytesRing(ring,len,target, targetOffset,restorePosition(ring,pos));
        }
        return len;
    }
    
    private static void readBytesConst(RingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetIdx++]=buffer[pos++];
            };
    }

    private static void readBytesRing(RingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target[targetIdx++]=buffer[mask & pos++];
            }
    }
    
    public static int readBytes(RingBuffer ring, int idx, byte[] target, int targetOffset, int targetMask) {
        long tmp = ring.workingTailPos.value + (OFF_MASK&idx);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,targetMask, POS_CONST_MASK & pos);
        } else {
            readBytesRing(ring.byteBuffer, restorePosition(ring,pos), ring.byteMask, target, targetOffset, targetMask,	len);
        }
        return len;
    }
    
	public static int copyBytes(final RingBuffer inputRing,	final RingBuffer outputRing, int fieldId) {
		//High level API example of reading bytes from one ring buffer into another array that wraps with a mask
		int length = readBytes(inputRing, fieldId, outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask);
		outputRing.validateVarLength(length);							
		
		int p = outputRing.byteWorkingHeadPos.value;                                
		RingBuffer.addBytePosAndLen(outputRing.buffer, outputRing.mask, outputRing.workingHeadPos, outputRing.bytesHeadPos.get()&outputRing.byteMask, p, length);							
		outputRing.byteWorkingHeadPos.value = p + length;
		return length;
	}
    
    private static void readBytesConst(RingBuffer ring, int len, byte[] target, int targetIdx, int targetMask, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {//TODO: A,  need to replace with intrinsics.
                target[targetMask & targetIdx++]=buffer[pos++];
            };
    }

    public static void readBytesRing(byte[] source, int sourceIdx, int sourceMask, byte[] target, int targetIdx, int targetMask, int length) {
    	final int tStop = (targetIdx + length) & targetMask;
		final int tStart = targetIdx & targetMask;
		final int rStop = (sourceIdx + length) & sourceMask;
		final int rStart = sourceIdx & sourceMask;
		if (tStop >= tStart) {
			if (rStop >= rStart) {
				//the source and target do not wrap
				System.arraycopy(source, rStart, target, tStart, length);
			} else {
				//the source is wrapping but not the target
				int srcFirstLen = (1 + sourceMask) - rStart;
				System.arraycopy(source, rStart, target, tStart, srcFirstLen);
				System.arraycopy(source, 0, target, tStart+srcFirstLen, length-srcFirstLen);
			}    			
		} else {
			if (rStop >= rStart) {
				//the source does not wrap but the target does
				// done as two copies
			    int targFirstLen = (1 + targetMask) - tStart;
			    System.arraycopy(source, rStart, target, tStart, targFirstLen);
			    System.arraycopy(source, rStart + targFirstLen, target, 0, length - targFirstLen);
			} else {
		        if (length>0) {
					//both the target and the source wrap
					int srcFirstLen = (1 + sourceMask) - rStart;
				    int targFirstLen = (1 + targetMask) - tStart;
				    if (srcFirstLen<targFirstLen) {
				    	//split on src first
				    	System.arraycopy(source, rStart, target, tStart, srcFirstLen);
				    	System.arraycopy(source, 0, target, tStart+srcFirstLen, targFirstLen - srcFirstLen);
				    	System.arraycopy(source, targFirstLen - srcFirstLen, target, 0, length - targFirstLen);    			    	
				    } else {
				    	//split on targ first
				    	System.arraycopy(source, rStart, target, tStart, targFirstLen);
				    	System.arraycopy(source, rStart + targFirstLen, target, 0, srcFirstLen - targFirstLen); 
				    	System.arraycopy(source, 0, target, srcFirstLen - targFirstLen, length - srcFirstLen);
				    }
		        }
			}
		}
	}


    public static boolean isNewMessage(RingBuffer rb) {
		return RingWalker.isNewMessage(rb.consumerData);
	}

}
