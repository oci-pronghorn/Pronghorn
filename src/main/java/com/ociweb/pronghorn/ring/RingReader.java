package com.ociweb.pronghorn.ring;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

/**
 * Public interface for applications desiring to consume data from a FAST feed.
 * @author Nathan Tippy
 *
 */
public class RingReader {//TODO: B, build another static reader that does auto convert to the requested type.
      

    public final static int POS_CONST_MASK = 0x7FFFFFFF;
    
    public final static int OFF_MASK  =   FieldReferenceOffsetManager.RW_FIELD_OFF_MASK;
    public final static int STACK_OFF_MASK = FieldReferenceOffsetManager.RW_STACK_OFF_MASK;
    public final static int STACK_OFF_SHIFT = FieldReferenceOffsetManager.RW_STACK_OFF_SHIFT;
    public final static int OFF_BITS = FieldReferenceOffsetManager.RW_FIELD_OFF_BITS;

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
    
//    /**
//     * These deprecated methods will be deleted in Feb 2015
//     * They should be in-lined or replaced before then
//     */
//    @Deprecated
//    public static int readInt(int[] buffer, int mask, PaddedLong pos, int loc) {
//          return RingBuffer.readInt(buffer, mask, pos.value +  loc);
//    }
//	@Deprecated
//    public static long readLong(int[] buffer, int mask, PaddedLong pos, int loc) {
//    	return RingBuffer.readLong(buffer, mask, pos.value + loc);
//    }
    
	public static int readInt(RingBuffer ring, int loc) {
		//allow all types of int and length
		assert((loc&0x1C<<OFF_BITS)==0 || (loc&0x1F<<OFF_BITS)==(0x14<<OFF_BITS)) : "Expected to read some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
        return RingBuffer.readInt(ring.buffer, ring.mask, ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }
	
	public static short readShort(RingBuffer ring, int loc) {
		assert((loc&0x1C<<OFF_BITS)==0) : "Expected to read some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
        return (short)RingBuffer.readInt(ring.buffer, ring.mask, ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }
	
	public static byte readByte(RingBuffer ring, int loc) {
		assert((loc&0x1C<<OFF_BITS)==0) : "Expected to read some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
        return (byte)RingBuffer.readInt(ring.buffer, ring.mask, ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }

	public static long readLong(RingBuffer ring, int loc) {
		assert((loc&0x1C<<OFF_BITS)==(0x4<<OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   
        return RingBuffer.readLong(ring.buffer, ring.mask, ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] +(OFF_MASK&loc));
    }

    public static double readDouble(RingBuffer ring, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return ((double)readDecimalMantissa(ring,loc))*powdi[64 + readDecimalExponent(ring,loc)];
    }

    public static double readLongBitsToDouble(RingBuffer ring, int loc) {
    	assert((loc&0x1C<<OFF_BITS)==(0x4<<OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   
        return Double.longBitsToDouble(readLong(ring,loc));
    }    
    
    public static float readFloat(RingBuffer ring, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return ((float)readDecimalMantissa(ring,loc))*powfi[64 + readDecimalExponent(ring,loc)];
    }
    
    public static float readIntBitsToFloat(RingBuffer ring, int loc) {
    	assert((loc&0x1C<<OFF_BITS)==0) : "Expected to read some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
        return Float.intBitsToFloat(readInt(ring,loc));
    }    

    public static int readDecimalExponent(RingBuffer ring, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to read some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	return RingBuffer.readInt(ring.buffer,ring.mask,ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc));
    }
    
    public static long readDecimalMantissa(RingBuffer ring, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to read some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return RingBuffer.readLong(ring.buffer, ring.mask, ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc) + 1);//plus one to skip over exponent
    }
    
    public static int readDataLength(RingBuffer ring, int loc) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        return ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc) + 1)];// second int is always the length
    }
    
    public static Appendable readASCII(RingBuffer ring, int loc, Appendable target) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
        int pos = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        return RingBuffer.readASCII(ring, target, pos, RingReader.readDataLength(ring, loc));
    }

	public static Appendable readUTF8(RingBuffer ring, int loc, Appendable target) {
		assert((loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        int pos = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        return RingBuffer.readUTF8(ring, target, pos, RingReader.readDataLength(ring, loc));
    }

	public static int readUTF8(RingBuffer ring, int loc, char[] target, int targetOffset) {
		assert((loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        int pos = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        int bytesLength = RingReader.readDataLength(ring, loc);
        
        
        if (pos < 0) {
            return readUTF8Const(ring,bytesLength,target, targetOffset, POS_CONST_MASK & pos);
        } else {
            return readUTF8Ring(ring,bytesLength,target, targetOffset,RingBuffer.restorePosition(ring,pos));
        }
    }
    
	private static int readUTF8Const(RingBuffer ring, int bytesLen, char[] target, int targetloc, int ringPos) {
	  
	  long charAndPos = ((long)ringPos)<<32;
	  long limit = ((long)ringPos+bytesLen)<<32;
			  
	  int i = targetloc;
	  while (charAndPos<limit) {
	      charAndPos = RingBuffer.decodeUTF8Fast(ring.constByteBuffer, charAndPos, 0xFFFFFFFF);//constants never loop back            
	      target[i++] = (char)charAndPos;
	  }
	  return i - targetloc;    
	}
    
	private static int readUTF8Ring(RingBuffer ring, int bytesLen, char[] target, int targetloc, int ringPos) {
		  
		  long charAndPos = ((long)ringPos)<<32;
		  long limit = ((long)(ringPos+bytesLen))<<32;
						  
		  int i = targetloc;
		  while (charAndPos<limit) {		      
		      charAndPos = RingBuffer.decodeUTF8Fast(ring.byteBuffer, charAndPos, ring.byteMask);    
		      target[i++] = (char)charAndPos;		
		  }
		  return i - targetloc;
		         
	}
	
	
    public static int readASCII(RingBuffer ring, int loc, char[] target, int targetOffset) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        long tmp = ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
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
            readASCIIRing(ring,len,target, targetOffset,RingBuffer.restorePosition(ring,pos));
        }
        return len;
    }
    

    private static void readASCIIConst(RingBuffer ring, int len, char[] target, int targetloc, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            char c = (char)buffer[pos++];
            target[targetloc++] = c;
        }
        
    }
    
    
    private static void readASCIIRing(RingBuffer ring, int len, char[] target, int targetloc, int pos) {
    	
        byte[] buffer = ring.byteBuffer;
        int mask = ring.byteMask;
        while (--len >= 0) {
            target[targetloc++]=(char)buffer[mask & pos++];
        }
    }
   
    
  public static boolean eqUTF8(RingBuffer ring, int loc, CharSequence seq) {
		assert( (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        int len = RingReader.readDataLength(ring, loc);
        if (0==len && seq.length()==0) {
            return true;
        }
        //char count is not comparable to byte count for UTF8 of length greater than zero.
        //must convert one to the other before comparison.
        
        int pos = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        if (pos < 0) {
            return eqUTF8Const(ring,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqUTF8Ring(ring,len,seq,RingBuffer.restorePosition(ring,pos));
        }
    }
    
    
    public static boolean eqASCII(RingBuffer ring, int loc, CharSequence seq) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
	
		long idx = ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
        int len = ring.buffer[ring.mask & (int)(idx + 1)];
        if (len!=seq.length()) {
            return false;
        }
		int pos = ring.buffer[ring.mask & (int)idx];
        if (pos < 0) {
            return eqASCIIConst(ring,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqASCIIRing(ring,len,seq,RingBuffer.restorePosition(ring,pos));
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
            
            charAndPos = RingBuffer.decodeUTF8Fast(ring.constByteBuffer, charAndPos, Integer.MAX_VALUE);
            
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
            
            charAndPos = RingBuffer.decodeUTF8Fast(ring.byteBuffer, charAndPos, mask);
            
            if (seq.charAt(i++) != (char)charAndPos) {
                return false;
            }
            
        }
        if (chars >= 0 || charAndPos<limit) {
        	return false;
        }
                
        return true;
        
        
    }   
    
    
    
    
    //Bytes
    
    public static int readBytesLength(RingBuffer ring, int loc) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS ||  //  11110   001011   TODO: revis these rules they dont pick up utf8 optionals? 01011 B
			   (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || 
			   (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        return ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc) + 1)];// second int is always the length
    }
    
    public static int readBytesPosition(RingBuffer ring, int loc) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        int tmp = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc) )];
		return tmp<0 ? POS_CONST_MASK & tmp : RingBuffer.restorePosition(ring,tmp);// first int is always the length
    }

    public static byte[] readBytesBackingArray(RingBuffer ring, int loc) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	 int pos = ring.buffer[ring.mask & (int)(ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc))];
    	 return pos<0 ? ring.constByteBuffer :  ring.byteBuffer;
    }
    
    public static ByteBuffer readBytes(RingBuffer ring, int loc, ByteBuffer target) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        long tmp = ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
                
        return RingBuffer.readBytes(ring, target, pos, len);
    }

	public static int readBytes(RingBuffer ring, int loc, byte[] target, int targetOffset) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	long tmp = ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);

        int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
                
        if (pos < 0) {
            readBytesConst(ring,len,target,targetOffset,POS_CONST_MASK & pos);
        } else {
            readBytesRing(ring,len,target,targetOffset,RingBuffer.restorePosition(ring,pos));
        }
        return len;
    }
    
    private static void readBytesConst(RingBuffer ring, int len, byte[] target, int targetloc, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetloc++]=buffer[pos++];
            };
    }

    private static void readBytesRing(RingBuffer ring, int len, byte[] target, int targetloc, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.byteMask;
            while (--len >= 0) {
                target[targetloc++]=buffer[mask & pos++];
            }
    }
    
    public static int readBytes(RingBuffer ring, int loc, byte[] target, int targetOffset, int targetMask) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
        long tmp = ring.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		int pos = ring.buffer[ring.mask & (int)(tmp)];
        int len = ring.buffer[ring.mask & (int)(tmp + 1)];
                
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,targetMask, POS_CONST_MASK & pos);
        } else {
            RingBuffer.copyBytesFromToRing(ring.byteBuffer, RingBuffer.restorePosition(ring,pos), ring.byteMask, target, targetOffset, targetMask,	len);
        }
        return len;
    }
    
    
    public static void copyInt(final RingBuffer sourceRing,	final RingBuffer targetRing, int sourceLOC, int targetLOC) {
		assert((sourceLOC&0x1C<<OFF_BITS)==0) : "Expected to read some type of int but found "+TypeMask.toString((sourceLOC>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		assert((targetLOC&0x1C<<RingWriter.OFF_BITS)==0) : "Expected to write some type of int but found "+TypeMask.toString((targetLOC>>RingWriter.OFF_BITS)&TokenBuilder.MASK_TYPE);
        
		targetRing.buffer[targetRing.mask &((int)targetRing.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(targetLOC>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&targetLOC))] =
		     sourceRing.buffer[sourceRing.mask & (int)(sourceRing.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(sourceLOC>>STACK_OFF_SHIFT)]+(OFF_MASK&sourceLOC))];
    }
    
    public static void copyLong(final RingBuffer sourceRing, final RingBuffer targetRing, int sourceLOC, int targetLOC) {
    	assert((sourceLOC&0x1C<<RingReader.OFF_BITS)==(0x4<<RingReader.OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((sourceLOC>>RingReader.OFF_BITS)&TokenBuilder.MASK_TYPE);
    	assert((targetLOC&0x1C<<RingWriter.OFF_BITS)==(0x4<<RingWriter.OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((targetLOC>>RingWriter.OFF_BITS)&TokenBuilder.MASK_TYPE);
		long srcIdx = sourceRing.ringWalker.activeReadFragmentStack[RingReader.STACK_OFF_MASK&(sourceLOC>>RingReader.STACK_OFF_SHIFT)] +(RingReader.OFF_MASK&sourceLOC);   	
		long targetIdx = (targetRing.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(targetLOC>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&targetLOC));	
		targetRing.buffer[targetRing.mask & (int)targetIdx]     = sourceRing.buffer[sourceRing.mask & (int)srcIdx];
		targetRing.buffer[targetRing.mask & (int)targetIdx+1] = sourceRing.buffer[sourceRing.mask & (int)srcIdx+1];
    }
    
    public static void copyDecimal(final RingBuffer sourceRing, final RingBuffer targetRing, int sourceLOC, int targetLOC) {
    	assert((sourceLOC&0x1E<<RingReader.OFF_BITS)==(0x0C<<RingReader.OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((sourceLOC>>RingReader.OFF_BITS)&TokenBuilder.MASK_TYPE);
    	assert((targetLOC&0x1E<<RingWriter.OFF_BITS)==(0x0C<<RingWriter.OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((targetLOC>>RingWriter.OFF_BITS)&TokenBuilder.MASK_TYPE); 

    	long srcIdx = sourceRing.ringWalker.activeReadFragmentStack[RingReader.STACK_OFF_MASK&(sourceLOC>>RingReader.STACK_OFF_SHIFT)] +(RingReader.OFF_MASK&sourceLOC);   	
		long targetIdx = (targetRing.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(targetLOC>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&targetLOC));	
	
		targetRing.buffer[targetRing.mask & (int)targetIdx]     = sourceRing.buffer[sourceRing.mask & (int)srcIdx];
		targetRing.buffer[targetRing.mask & (int)targetIdx+1] = sourceRing.buffer[sourceRing.mask & (int)srcIdx+1];
		targetRing.buffer[targetRing.mask & (int)targetIdx+2] = sourceRing.buffer[sourceRing.mask & (int)srcIdx+2];
		
    }
        
	public static int copyBytes(final RingBuffer sourceRing,	final RingBuffer targetRing, int sourceLOC, int targetLOC) {
		assert((sourceLOC&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (sourceLOC&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (sourceLOC&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to read some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((sourceLOC>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		assert((targetLOC&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (targetLOC&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (targetLOC&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((targetLOC>>OFF_BITS)&TokenBuilder.MASK_TYPE);
				
		//High level API example of reading bytes from one ring buffer into another array that wraps with a mask
		int length = readBytes(sourceRing, sourceLOC, targetRing.byteBuffer, targetRing.byteWorkingHeadPos.value, targetRing.byteMask);
		RingBuffer.validateVarLength(targetRing, length);							
		
		int p = targetRing.byteWorkingHeadPos.value;
		
		RingBuffer.setBytePosAndLen(targetRing.buffer, targetRing.mask, 
				targetRing.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(targetLOC>>STACK_OFF_SHIFT)]+(OFF_MASK&targetLOC), p, length, RingBuffer.bytesWriteBase(targetRing)); 
	
		targetRing.byteWorkingHeadPos.value =  0xEFFFFFFF&(p + length);	
		
		return length;
	}
    
    private static void readBytesConst(RingBuffer ring, int len, byte[] target, int targetloc, int targetMask, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {//TODO: A,  need to replace with intrinsics.
                target[targetMask & targetloc++]=buffer[pos++];
            };
    }

	public static void setReleaseBatchSize(RingBuffer rb, int size) {
		RingBuffer.setReleaseBatchSize(rb, size);  	
	}

	/**
	 * Copies current message from input ring to output ring.  Once copied that message is no longer readable.
	 * Message could be read before calling this copy using a low-level look ahead technique.
	 * 
	 * Returns false until the full message is copied.
	 * 
	 * Once called must continue to retry until true is returned or the message will be left in a partial state.
	 * 
	 * 
	 * @param inputRing
	 * @param outputRing
	 * @return
	 */
	public static boolean tryMoveSingleMessage(RingBuffer inputRing, RingBuffer outputRing) {
		assert(RingBuffer.from(inputRing) == RingBuffer.from(outputRing));
		//NOTE: all the reading makes use of the high-level API to manage the fragment state, this call assumes tryRead was called once already.
			
		//we may re-enter this function to continue the copy
		RingWalker consumerData = inputRing.ringWalker;
		boolean copied = RingWalker.copyFragment0(inputRing, outputRing, inputRing.workingTailPos.value, consumerData.nextWorkingTail);
		while (copied && !FieldReferenceOffsetManager.isTemplateStart(RingBuffer.from(inputRing), consumerData.nextCursor)) {			
			//using short circut logic so copy does not happen unless the prep is successful
			copied = RingWalker.prepReadFragment(inputRing, consumerData) && RingWalker.copyFragment0(inputRing, outputRing, inputRing.workingTailPos.value, consumerData.nextWorkingTail);			
		}
		return copied;
	}

	public static boolean isNewMessage(RingWalker rw) {
		return rw.isNewMessage;
	}

	public static boolean isNewMessage(RingBuffer ring) {
		return ring.ringWalker.isNewMessage;
	}

	public static int getMsgIdx(RingBuffer rb) {
		return rb.ringWalker.msgIdx;
	}

	public static int getMsgIdx(RingWalker rw) {
		return rw.msgIdx;
	}

	//this impl only works for simple case where every message is one fragment. 
	public static boolean tryReadFragment(RingBuffer ringBuffer) { 
	
		if (null==ringBuffer.buffer) {
			ringBuffer.init();//hack test
		}		
		
		if (FieldReferenceOffsetManager.isTemplateStart(RingBuffer.from(ringBuffer), ringBuffer.ringWalker.nextCursor)) {    		
			return RingWalker.prepReadMessage(ringBuffer, ringBuffer.ringWalker);			   
	    } else {  
			return RingWalker.prepReadFragment(ringBuffer, ringBuffer.ringWalker);
	    }
	}

	public static void releaseReadLock(RingBuffer ringBuffer) {
		ringBuffer.workingTailPos.value = ringBuffer.ringWalker.nextWorkingTail;
		
		ringBuffer.bytesTailPos.lazySet(ringBuffer.byteWorkingTailPos.value); 			
		ringBuffer.tailPos.lazySet(ringBuffer.workingTailPos.value); //inlined release however the byte adjust must happen on every message so its done earlier
	}

}
