package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Public interface for applications desiring to consume data from a FAST feed.
 * @author Nathan Tippy
 *
 */
public class PipeReader {//TODO: B, build another static reader that does auto convert to the requested type.
      

    public final static int POS_CONST_MASK = 0x7FFFFFFF;
    
    public final static int OFF_MASK  =   FieldReferenceOffsetManager.RW_FIELD_OFF_MASK;
    public final static int STACK_OFF_MASK = FieldReferenceOffsetManager.RW_STACK_OFF_MASK;
    public final static int STACK_OFF_SHIFT = FieldReferenceOffsetManager.RW_STACK_OFF_SHIFT;
    public final static int OFF_BITS = FieldReferenceOffsetManager.RW_FIELD_OFF_BITS;
    
    public final static Logger log = LoggerFactory.getLogger(PipeReader.class);

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
    
    public static <S extends MessageSchema<S>> DataInputBlobReader<S> inputStream(Pipe<S> pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc)+"  b"+Integer.toBinaryString(loc);

		DataInputBlobReader<S> stream = Pipe.inputStream(pipe);
		stream.openHighLevelAPIField(loc);
		return stream;
	}

    /**
     * Reads int from specified pipe
     * @param pipe to read from
     * @param loc location of int to read
     */
	public static int readInt(Pipe pipe, int loc) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);

        return Pipe.readInt(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }
	
	public static int readIntSecure(Pipe pipe, int loc, int clearValue) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);

        return Pipe.readIntSecure(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc),clearValue);
    }

    /**
     * Reads short from specified pipe
     * @param pipe to be read
     * @param loc location of short to read
     */
	public static short readShort(Pipe pipe, int loc) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

        return (short)Pipe.readInt(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }

    /**
     * Reads byte from specified pipe
     * @param pipe to be read
     * @param loc location of short to read
     */
	public static byte readByte(Pipe pipe, int loc) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

        return (byte)Pipe.readInt(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]+(OFF_MASK&loc));
    }

    /**
     * Reads long from specified pipe
     * @param pipe to be read
     * @param loc location of long to read
     */
	public static long readLong(Pipe pipe, int loc) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);
	    
        return Pipe.readLong(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] +(OFF_MASK&loc));
    }

    /**
     * Reads double from specified pipe
     * @param pipe to be read
     * @param loc location of long to read
     */
    public static double readDouble(Pipe pipe, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return ((double)readDecimalMantissa(pipe,loc))*powdi[64 - readDecimalExponent(pipe,loc)];
    }

    public static double readLongBitsToDouble(Pipe pipe, int loc) {
    	assert((loc&0x1C<<OFF_BITS)==(0x4<<OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   
        return Double.longBitsToDouble(readLong(pipe,loc));
    }

    /**
     * Reads float from specified pipe
     * @param pipe to be read
     * @param loc location of float to read
     */
    public static float readFloat(Pipe pipe, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return ((float)readDecimalMantissa(pipe,loc))*powfi[64 - readDecimalExponent(pipe,loc)];
    }
    
    public static float readIntBitsToFloat(Pipe pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

        return Float.intBitsToFloat(readInt(pipe,loc));
    }    

    public static int readDecimalExponent(Pipe pipe, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to read some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	return Pipe.readInt(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc));
    }
    
    public static long readDecimalMantissa(Pipe pipe, int loc) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to read some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
        return Pipe.readLong(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc) + 1);//plus one to skip over exponent
    }


    /**
     * Checks specified pipe to see if charSeq and value at specific location are equal
     * @param pipe pipe to be checked
     * @param loc location of value to compare
     * @param charSeq CharSequence to compare
     * @return <code>true</code> if the values are equal else <code>false</code>
     */
    public static boolean isEqual(Pipe pipe, int loc, CharSequence charSeq) {
    	int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];      	
    	return Pipe.isEqual(pipe, charSeq, pos, PipeReader.readBytesLength(pipe,loc));
    }
    
    public static Appendable readASCII(Pipe pipe, int loc, Appendable target) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
        int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        int len = PipeReader.readBytesLength(pipe,loc);
        return Pipe.readASCII(pipe, target, pos, len);
    }

	public static <A extends Appendable> A readUTF8(Pipe pipe, int loc, A target) {
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
        int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        return (A)Pipe.readUTF8(pipe, target, pos, PipeReader.readBytesLength(pipe,loc));
    }

	public static int readUTF8(Pipe pipe, int loc, char[] target, int targetOffset) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
        int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        int bytesLength = PipeReader.readBytesLength(pipe,loc);
        
        
        if (pos < 0) {
            return readUTF8Const(pipe,bytesLength,target, targetOffset, POS_CONST_MASK & pos);
        } else {
            return readUTF8Ring(pipe,bytesLength,target, targetOffset,Pipe.restorePosition(pipe, pos));
        }
    }
    
	private static int readUTF8Const(Pipe pipe, int bytesLen, char[] target, int targetloc, int ringPos) {
	  
	  long charAndPos = ((long)ringPos)<<32;
	  long limit = ((long)ringPos+bytesLen)<<32;
			  
	  int i = targetloc;
	  while (charAndPos<limit) {
	      charAndPos = Pipe.decodeUTF8Fast(pipe.blobConstBuffer, charAndPos, 0xFFFFFFFF);//constants never loop back            
	      target[i++] = (char)charAndPos;
	  }
	  return i - targetloc;    
	}
    
	private static int readUTF8Ring(Pipe pipe, int bytesLen, char[] target, int targetloc, int ringPos) {
		  
		  long charAndPos = ((long)ringPos)<<32;
		  long limit = ((long)(ringPos+bytesLen))<<32;
						  
		  int i = targetloc;
		  while (charAndPos<limit) {		      
		      charAndPos = Pipe.decodeUTF8Fast(Pipe.byteBuffer(pipe), charAndPos, pipe.blobMask);    
		      target[i++] = (char)charAndPos;		
		  }
		  return i - targetloc;
		         
	}
	
	
    public static int readASCII(Pipe pipe, int loc, char[] target, int targetOffset) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
	
        long tmp = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp)];
        int len = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp + 1)];
        
        
        if (pos < 0) {
            try {
                readASCIIConst(pipe,len,target, targetOffset, POS_CONST_MASK & pos);
            } catch (Exception e) {
                
                e.printStackTrace();
                System.err.println("pos now :"+(POS_CONST_MASK & pos)+" len "+len); 
                throw new RuntimeException(e);
                
                
            }
        } else {
            readASCIIRing(pipe,len,target, targetOffset,Pipe.restorePosition(pipe, pos));
        }
        return len;
    }
    

    private static void readASCIIConst(Pipe pipe, int len, char[] target, int targetloc, int pos) {
        byte[] buffer = pipe.blobConstBuffer;
        while (--len >= 0) {
            char c = (char)buffer[pos++];
            target[targetloc++] = c;
        }
        
    }
    
    
    private static void readASCIIRing(Pipe pipe, int len, char[] target, int targetloc, int pos) {
    	
        byte[] buffer = Pipe.blob(pipe);
        int mask = pipe.blobMask;
        while (--len >= 0) {
            target[targetloc++]=(char)buffer[mask & pos++];
        }
    }
   
    
  public static boolean eqUTF8(Pipe pipe, int loc, CharSequence seq) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
        int len = PipeReader.readBytesLength(pipe,loc);
        if (0==len && seq.length()==0) {
            return true;
        }
        //char count is not comparable to byte count for UTF8 of length greater than zero.
        //must convert one to the other before comparison.
        
        int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))];
        if (pos < 0) {
            return eqUTF8Const(pipe,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqUTF8Ring(pipe,len,seq,Pipe.restorePosition(pipe,pos));
        }
    }
    
    
    public static boolean eqASCII(Pipe pipe, int loc, CharSequence seq) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
	
		long idx = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
        int len = Pipe.slab(pipe)[pipe.slabMask & (int)(idx + 1)];
        if (len!=seq.length()) {
            return false;
        }
		int pos = Pipe.slab(pipe)[pipe.slabMask & (int)idx];
        if (pos < 0) {
            return eqASCIIConst(pipe,len,seq,POS_CONST_MASK & pos);
        } else {
            return eqASCIIRing(pipe,len,seq,Pipe.restorePosition(pipe,pos));
        }
    }

    private static boolean eqASCIIConst(Pipe pipe, int len, CharSequence seq, int pos) {
        byte[] buffer = pipe.blobConstBuffer;
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
     */
    private static boolean eqUTF8Const(Pipe pipe, int bytesLen, CharSequence seq, int ringPos) {
        
        long charAndPos = ((long)ringPos)<<32;
        
        int i = 0;
        int chars = seq.length();
        while (--chars>=0) {
            
            charAndPos = Pipe.decodeUTF8Fast(pipe.blobConstBuffer, charAndPos, Integer.MAX_VALUE);
            
            if (seq.charAt(i++) != (char)charAndPos) {
                return false;
            }
            
        }
                
        return true;
    }
    
    
    private static boolean eqASCIIRing(Pipe pipe, int len, CharSequence seq, int pos) {
    	
        byte[] buffer = Pipe.blob(pipe);
        
        int mask = pipe.blobMask;
        int i = 0;
        while (--len >= 0) {
            if (seq.charAt(i++)!=buffer[mask & pos++]) {
                //System.err.println("text match failure on:"+seq.charAt(i-1)+" pos "+pos+" mask "+mask);
                return false;
            }
        }
        return true;
    }
    
    private static boolean eqUTF8Ring(Pipe pipe, int lenInBytes, CharSequence seq, int ringPos) {
        
        
        long charAndPos = ((long)ringPos)<<32;
        long limit = ((long)ringPos+lenInBytes)<<32;
        
        
        int mask = pipe.blobMask;
        int i = 0;
        int chars = seq.length();
        while (--chars>=0 && charAndPos<limit) {
            
            charAndPos = Pipe.decodeUTF8Fast(Pipe.blob(pipe), charAndPos, mask);
            
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
    
    public static int readBytesLength(Pipe pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
        return Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc) + 1)];// second int is always the length
    }
    
    public static int readBytesMask(Pipe pipe, int loc) {
        assert(0!=loc) : "This field needed for swapping to different array per field, like the constants array";
    	return pipe.blobMask;
    }
    
    public static int readBytesPosition(Pipe pipe, int loc) {
        int tmp = readBytesMeta(pipe, loc);
		return tmp<0 ? POS_CONST_MASK & tmp : Pipe.restorePosition(pipe,tmp);// first int is always the length
    }
    
    public static boolean isStructured(Pipe pipe, int loc) {
    	return 0!=(Pipe.STRUCTURED_POS_MASK & readBytesMeta(pipe, loc));
    }

	private static int readBytesMeta(Pipe pipe, int loc) {
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		return Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc) )];
	}

    public static byte[] readBytesBackingArray(Pipe pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	 int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)]  + (OFF_MASK&loc))];
    	 return pos<0 ? pipe.blobConstBuffer :  Pipe.byteBuffer(pipe);
    }
    
    public static ByteBuffer readBytes(Pipe pipe, int loc, ByteBuffer target) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

        long tmp = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp)];
        int len = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp + 1)];
        return Pipe.readBytes(pipe, target, pos, len);
    }

    public static DataOutputBlobWriter<?> readBytes(Pipe pipe, int loc, DataOutputBlobWriter<?> target) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

        long tmp = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		return Pipe.readBytes(pipe, target, Pipe.slab(pipe)[pipe.slabMask & (int)(tmp)], Pipe.slab(pipe)[pipe.slabMask & (int)(tmp + 1)]);
    }
    
    public static ByteBuffer[] wrappedUnstructuredLayoutBuffer(Pipe pipe, int loc) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
    	return wrappedUnstructuredLayoutBufferImpl(pipe, pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK & (loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc));
    }

	private static ByteBuffer[] wrappedUnstructuredLayoutBufferImpl(Pipe pipe, long pos) {
		return wrappedUnstructuredLayoutBufferImpl(pipe, 
        		          Pipe.slab(pipe)[pipe.slabMask & (int)(pos)], 
        		          Pipe.slab(pipe)[pipe.slabMask & (int)(pos + 1)]);
	}

	private static ByteBuffer[] wrappedUnstructuredLayoutBufferImpl(Pipe pipe, int meta, int len) {
		if (meta >= 0) {
			return Pipe.wrappedReadingBuffersRing(pipe, len, (pipe).blobMask & Pipe.convertToPosition(meta,pipe));
		} else {
			return Pipe.wrappedReadingBuffersConst(pipe, meta, len);
		}
	}


    /**
     * Reads byte array at specified location in pipe
     * @param pipe pipe to check
     * @param loc location to read from
     * @param target target byte array
     * @return length of byte array
     */
    public static int readBytes(Pipe pipe, int loc, byte[] target, int targetOffset) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc)+"  b"+Integer.toBinaryString(loc);

    	long tmp = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);

        int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp)];
        int len = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp + 1)];
                
        if (pos < 0) {
            readBytesConst(pipe,len,target,targetOffset,POS_CONST_MASK & pos);
        } else {
            readBytesRing(pipe,len,target,targetOffset,Pipe.restorePosition(pipe, pos));
        }
        return len;
    }
    
    private static void readBytesConst(Pipe pipe, int len, byte[] target, int targetloc, int pos) {
            byte[] buffer = pipe.blobConstBuffer;
            while (--len >= 0) {
                target[targetloc++]=buffer[pos++]; //TODO:M replace with arrayCopy
            }
    }

    private static void readBytesRing(Pipe pipe, int len, byte[] target, int targetloc, int pos) {
            byte[] buffer = Pipe.blob(pipe);
            int mask = pipe.blobMask;
            while (--len >= 0) {
                target[targetloc++]=buffer[mask & pos++]; //TODO:M replace with dual arrayCopy as seen elsewhere
            }
    }
    
    public static int readBytes(Pipe pipe, int loc, byte[] target, int targetOffset, int targetMask) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

        long tmp = pipe.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
		int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp)];
        int len = Pipe.slab(pipe)[pipe.slabMask & (int)(tmp + 1)];
                
        if (len >= 0) {
        	if (len>0) {
		        if (pos < 0) {
		            readBytesConst(pipe,len,target, targetOffset,targetMask, POS_CONST_MASK & pos);
		        } else {
		            Pipe.copyBytesFromToRing(Pipe.blob(pipe), Pipe.restorePosition(pipe, pos), pipe.blobMask, target, targetOffset, targetMask,	len);
		        }
        	}
	        return len;
        } else {
        	return 0;
        }
    }

    /**
     * Copies int from specified pipe ring to target pipe ring
     * @param sourceRing to copy from
     * @param targetRing to copy to
     * @param sourceLOC location of the int to copy
     * @param targetLOC location to put the copied int
     */
    public static void copyInt(final Pipe sourceRing,	final Pipe targetRing, int sourceLOC, int targetLOC) {
        assert(LOCUtil.isLocOfAnyType(sourceLOC, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(sourceLOC);
        assert(LOCUtil.isLocOfAnyType(targetLOC, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(targetLOC);
        
		Pipe.primaryBuffer(targetRing)[targetRing.slabMask &((int)targetRing.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(targetLOC>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&targetLOC))] =
		     Pipe.primaryBuffer(sourceRing)[sourceRing.slabMask & (int)(sourceRing.ringWalker.activeReadFragmentStack[STACK_OFF_MASK&(sourceLOC>>STACK_OFF_SHIFT)]+(OFF_MASK&sourceLOC))];
    }

    /**
     * Copies long from specified pipe to target pipe
     * @param sourcePipe to copy from
     * @param targetPipe to copy to
     * @param sourceLOC location of long to copy
     * @param targetLOC location to put copied long
     */
    public static void copyLong(final Pipe sourcePipe, final Pipe targetPipe, int sourceLOC, int targetLOC) {
    	assert((sourceLOC&0x1C<<PipeReader.OFF_BITS)==(0x4<<PipeReader.OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((sourceLOC>>PipeReader.OFF_BITS)&TokenBuilder.MASK_TYPE);
    	assert((targetLOC&0x1C<<PipeWriter.OFF_BITS)==(0x4<<PipeWriter.OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((targetLOC>>PipeWriter.OFF_BITS)&TokenBuilder.MASK_TYPE);
		long srcIdx = sourcePipe.ringWalker.activeReadFragmentStack[PipeReader.STACK_OFF_MASK&(sourceLOC>>PipeReader.STACK_OFF_SHIFT)] +(PipeReader.OFF_MASK&sourceLOC);   	
		long targetIdx = (targetPipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(targetLOC>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&targetLOC));	
		Pipe.slab(targetPipe)[targetPipe.slabMask & (int)targetIdx]     = Pipe.slab(sourcePipe)[sourcePipe.slabMask & (int)srcIdx];
		Pipe.slab(targetPipe)[targetPipe.slabMask & (int)targetIdx+1] = Pipe.slab(sourcePipe)[sourcePipe.slabMask & (int)srcIdx+1];
    }

    /**
     * Copies decimal from specified pipe ring to target pipe ring
     * @param sourceRing to copy from
     * @param targetRing to copy to
     * @param sourceLOC location of decimal to copy
     * @param targetLOC location to put copied decimal
     */
    public static void copyDecimal(final Pipe sourceRing, final Pipe targetRing, int sourceLOC, int targetLOC) {
    	assert((sourceLOC&0x1E<<PipeReader.OFF_BITS)==(0x0C<<PipeReader.OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((sourceLOC>>PipeReader.OFF_BITS)&TokenBuilder.MASK_TYPE);
    	assert((targetLOC&0x1E<<PipeWriter.OFF_BITS)==(0x0C<<PipeWriter.OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((targetLOC>>PipeWriter.OFF_BITS)&TokenBuilder.MASK_TYPE); 

    	long srcIdx = sourceRing.ringWalker.activeReadFragmentStack[PipeReader.STACK_OFF_MASK&(sourceLOC>>PipeReader.STACK_OFF_SHIFT)] +(PipeReader.OFF_MASK&sourceLOC);   	
		long targetIdx = (targetRing.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(targetLOC>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&targetLOC));	
	
		int[] tSlab = Pipe.slab(targetRing);
		int[] sSlab = Pipe.slab(sourceRing);
		tSlab[targetRing.slabMask & (int)targetIdx]   = sSlab[sourceRing.slabMask & (int)srcIdx];
		tSlab[targetRing.slabMask & (int)targetIdx+1] = sSlab[sourceRing.slabMask & (int)srcIdx+1];
		tSlab[targetRing.slabMask & (int)targetIdx+2] = sSlab[sourceRing.slabMask & (int)srcIdx+2];
		
    }

    /**
     * Copies bytes from specified pipe to target pipe
     * @param sourcePipe to copy from
     * @param targetPipe to copy to
     * @param sourceLOC location of bytes to copy
     * @param targetLOC location to put bytes
     * @return length of copied bytes
     */
	public static int copyBytes(final Pipe sourcePipe,	final Pipe targetPipe, int sourceLOC, int targetLOC) {
        assert(LOCUtil.isLocOfAnyType(sourceLOC, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(sourceLOC);
        assert(LOCUtil.isLocOfAnyType(targetLOC, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(targetLOC);
	
        //alternate implementation
        DataInputBlobReader src = PipeReader.inputStream(sourcePipe, sourceLOC);
        DataOutputBlobWriter tgt = PipeWriter.outputStream(targetPipe);
        DataOutputBlobWriter.openField(tgt);
        int len = src.available();
        tgt.writeStream(src, len);
        
        if (src.isStructured) {
        	src.readFromEndInto(tgt);
        }
        
        DataOutputBlobWriter.closeHighLevelField(tgt, targetLOC);
        return len;
	}

	private static int copyBytes(final Pipe targetPipe, int targetLOC, int length) {
	    assert(LOCUtil.isLocOfAnyType(targetLOC, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(targetLOC);

	    int byteWrkHdPos = Pipe.getWorkingBlobHeadPosition(targetPipe);
	    
		Pipe.validateVarLength(targetPipe, length);	
		Pipe.setBytePosAndLen(Pipe.slab(targetPipe),
				targetPipe.slabMask, 
				targetPipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(targetLOC>>STACK_OFF_SHIFT)]+(OFF_MASK&targetLOC), 
				byteWrkHdPos, length, Pipe.bytesWriteBase(targetPipe)); 
	
		Pipe.addAndGetBytesWorkingHeadPosition(targetPipe, length);
		return length;
	}
    
    private static void readBytesConst(Pipe pipe, int len, byte[] target, int targetloc, int targetMask, int pos) {
            byte[] buffer = pipe.blobConstBuffer;
            while (--len >= 0) {//TODO:M replace with double arrayCopy as seen elsewhere
                target[targetMask & targetloc++]=buffer[pos++];
            }
    }

	/**
	 * Copies current message from input ring to output ring.  Once copied that message is no longer readable.
	 * Message could be read before calling this copy using a low-level look ahead technique.
	 * 
	 * Returns false until the full message is copied.
	 * 
	 * Once called must continue to retry until true is returned or the message will be left in a partial state.
	 * 
	 *  NEVER follow this with publish since it has already been done.
	 * 
	 * @param pipeIn
	 * @param pipeOut
	 */
	public static boolean tryMoveSingleMessage(Pipe pipeIn, Pipe pipeOut) {
		
		assert( (!PipeReader.hasContentToRead(pipeIn)) 
				|| (!PipeWriter.hasRoomForWrite(pipeOut))
				|| PipeMonitor.monitor(pipeIn,
						pipeIn.ringWalker.nextWorkingTail,
						Pipe.bytesReadBase(pipeIn)
						) );
		assert(Pipe.from(pipeIn) == Pipe.from(pipeOut));
		assert(Pipe.singleThreadPerPipeRead(pipeIn.id));
		//NOTE: all the reading makes use of the high-level API to manage the fragment state, this call assumes tryRead was called once already.
			
		//we may re-enter this function to continue the copy
		boolean copied = StackStateWalker.copyFragment0(pipeIn, pipeOut, Pipe.getWorkingTailPosition(pipeIn), pipeIn.ringWalker.nextWorkingTail);
		while (copied && !FieldReferenceOffsetManager.isTemplateStart(Pipe.from(pipeIn), pipeIn.ringWalker.nextCursor)) {			
			//using short circut logic so copy does not happen unless the prep is successful
			copied = StackStateWalker.prepReadFragment(pipeIn, pipeIn.ringWalker) && StackStateWalker.copyFragment0(pipeIn, pipeOut, Pipe.getWorkingTailPosition(pipeIn), pipeIn.ringWalker.nextWorkingTail);			
		}
		return copied;
	}

	public static boolean isNewMessage(StackStateWalker rw) {
		return rw.isNewMessage;
	}

	public static boolean isNewMessage(Pipe ring) {
		return ring.ringWalker.isNewMessage;
	}

	public static int getMsgIdx(Pipe rb) {
		return rb.lastMsgIdx = rb.ringWalker.msgIdx;
	}

	static int getMsgIdx(StackStateWalker rw) {
		return rw.msgIdx;
	}

	
	public static int bytesConsumedByFragment(Pipe ringBuffer) {
		return ringBuffer.ringWalker.nextWorkingTail>0 ? bytesConsumed(ringBuffer) : 0;
	}
	
	public static boolean hasContentToRead(Pipe pipe) {
	    return StackStateWalker.hasContentToRead(pipe);
	}
	
	/**
	 * @param pipe
	 * @param loc
	 * @param expected
	 * @return true if the value exists and matches the expected, when false is returned it does not mean not equals but rather undetermined.
	 */
	public static boolean peekEquals(Pipe pipe, int loc, int expected) {			
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);
		return StackStateWalker.hasContentToRead(pipe) && (expected == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc)));
	}
	
	public static boolean peekMsg(Pipe pipe, int expected) {
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
		return StackStateWalker.hasContentToRead(pipe) 
				&& (expected == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail));
	}
	
	public static boolean peekMsg(Pipe pipe, int expected1, int expected2) {
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
		return StackStateWalker.hasContentToRead(pipe) 
				&& ( 
						   (expected1 == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail))
						|| (expected2 == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail)) 
					);
	}
	
	public static boolean peekMsg(Pipe pipe, int expected1, int expected2, int expected3) {
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
		return StackStateWalker.hasContentToRead(pipe) 
				&& ( 
						   (expected1 == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail))
						|| (expected2 == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail))
						|| (expected3 == Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail))
					);
	}
	
	public static boolean peekNotMsg(Pipe pipe, int expected) {			
		return StackStateWalker.hasContentToRead(pipe) && (expected != Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail));
	}
	
	public static boolean peekNotMsg(Pipe pipe, int expected1, int expected2) {			
		return StackStateWalker.hasContentToRead(pipe) && 
			(expected1 != Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail)) && 
			(expected2 != Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail));
	}
	
	public static int peekInt(Pipe pipe, int loc) {			
		assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);
		return Pipe.readValue(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc));
	}
	
	public static long peekLong(Pipe pipe, int loc) {
		assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);
		return Pipe.readLong(Pipe.slab(pipe),pipe.slabMask,pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc));
	}
	
	public static <A extends Appendable> A peekUTF8(Pipe pipe, int loc, A target) {
		assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        return (A)Pipe.readUTF8(pipe, target, peekDataPosition(pipe, loc), PipeReader.peekDataLength(pipe, loc));
    }
	
    public static int peekDataLength(Pipe pipe, int loc) {
    	assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
        return Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc)+1)];// second int is always the length
    }
	
    public static int peekDataPosition(Pipe pipe, int loc) {
    	assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        int meta = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc))];
		return meta<0 ? POS_CONST_MASK & meta : Pipe.restorePosition(pipe, meta);
    }
    
    public static int peekDataMeta(Pipe pipe, int loc) {
    	assert(PipeReader.hasContentToRead(pipe)) : "results would not be repeatable, before peek hasContentToRead must be called.";
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        return Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.nextWorkingTail+(OFF_MASK&loc))];
    }
    
    public static byte[] peekDataBackingArray(Pipe pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	 int pos = Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.nextWorkingTail+ (OFF_MASK&loc))];
    	 return pos<0 ? pipe.blobConstBuffer :  Pipe.blob(pipe);
    }
	
    public static <S extends MessageSchema<S>> DataInputBlobReader<S> peekInputStream(Pipe<S> pipe, int loc) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc)+"  b"+Integer.toBinaryString(loc);

		DataInputBlobReader<S> stream = Pipe.inputStream(pipe);		
		DataInputBlobReader.peekHighLevelAPIField(stream, loc);
		return stream;
	}
    
	//this impl only works for simple case where every message is one fragment. 
	public static <S extends MessageSchema<S>> boolean tryReadFragment(Pipe<S> pipe) {
        
    	assert( (!PipeReader.hasContentToRead(pipe)) || PipeMonitor.monitor(pipe, 
			    			pipe.ringWalker.nextWorkingTail,
			    			Pipe.bytesReadBase(pipe)
			    			) );
    	   
		assert(pipe.usingHighLevelAPI);
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
		assert(null!=pipe.ringWalker) : "NullPointer, double check that pipe was passed into super constructor of stage.";
		if (FieldReferenceOffsetManager.isTemplateStart(Pipe.from(pipe), pipe.ringWalker.nextCursor)) {    
		    assert(StackStateWalker.isSeqStackEmpty(pipe.ringWalker)) : "Error the seqStack should be empty";
			return StackStateWalker.prepReadMessage(pipe, pipe.ringWalker);			   
	    } else {  
			return StackStateWalker.prepReadFragment(pipe, pipe.ringWalker);
	    }
	}



	private static int collectConsumedCountOfBytes(Pipe pipe) {
	    if (pipe.ringWalker.nextWorkingTail>0) { //first iteration it will not have a valid position
	        //must grab this value now, its the last chance before we allow it to be written over.
	        //these are all accumulated from every fragment, messages many have many fragments.
	        int bytesConsumed = bytesConsumed(pipe);
	        Pipe.addAndGetBytesWorkingTailPosition(pipe, bytesConsumed);
	        return bytesConsumed;
	    } 
	    return 0;
	}

    private static int bytesConsumed(Pipe pipe) {
        return Pipe.slab(pipe)[pipe.slabMask & (int)(pipe.ringWalker.nextWorkingTail-1)];
    }

    /**
     * Release the record which has been most recently read. 
     * The act of releasing a record/message allows it to now be written over.
     * @param pipe The pipe where this message is found.
     * @return The total number of variable length field bytes consumed by this now released message.
     */
	public static int releaseReadLock(Pipe pipe) {
		
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
        int consumed = collectConsumedCountOfBytes(pipe); 
	    
	    //ensure we only call for new templates.
	    if (FieldReferenceOffsetManager.isTemplateStart(Pipe.from(pipe), pipe.ringWalker.nextCursor)) {
            assert(Pipe.isReplaying(pipe) || pipe.ringWalker.nextWorkingTail!=Pipe.getWorkingTailPosition(pipe)) : "Only call release once per message";
            Pipe.markBytesReadBase(pipe); //moves us forward so we can read the next fragment/message
            Pipe.releaseBatchedReads(pipe, Pipe.getWorkingBlobRingTailPosition(pipe), pipe.ringWalker.nextWorkingTail);
	    } else {
	        Pipe.decBatchRelease(pipe);//sequence fragments must cause this number to move
	    }
	    //ensure that the working value does not fall behind the new published tail position.
	    //this allows peek by direct offset to be supported when needed
	    Pipe.setWorkingTailPosition(pipe, Pipe.tailPosition(pipe));
	    return consumed;
	}

	public static void releaseAllPendingReadLock(Pipe pipe) {
		Pipe.releaseAllPendingReadLock(pipe);
	}
	
	public static void releaseAllPendingReadLock(Pipe pipe, int consumed) {
		Pipe.releasePendingAsReadLock(pipe, consumed);
	}
	
	public static boolean readNextWithoutReleasingReadLock(Pipe pipe) {
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
        int bytesConsumed = collectConsumedCountOfBytes(pipe); 
        
        if (FieldReferenceOffsetManager.isTemplateStart(Pipe.from(pipe), pipe.ringWalker.nextCursor)) {
            assert(Pipe.isReplaying(pipe) || pipe.ringWalker.nextWorkingTail!=Pipe.getWorkingTailPosition(pipe)) : "Only call release once per message";
            //moves us forward so we can read the next fragment/message
            Pipe.markBytesReadBase(pipe); 
            PendingReleaseData.appendPendingReadRelease(pipe.pendingReleases, 
            		                                    pipe.ringWalker.nextWorkingTail, 
            		                                    Pipe.getWorkingBlobRingTailPosition(pipe), 
            		                                    bytesConsumed);
            
            return true;
        } else {
            return false;
        }
	}
	
	public static int sizeOfFragment(Pipe input) {
        return Pipe.from(input).fragDataSize[input.ringWalker.cursor];
    }
    
    public static void printFragment(Pipe input, Appendable target) {
        int cursor = input.ringWalker.cursor;
        try {
            if (cursor<0) {
                target.append("EOF").append("/n");
                return;
            }
            target.append(" new message: "+input.ringWalker.isNewMessage);
            
        } catch (IOException ioe) {
            log.error("Unable to build text for fragment.",ioe);
            throw new RuntimeException(ioe);
        }
        
        Pipe.appendFragment(input, target, cursor);
    }

    public static void readFieldIntoOutputStream(int loc, Pipe pipe, OutputStream out) throws IOException {    
        int length    = readBytesLength(pipe, loc);
        if (length>0) {                
            int off = readBytesPosition(pipe, loc) & Pipe.blobMask(pipe);
            copyFieldToOutputStream(out, length, readBytesBackingArray(pipe, loc), off, pipe.sizeOfBlobRing-off);
        }
    }
    
    public static void readFieldIntoDataOutput(int loc, Pipe pipe, DataOutput out) throws IOException {    
        int length    = readBytesLength(pipe, loc);
        if (length>0) {                
            int off = readBytesPosition(pipe, loc) & Pipe.blobMask(pipe);
            copyFieldToDataOutput(out, length, readBytesBackingArray(pipe, loc), off, pipe.sizeOfBlobRing-off);
        }
    }

    private static void copyFieldToOutputStream(OutputStream out, int length, byte[] backing, int off, int len1)
            throws IOException {
        if (len1>=length) {
            //simple add bytes
            out.write(backing, off, length); 
        } else {                        
            //rolled over the end of the buffer
            out.write(backing, off, len1);
            out.write(backing, 0, length-len1);
        }
    }

    private static void copyFieldToDataOutput(DataOutput out, int length, byte[] backing, int off, int len1)
            throws IOException {
        if (len1>=length) {
            //simple add bytes
            out.write(backing, off, length); 
        } else {                        
            //rolled over the end of the buffer
            out.write(backing, off, len1);
            out.write(backing, 0, length-len1);
        }
    }

}
