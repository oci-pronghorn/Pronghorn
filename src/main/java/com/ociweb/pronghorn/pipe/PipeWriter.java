package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;



public class PipeWriter {

  private static final Logger logger = LoggerFactory.getLogger(PipeWriter.class);
  
  public final static int OFF_MASK  =   FieldReferenceOffsetManager.RW_FIELD_OFF_MASK;
  public final static int STACK_OFF_MASK = FieldReferenceOffsetManager.RW_STACK_OFF_MASK;
  public final static int STACK_OFF_SHIFT = FieldReferenceOffsetManager.RW_STACK_OFF_SHIFT;
  public final static int OFF_BITS = FieldReferenceOffsetManager.RW_FIELD_OFF_BITS;
	
  public static double[] powd = new double[] {
	  1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
	  1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
	  1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
	  1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
	  1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};

//  public static float[] powf = new float[] {
//	  Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,
//	  1.0E-45f,1.0E-44f,1.0E-43f,1.0E-42f,1.0E-41f,1.0E-40f,1.0E-39f,1.0E-38f,1.0E-37f,1.0E-36f,1.0E-35f,1.0E-34f,1.0E-33f,1.0E-32f,1.0E-31f,1.0E-30f,1.0E-29f,1.0E-28f,1.0E-27f,1.0E-26f,1.0E-25f,1.0E-24f,1.0E-23f,1.0E-22f,
//	  1.0E-21f,1.0E-20f,1.0E-19f,1.0E-18f,1.0E-17f,1.0E-16f,1.0E-15f,1.0E-14f,1.0E-13f,1.0E-12f,1.0E-11f,1.0E-10f,1.0E-9f,1.0E-8f,1.0E-7f,1.0E-6f,1.0E-5f,1.0E-4f,0.001f,0.01f,0.1f,1.0f,10.0f,100.0f,1000.0f,10000.0f,100000.0f,1000000.0f,
//	  1.0E7f,1.0E8f,1.0E9f,1.0E10f,1.0E11f,1.0E12f,1.0E13f,1.0E14f,1.0E15f,1.0E16f,1.0E17f,1.0E18f,1.0E19f,1.0E20f,1.0E21f,1.0E22f,1.0E23f,1.0E24f,1.0E25f,1.0E26f,1.0E27f,1.0E28f,1.0E29f,1.0E30f,1.0E31f,1.0E32f,1.0E33f,1.0E34f,1.0E35f,
//	  1.0E36f,1.0E37f,1.0E38f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN};


    //////////////////////////////////
    ///Code after this point is part of the new high level API
    ///////////////////////////////
    
    public static void writeInt(Pipe pipe, int loc, int value) {
    	//allow for all types of int and for length
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);

		long p = structuredPositionForLOC(pipe, loc);	
		
		assert(p < pipe.ringWalker.nextWorkingHead-1L) : "Is this field applicable for the this pipes schema? "+pipe.schemaName(pipe);
		
		Pipe.slab(pipe)[pipe.slabMask & (int)p] = value;  
		
    }
    
    /**
     * 
     * @param pipe to be updated
     * @param loc for the field to be updated
     * @param value to be ORed with the existing data in this position
     */
    public static void accumulateBitsValue(Pipe pipe, int loc, int value) {
    	//allow for all types of int and for length
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);

		Pipe.slab(pipe)[pipe.slabMask &((int)pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] |= value;         
    }

	/**
	 *
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param value short to write to specified location
	 */
    public static void writeShort(Pipe pipe, int loc, short value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	Pipe.slab(pipe)[pipe.slabMask &((int)pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

	/**
	 *
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param value byte to write to specified location
	 */
    public static void writeByte(Pipe pipe, int loc, byte value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

		Pipe.slab(pipe)[pipe.slabMask &((int)pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

	/**
	 *
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param value long to write to specified location
	 */
    public static void writeLong(Pipe pipe, int loc, long value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

		long p = structuredPositionForLOC(pipe, loc);
		assert(p+1L < pipe.ringWalker.nextWorkingHead) :
			  "Is this field applicable for the this pipes schema? "+pipe.schemaName(pipe)+" Or message? "+
		      "\n next write position is "+p+" next limit is "+pipe.ringWalker.nextWorkingHead+
			  "\n Pipe "+pipe+
		      "\n loc "+loc;
				
		int[] buffer = Pipe.slab(pipe);
		int rbMask = pipe.slabMask;	
		buffer[rbMask & (int)p] = (int)(value >>> 32);
		buffer[rbMask & (int)(p+1)] = (int)(value & 0xFFFFFFFF);		
    }

    public static void writeDecimal(Pipe pipe, int loc, int exponent, long mantissa) {    	
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);     	
        int[] buffer = Pipe.slab(pipe);
		int rbMask = pipe.slabMask;

		long p = structuredPositionForLOC(pipe, loc);
		assert(p+2L <= pipe.ringWalker.nextWorkingHead-1L) : "Is this field applicable for the this pipes schema? "+pipe.schemaName(pipe);
		
		
		buffer[rbMask & (int)p++] = exponent;
		buffer[rbMask & (int)p++] = (int) (mantissa >>> 32);
		buffer[rbMask & (int)p] = (int)mantissa & 0xFFFFFFFF;		  
    }

    public static long structuredPositionForLOC(Pipe pipe, int loc) {
        return pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
    }
    
    public static void writeFloat(Pipe pipe, int loc, float value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   	
    	Pipe.setValues(Pipe.slab(pipe), pipe.slabMask, structuredPositionForLOC(pipe, loc), -places, (long)Math.rint(value*powd[64+places]));
    }
    public static void writeDouble(Pipe pipe, int loc, double value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	Pipe.setValues(Pipe.slab(pipe), pipe.slabMask, structuredPositionForLOC(pipe, loc), -places, (long)Math.rint(value*powd[64+places]));
    }

	/**
	 *
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param value float to write to specified location
	 */
    public static void writeFloatAsIntBits(Pipe pipe, int loc, float value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	writeInt(pipe, loc, Float.floatToIntBits(value));
    }
    
    public static void writeDoubleAsLongBits(Pipe pipe, int loc,  double value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc); 
    	writeLong(pipe, loc, Double.doubleToLongBits(value));
    }    
          //<<OFF_BITS
    private static void finishWriteBytesAlreadyStarted(Pipe pipe, int loc, int length) {
        int p = Pipe.getWorkingBlobHeadPosition( pipe);
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	Pipe.validateVarLength(pipe, length);
		writeSpecialBytesPosAndLen(pipe, loc, length, p);
		
    }
    
    public static void writeBytes(Pipe pipe, int loc, byte[] source, int offset, int length) {
        assert(offset+length<=source.length) : "out of bounds";
        writeBytes(pipe,loc,source,offset,length,Integer.MAX_VALUE);
    }
    
    public static void writeBytes(Pipe pipe, int loc, byte[] source, int offset, int length, int mask) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	assert(length>=0);
		Pipe.copyBytesFromToRing(source, offset, mask, Pipe.blob(pipe), Pipe.getWorkingBlobHeadPosition(pipe), pipe.blobMask, length);		
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), Pipe.getWorkingBlobHeadPosition( pipe), length, Pipe.bytesWriteBase(pipe));
        Pipe.addAndGetBytesWorkingHeadPosition(pipe, length);	
    }
        
    public static void writeBytes(Pipe pipe, int loc, byte[] source) { // 01000
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	int sourceLen = source.length;
    	Pipe.validateVarLength(pipe, sourceLen);
		
        assert(sourceLen>=0);		
        Pipe.copyBytesFromArrayToRing(source, 0, Pipe.blob(pipe), Pipe.getWorkingBlobHeadPosition( pipe), pipe.blobMask, sourceLen);   	
        Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), Pipe.getWorkingBlobHeadPosition( pipe), sourceLen, Pipe.bytesWriteBase(pipe));
        Pipe.addAndGetBytesWorkingHeadPosition(pipe,sourceLen);
    }
        
    public static void writeBytes(Pipe pipe, int loc, ByteBuffer source) {  
        int length = source.remaining();
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
 
        assert(length>=0);
        int bytePos = Pipe.getWorkingBlobHeadPosition( pipe);
        Pipe.copyByteBuffer(source, length, pipe);
        Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, Pipe.bytesWriteBase(pipe));
    }
    
	public static void writeBytes(Pipe pipe, int loc, ByteBuffer source, int length) {		
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	assert(length>=0);
    	int bytePos = Pipe.getWorkingBlobHeadPosition( pipe);
    	Pipe.copyByteBuffer(source, length, pipe);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, Pipe.bytesWriteBase(pipe));
    }

	public static void writeSpecialBytesPosAndLen(Pipe pipe, int loc, int length, int bytePos) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

	    Pipe.validateVarLength(pipe,length);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), 
				     pipe.slabMask, 
				     pipe.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), 
				     bytePos, 
				     length, 
				     Pipe.bytesWriteBase(pipe));
		Pipe.addAndGetBytesWorkingHeadPosition(pipe, length>=0?length:0);        
	}

	/**
	 * Writes UTF8 to defined location in specified pipe
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param source CharSequence to write to specified location
	 */
    public static void writeUTF8(Pipe pipe, int loc, CharSequence source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
        int sourceLen = null==source? -1 : source.length();
        int actualByteCount;
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask,
				pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc),				
				Pipe.getWorkingBlobHeadPosition( pipe), actualByteCount = Pipe.copyUTF8ToByte(source,0, sourceLen, pipe), Pipe.bytesWriteBase(pipe));
		Pipe.validateVarLength(pipe, actualByteCount); //throws if too many bytes were decoded, there is no recover for the data on the pipe.
    }

    public static void writeUTF8(Pipe pipe, int loc, CharSequence source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
        int sourceLen = null==source? -1 : source.length();
    	Pipe.validateVarLength(pipe, sourceLen<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), Pipe.getBlobWorkingHeadPosition(pipe), Pipe.copyUTF8ToByte(source, offset, length, pipe), Pipe.bytesWriteBase(pipe));
    }

	/**
	 * Writes UTF8 to defined location in specified pipe
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param source char[] to write to specified location
	 */
	public static void writeUTF8(Pipe pipe, int loc, char[] source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);

        int sourceLen = null==source? -1 : source.length;        
    	Pipe.validateVarLength(pipe, sourceLen<<3); //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)      
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), Pipe.getBlobWorkingHeadPosition(pipe), Pipe.copyUTF8ToByte(source, sourceLen, pipe), Pipe.bytesWriteBase(pipe));
    }
      
    public static void writeUTF8(Pipe pipe, int loc, char[] source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(pipe, length<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)     
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), 
		        Pipe.getBlobWorkingHeadPosition( pipe), 
		        Pipe.copyUTF8ToByte(source, offset, length, pipe), 
		        Pipe.bytesWriteBase(pipe));
    }

	/**
	 * Writes ASCII to defined location in specified pipe
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param source char[] to write to specified location
	 */
    public static void writeASCII(Pipe pipe, int loc, char[] source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
        int sourceLen = null==source?-1:source.length;
    	Pipe.validateVarLength(pipe,sourceLen);
        final int p = Pipe.copyASCIIToBytes(source, 0, sourceLen,	pipe);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, sourceLen, Pipe.bytesWriteBase(pipe));
    }
    
    public static void writeASCII(Pipe pipe, int loc, char[] source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc)+" b"+Integer.toBinaryString(loc);
 
    	Pipe.validateVarLength(pipe,length);
        final int p = Pipe.copyASCIIToBytes(source, offset, length,	pipe);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, length, Pipe.bytesWriteBase(pipe));
    }

	/**
	 * Writes ASCII to defined location in specified pipe
	 * @param pipe to be updated
	 * @param loc for the field to be updated
	 * @param source CharSequence to write to specified location
	 */
	public static void writeASCII(Pipe pipe, int loc, CharSequence source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
        int sourceLen = null==source?-1:source.length();
    	Pipe.validateVarLength(pipe, sourceLen);
        final int p = Pipe.copyASCIIToBytes(source, 0, sourceLen, pipe);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, sourceLen, Pipe.bytesWriteBase(pipe));
    }
    
    public static void writeASCII(Pipe pipe, int loc, CharSequence source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(pipe, length);
        final int p = Pipe.copyASCIIToBytes(source, offset, length, pipe);
		Pipe.setBytePosAndLen(Pipe.slab(pipe), pipe.slabMask, pipe.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, length, Pipe.bytesWriteBase(pipe));
    }
    
    public static void writeIntAsText(Pipe pipe, int loc, int value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	int max = 12+ Pipe.getBlobWorkingHeadPosition(pipe);
    	int len = Pipe.leftConvertIntToASCII(pipe, value, max);    	
    	finishWriteBytesAlreadyStarted(pipe, loc, len);
        Pipe.addAndGetBytesWorkingHeadPosition(pipe,len);  	
	}

	/**
	 *
	 * @param pipe to be updated
	 * @param loc for field to be updated
	 * @param value long to write to specified location
	 */
    public static void writeLongAsText(Pipe pipe, int loc, long value) { 
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	int max = 21+Pipe.getBlobWorkingHeadPosition(pipe);
    	int len = Pipe.leftConvertLongToASCII(pipe, value, max);
    	finishWriteBytesAlreadyStarted(pipe, loc, len);
        Pipe.addAndGetBytesWorkingHeadPosition(pipe,len);   	
	}

    public static void publishEOF(Pipe[] pipe) {
    	int i = pipe.length;
    	while (--i>=0) {
    		if (null!=pipe[i] && Pipe.isInit(pipe[i])) {
    			publishEOF(pipe[i]);
    		}
    	}
    }
    
	public static void publishEOF(Pipe pipe) {	
		int i = pipe.pubListeners.length;
    	while (--i>=0) {
    		((PipePublishListener)(pipe.pubListeners[i])).published();
    	}
		
		assert(Pipe.singleThreadPerPipeWrite(pipe.id));
		StackStateWalker.writeEOF(pipe);
		Pipe.publishWorkingHeadPosition(pipe, pipe.ringWalker.nextWorkingHead = pipe.ringWalker.nextWorkingHead + Pipe.EOF_SIZE);		
	}

	public static void publishEOL(Pipe pipe) {
		publishEOF(pipe);
	}
	
    public static int publishWrites(Pipe pipe) {
    	int i = pipe.pubListeners.length;
    	while (--i>=0) {
    		((PipePublishListener)(pipe.pubListeners[i])).published();
    	}
	
		
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
        assert(Pipe.workingHeadPosition(pipe)!=Pipe.headPosition(pipe)) : "Fragment was already published, check the workflow logic and remove call to publishWrites(pipe)";

		int consumed = Pipe.writeTrailingCountOfBytesConsumed(pipe, pipe.ringWalker.nextWorkingHead-1 ); 

		//single length field still needs to move this value up, so this is always done
	    Pipe.updateBytesWriteLastConsumedPos(pipe);
		
		if ((Pipe.decBatchPublish(pipe)>0)) {		
			Pipe.storeUnpublishedWrites(pipe);
			return consumed;
		}
		
		publishWrites2(pipe);
		return consumed; 
	}

	private static void publishWrites2(Pipe pipe) {
		
		//TODO: Not sure this assert works when we write high and read low....
		//assert(Pipe.workingHeadPosition(pipe) <= pipe.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";
		
		//publish writes			
		Pipe.setBytesHead(pipe, Pipe.getWorkingBlobHeadPosition(pipe));		
		Pipe.publishWorkingHeadPosition(pipe, Pipe.workingHeadPosition(pipe));

		Pipe.beginNewPublishBatch(pipe);
	}

	/*
	 * blocks until there is enough room for the requested fragment on the output ring.
	 * if the fragment needs a template id it is written and the workingHeadPosition is set to the first field. 
	 * 
	 */
	@Deprecated
	public static void blockWriteFragment(Pipe pipe, int messageTemplateLOC) { //caution does not have same behavior as tryWrite.
	    //maximize code re-use and unify behavior of the writeFragment methods.
	    Pipe.writeTrailingCountOfBytesConsumed(pipe, pipe.ringWalker.nextWorkingHead -1 ); 	       
		StackStateWalker.blockWriteFragment0(pipe, messageTemplateLOC, Pipe.from(pipe), pipe.ringWalker);
	}

	/**
	 * Copy message previously send and publish it again.
	 * 
	 * @param pipe
	 * @param historicSlabPosition
	 * @param historicBlobPosition
	 */
	public static boolean tryReplication(Pipe pipe, 
			                             final long historicSlabPosition, 
			                             final int historicBlobPosition) {
		
		assert(Pipe.singleThreadPerPipeWrite(pipe.id));
	

		final int[] slab = pipe.slab(pipe);
		final int idx = (int)historicSlabPosition & pipe.slabMask;		
		final int msgIdx = slab[idx]; //false share as this is a dirty read
				
		//first part is to protect against dirty reading		
		if ((msgIdx<Pipe.from(pipe).fragDataSize.length) 
			 && Pipe.headPosition(pipe) == pipe.workingHeadPosition(pipe)	
			 && tryWriteFragment(pipe, msgIdx)) {
			final byte[] blob = pipe.blob(pipe);
			
			//get the sizes of ints and bytes
			int slabMsgSize = Pipe.sizeOf(pipe,msgIdx);
			int blobMsgSize = slab[(int)(historicSlabPosition+slabMsgSize-1) & pipe.slabMask];
			
			//copy all the bytes
			int blobPos = Pipe.getWorkingBlobHeadPosition(pipe);
			Pipe.copyBytesFromToRing(blob, historicBlobPosition, Pipe.blobMask(pipe), blob, blobPos, Pipe.blobMask(pipe), blobMsgSize);			
			Pipe.addAndGetBytesWorkingHeadPosition(pipe, blobMsgSize);

			//copy all the ints
			long slabPos = Pipe.headPosition(pipe);
			//the header is already written by tryWriteFragment so pos is up by one and length is short by one
			Pipe.copyIntsFromToRing(slab, idx+1, Pipe.slabMask(pipe), slab, (int)slabPos+1, Pipe.slabMask(pipe), slabMsgSize-1);	

			//Appendables.appendHexArray(System.out.append("replicate slab: "), '[', slab, historicSlabPosition, pipe.slabMask, ']', slabMsgSize).append('\n');
			
			
//			logger.info("replicate data from old:{} {} new:{} {} ",
//					     historicBlobPosition, historicSlabPosition,
//					     (blobPos&Pipe.blobMask(pipe)), (slabPos&Pipe.slabMask(pipe)));
			
			
			return true;
		} else {
			return false;
		}
	}
	
	/*
	 * We assume that the pipe has room and this was already checked earlier in the code.
	 * This method will call tryWriteFragment and if is not successful it will.
	 *  1. log an error so the developer can ensure this pipe is checked before calling this method.
	 *  2. continues to tryWrite until successful (may not return) this makes is a blocking call.
	 *  
	 *  This behaves as a blocking call if there is an error in the code up stream...
	 */
	public static void presumeWriteFragment(Pipe pipe, int fragmentId) {
		
		if (!tryWriteFragment(pipe,fragmentId)) {
			logger.error("Expected pipe {} to have room",pipe, new UnsupportedOperationException("Ensure PipeWriter.hasRoomForWrite(pipe) returns true before calling this presumeWriteFragment method."));
			while (!tryWriteFragment(pipe,fragmentId)) {
				Pipe.spinWork(pipe);//safe spin which watches for shutdown or interrupt.
			}
		}
	}
	
    /*
	 * Return true if there is room for the desired fragment in the output buffer.
	 * Places working head in place for the first field to be written (eg after the template Id, which is written by this method)
	 * 
	 */
	public static boolean tryWriteFragment(Pipe pipe, final int fragmentId) {
		assert(fragmentId<Pipe.from(pipe).fragDataSize.length) : "Is this pipe for the schema holding this message?";
		assert(Pipe.singleThreadPerPipeWrite(pipe.id));
	    assert(null!=pipe);
	    assert(Pipe.isInit(pipe)) : "Pipe must be initialized before use: "+pipe+" call the method initBuffers";
	
		return StackStateWalker.tryWriteFragment0(pipe, fragmentId, Pipe.from(pipe).fragDataSize[fragmentId], pipe.ringWalker.nextWorkingHead - (pipe.sizeOfSlabRing - Pipe.from(pipe).fragDataSize[fragmentId]));
	}

	/**
	 * Check to see if specified pipe has room to write
	 * @param pipe to be checked
	 * @return <code>true</code> if pipe has room else <code>false</code>
	 */
    public static boolean hasRoomForWrite(Pipe pipe) {
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
    	assert(pipe!=null);
    	assert(Pipe.isInit(pipe));
    	assert(pipe.usingHighLevelAPI);
        return StackStateWalker.hasRoomForFragmentOfSizeX(pipe, pipe.ringWalker.nextWorkingHead - (pipe.sizeOfSlabRing - FieldReferenceOffsetManager.maxFragmentSize( Pipe.from(pipe))));
    }

	/**
	 * Checks to see if specified pipe has room for a specific size of data
	 * @param pipe to be checked
	 * @param fragSize size of data to add
	 * @return <code>true</code> if data fragment fits else <code>false</code>
	 */
    public static boolean hasRoomForFragmentOfSize(Pipe pipe, int fragSize) {
	    return StackStateWalker.hasRoomForFragmentOfSizeX(pipe, pipe.ringWalker.nextWorkingHead - (pipe.sizeOfSlabRing - fragSize));
	}

    public static void setPublishBatchSize(Pipe pipe, int size) {
		Pipe.setPublishBatchSize(pipe, size);
	}
        
    public static void writeFieldFromInputStream(Pipe pipe, int loc, InputStream inputStream, final int byteCount) throws IOException { 
        buildFieldFromInputStream(pipe, loc, inputStream, byteCount, PipeReader.readBytesPosition(pipe, loc), PipeReader.readBytesMask(pipe, loc), PipeReader.readBytesBackingArray(pipe, loc), pipe.sizeOfBlobRing, PipeReader.readBytesPosition(pipe, loc), byteCount, 0);
    }

    private static void buildFieldFromInputStream(Pipe pipe, final int loc, InputStream inputStream, final int byteCount,
            int position, int byteMask, byte[] buffer, int sizeOfBlobRing, final int startPosition, int remaining, int size) throws IOException {
        
        while ( (remaining>0) && (size=Pipe.safeRead(inputStream, position&byteMask, buffer, sizeOfBlobRing, remaining))>=0 ) { 
            if (size>0) {
                remaining -= size;                    
                position += size;
            } else {
                Thread.yield();
            }
        }
        PipeWriter.writeSpecialBytesPosAndLen(pipe, loc, byteCount, startPosition);
    }

    public static void writeFieldFromDataInput(Pipe pipe, int loc, DataInput dataInput, final int byteCount) throws IOException { 
    	buildFieldFromDataInput(pipe, loc, dataInput, byteCount, PipeReader.readBytesPosition(pipe, loc), PipeReader.readBytesMask(pipe, loc), PipeReader.readBytesBackingArray(pipe, loc), pipe.sizeOfBlobRing, PipeReader.readBytesPosition(pipe, loc), byteCount, 0);
    }
    
    private static void buildFieldFromDataInput(Pipe pipe, final int loc, DataInput dataInput, final int byteCount,
            int position, int byteMask, byte[] buffer, int sizeOfBlobRing, final int startPosition, int remaining, int size) throws IOException {
        
        while ( (remaining>0) && (size=Pipe.safeRead(dataInput, position&byteMask, buffer, sizeOfBlobRing, remaining))>=0 ) { 
            if (size>0) {
                remaining -= size;                    
                position += size;
            } else {
                Thread.yield();
            }
        }
        PipeWriter.writeSpecialBytesPosAndLen(pipe, loc, byteCount, startPosition);
    }
    
    @Deprecated
	public static ByteBuffer wrappedUnstructuredLayoutBufferOpenA(Pipe<?> target, int loc) {
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		assert(PipeWriter.hasRoomForWrite(target)) : "must protect by ensuring we have room first";
		return Pipe.wrappedBlobForWritingA(Pipe.storeBlobWorkingHeadPosition(target), target);
	}
	
	@Deprecated
	public static ByteBuffer wrappedUnstructuredLayoutBufferOpenB(Pipe<?> target, int loc) {
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		assert(PipeWriter.hasRoomForWrite(target)) : "must protect by ensuring we have room first";
		return Pipe.wrappedBlobForWritingB(Pipe.getBlobWorkingHeadPosition(target), target);
	}
	
	
	/**
	 * Does not require tryWrite to be called first, we only need to check that there is room to write. This is for supporting buffer write
	 * to determine if we have data that can be written.
	 * 
	 * @param target
	 * @param loc
	 */
	public static ByteBuffer[] wrappedUnstructuredLayoutBufferOpen(Pipe<?> target, int loc) {
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		assert(PipeWriter.hasRoomForWrite(target)) : "must protect by ensuring we have room first";
		return Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target, target.maxVarLen);

	}
	
	public static ByteBuffer[] wrappedUnstructuredLayoutBufferOpen(Pipe<?> target, int maxLength, int loc) {
		assert(maxLength>=0) : "bad length of "+maxLength;
		assert(maxLength<=target.maxVarLen+1);
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		assert(PipeWriter.hasRoomForWrite(target)) : "must protect by ensuring we have room first";
		return Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target, maxLength);

	}
	
	
	
	public static void wrappedUnstructuredLayoutBufferCancel(Pipe<?> target) {
		Pipe.unstoreBlobWorkingHeadPosition(target);
	}

	public static void wrappedUnstructuredLayoutBufferClose(Pipe<?> target,	int loc, int length) {
		assert(length>=0);
		assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
		
		Pipe.validateVarLength(target,length);
		long ringPos = target.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc);		
		Pipe.slab(target)[target.slabMask & (int)ringPos] = (int)(target.sizeOfBlobRing + Pipe.unstoreBlobWorkingHeadPosition(target)-Pipe.bytesWriteBase(target)) & target.blobMask; //mask is needed for the negative case, does no harm in positive case
		Pipe.slab(target)[target.slabMask & (int)(ringPos+1)] = length;	
		Pipe.addAndGetBytesWorkingHeadPosition(target, length);
		
	}

	public static <S extends MessageSchema<S>> DataOutputBlobWriter<S> outputStream(Pipe<S> pipe) {
		return Pipe.outputStream(pipe);
	}
    
	
    
	
    
}
