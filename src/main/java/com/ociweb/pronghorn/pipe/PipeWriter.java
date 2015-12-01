package com.ociweb.pronghorn.pipe;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;



public class PipeWriter {

  public final static int OFF_MASK  =   FieldReferenceOffsetManager.RW_FIELD_OFF_MASK;
  public final static int STACK_OFF_MASK = FieldReferenceOffsetManager.RW_STACK_OFF_MASK;
  public final static int STACK_OFF_SHIFT = FieldReferenceOffsetManager.RW_STACK_OFF_SHIFT;
  public final static int OFF_BITS = FieldReferenceOffsetManager.RW_FIELD_OFF_BITS;
	
  static double[] powd = new double[] {
	  1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
	  1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
	  1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
	  1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
	  1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};

  static float[] powf = new float[] {
	  Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,
	  1.0E-45f,1.0E-44f,1.0E-43f,1.0E-42f,1.0E-41f,1.0E-40f,1.0E-39f,1.0E-38f,1.0E-37f,1.0E-36f,1.0E-35f,1.0E-34f,1.0E-33f,1.0E-32f,1.0E-31f,1.0E-30f,1.0E-29f,1.0E-28f,1.0E-27f,1.0E-26f,1.0E-25f,1.0E-24f,1.0E-23f,1.0E-22f,
	  1.0E-21f,1.0E-20f,1.0E-19f,1.0E-18f,1.0E-17f,1.0E-16f,1.0E-15f,1.0E-14f,1.0E-13f,1.0E-12f,1.0E-11f,1.0E-10f,1.0E-9f,1.0E-8f,1.0E-7f,1.0E-6f,1.0E-5f,1.0E-4f,0.001f,0.01f,0.1f,1.0f,10.0f,100.0f,1000.0f,10000.0f,100000.0f,1000000.0f,
	  1.0E7f,1.0E8f,1.0E9f,1.0E10f,1.0E11f,1.0E12f,1.0E13f,1.0E14f,1.0E15f,1.0E16f,1.0E17f,1.0E18f,1.0E19f,1.0E20f,1.0E21f,1.0E22f,1.0E23f,1.0E24f,1.0E25f,1.0E26f,1.0E27f,1.0E28f,1.0E29f,1.0E30f,1.0E31f,1.0E32f,1.0E33f,1.0E34f,1.0E35f,
	  1.0E36f,1.0E37f,1.0E38f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN};


    //////////////////////////////////
    ///Code after this point is part of the new high level API
    ///////////////////////////////
    
    public static void writeInt(Pipe rb, int loc, int value) {
    	//allow for all types of int and for length
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.GroupLength)): "Value found "+LOCUtil.typeAsString(loc);

		Pipe.primaryBuffer(rb)[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }
    
    public static void writeShort(Pipe rb, int loc, short value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	Pipe.primaryBuffer(rb)[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

    public static void writeByte(Pipe rb, int loc, byte value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

		Pipe.primaryBuffer(rb)[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

    public static void writeLong(Pipe rb, int loc, long value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);
  	
        int[] buffer = Pipe.primaryBuffer(rb);
		int rbMask = rb.mask;	
		
		long p = structuredPositionForLOC(rb, loc);	
		
		buffer[rbMask & (int)p] = (int)(value >>> 32);
		buffer[rbMask & (int)(p+1)] = (int)(value & 0xFFFFFFFF);		
    }

    public static void writeDecimal(Pipe rb, int loc, int exponent, long mantissa) {    	
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);     	
        int[] buffer = Pipe.primaryBuffer(rb);
		int rbMask = rb.mask;

		long p = structuredPositionForLOC(rb, loc);
		
		buffer[rbMask & (int)p++] = exponent;
		buffer[rbMask & (int)p++] = (int) (mantissa >>> 32);
		buffer[rbMask & (int)p] = (int)mantissa & 0xFFFFFFFF;		  
    }

    public static long structuredPositionForLOC(Pipe rb, int loc) {
        return rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc);
    }
    
    public static void writeFloat(Pipe rb, int loc, float value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   	
    	Pipe.setValues(Pipe.primaryBuffer(rb), rb.mask, structuredPositionForLOC(rb, loc), places, (long)Math.rint(value*powd[64+places]));
    }
    public static void writeDouble(Pipe rb, int loc, double value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	Pipe.setValues(Pipe.primaryBuffer(rb), rb.mask, structuredPositionForLOC(rb, loc), places, (long)Math.rint(value*powd[64+places]));
    }
    
    public static void writeFloatAsIntBits(Pipe rb, int loc, float value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	writeInt(rb, loc, Float.floatToIntBits(value));
    }
    
    public static void writeDoubleAsLongBits(Pipe rb, int loc,  double value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.LongSigned, TypeMask.LongSignedOptional, TypeMask.LongUnsigned, TypeMask.LongUnsignedOptional)): "Value found "+LOCUtil.typeAsString(loc); 
    	writeLong(rb, loc, Double.doubleToLongBits(value));
    }    
          //<<OFF_BITS
    private static void finishWriteBytesAlreadyStarted(Pipe rb, int loc, int length) {
        int p = Pipe.bytesWorkingHeadPosition(rb);
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	Pipe.validateVarLength(rb, length);
		writeSpecialBytesPosAndLen(rb, loc, length, p);
		
    }
    
    public static void writeBytes(Pipe rb, int loc, byte[] source, int offset, int length, int mask) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	assert(length>=0);
		Pipe.copyBytesFromToRing(source, offset, mask, Pipe.byteBuffer(rb), Pipe.bytesWorkingHeadPosition(rb), rb.byteMask, length);		
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), Pipe.bytesWorkingHeadPosition(rb), length, Pipe.bytesWriteBase(rb));
        Pipe.addAndGetBytesWorkingHeadPosition(rb, length);	
    }
        
    public static void writeBytes(Pipe rb, int loc, byte[] source) { // 01000
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	int sourceLen = source.length;
    	Pipe.validateVarLength(rb, sourceLen);
		
        assert(sourceLen>=0);		
        Pipe.copyBytesFromToRing(source, 0, Integer.MAX_VALUE, Pipe.byteBuffer(rb), Pipe.bytesWorkingHeadPosition(rb), rb.byteMask, sourceLen);   	
        Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), Pipe.bytesWorkingHeadPosition(rb), sourceLen, Pipe.bytesWriteBase(rb));
        Pipe.addAndGetBytesWorkingHeadPosition(rb,sourceLen);
    }
        
    public static void writeBytes(Pipe rb, int loc, ByteBuffer source) {  
        int length = source.remaining();
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
 
        assert(length>=0);
        int bytePos = Pipe.bytesWorkingHeadPosition(rb);
        Pipe.copyByteBuffer(source, length, rb);
        Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, Pipe.bytesWriteBase(rb));
    }
    
	public static void writeBytes(Pipe rb, int loc, ByteBuffer source, int length) {		
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	assert(length>=0);
    	int bytePos = Pipe.bytesWorkingHeadPosition(rb);
    	Pipe.copyByteBuffer(source, length, rb);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, Pipe.bytesWriteBase(rb));
    }

	public static void writeSpecialBytesPosAndLen(Pipe rb, int loc, int length, int bytePos) {
	    assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);


	    Pipe.validateVarLength(rb,length);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, Pipe.bytesWriteBase(rb));
		Pipe.addAndGetBytesWorkingHeadPosition(rb, length);        
	}
    
    public static void writeUTF8(Pipe rb, int loc, CharSequence source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb, source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask,
				rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc),				
				Pipe.bytesWorkingHeadPosition(rb), Pipe.copyUTF8ToByte(source, source.length(), rb), Pipe.bytesWriteBase(rb));
    }

    public static void writeUTF8(Pipe rb, int loc, CharSequence source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb, source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), Pipe.bytesWorkingHeadPosition(rb), Pipe.copyUTF8ToByte(source, offset, length, rb), Pipe.bytesWriteBase(rb));
    }
        
    public static void writeUTF8(Pipe rb, int loc, char[] source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);

    	Pipe.validateVarLength(rb, source.length<<3); //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)      
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), Pipe.bytesWorkingHeadPosition(rb), Pipe.copyUTF8ToByte(source, source.length, rb), Pipe.bytesWriteBase(rb));
    }
      
    public static void writeUTF8(Pipe rb, int loc, char[] source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb, length<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)     
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), 
		        Pipe.bytesWorkingHeadPosition(rb), 
		        Pipe.copyUTF8ToByte(source, offset, length, rb), 
		        Pipe.bytesWriteBase(rb));
    }

    public static void writeASCII(Pipe rb, int loc, char[] source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb,source.length);
		int sourceLen = source.length;
        final int p = Pipe.copyASCIIToBytes(source, 0, sourceLen,	rb);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, sourceLen, Pipe.bytesWriteBase(rb));
    }
    
    public static void writeASCII(Pipe rb, int loc, char[] source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc)+" b"+Integer.toBinaryString(loc);
 
    	Pipe.validateVarLength(rb,length);
        final int p = Pipe.copyASCIIToBytes(source, offset, length,	rb);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, length, Pipe.bytesWriteBase(rb));
    }   
    
    public static void writeASCII(Pipe rb, int loc, CharSequence source) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb, source.length());
		int sourceLen = source.length();
        final int p = Pipe.copyASCIIToBytes(source, 0, sourceLen, rb);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, sourceLen, Pipe.bytesWriteBase(rb));
    }
    
    public static void writeASCII(Pipe rb, int loc, CharSequence source, int offset, int length) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	Pipe.validateVarLength(rb, length);
        final int p = Pipe.copyASCIIToBytes(source, offset, length, rb);
		Pipe.setBytePosAndLen(Pipe.primaryBuffer(rb), rb.mask, rb.ringWalker.activeWriteFragmentStack[PipeWriter.STACK_OFF_MASK&(loc>>PipeWriter.STACK_OFF_SHIFT)] + (PipeWriter.OFF_MASK&loc), p, length, Pipe.bytesWriteBase(rb));
    }
    
    public static void writeIntAsText(Pipe rb, int loc, int value) {
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	int max = 12+ Pipe.bytesWorkingHeadPosition(rb);
    	int len = Pipe.leftConvertIntToASCII(rb, value, max);    	
    	finishWriteBytesAlreadyStarted(rb, loc, len);
        Pipe.addAndGetBytesWorkingHeadPosition(rb,len);  	
	}

    public static void writeLongAsText(Pipe rb, int loc, long value) { 
        assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.ByteArray, TypeMask.ByteArrayOptional)): "Value found "+LOCUtil.typeAsString(loc);
        
    	int max = 21+Pipe.bytesWorkingHeadPosition(rb);
    	int len = Pipe.leftConvertLongToASCII(rb, value, max);
    	finishWriteBytesAlreadyStarted(rb, loc, len);
        Pipe.addAndGetBytesWorkingHeadPosition(rb,len);   	
	}

	public static void publishEOF(Pipe ring) {		
		StackStateWalker.writeEOF(ring);		
		Pipe.publishWorkingHeadPosition(ring, ring.ringWalker.nextWorkingHead = ring.ringWalker.nextWorkingHead + Pipe.EOF_SIZE);	
		
	}

    public static void publishWrites(Pipe outputRing) {
	
	    Pipe.writeTrailingCountOfBytesConsumed(outputRing, outputRing.ringWalker.nextWorkingHead -1 ); 

		//single length field still needs to move this value up, so this is always done
	    Pipe.updateBytesWriteLastConsumedPos(outputRing);
		
		if ((Pipe.decBatchPublish(outputRing)>0)) {		
			Pipe.storeUnpublishedHead(outputRing);
			return;
		}
		publishWrites2(outputRing);
		 
	}

	private static void publishWrites2(Pipe outputRing) {
		assert(Pipe.workingHeadPosition(outputRing)<=outputRing.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";
		//publish writes			
		Pipe.setBytesHead(outputRing, Pipe.bytesWorkingHeadPosition(outputRing));		
		Pipe.publishWorkingHeadPosition(outputRing, Pipe.workingHeadPosition(outputRing));
		
		Pipe.beginNewPublishBatch(outputRing);
	}

	/*
	 * blocks until there is enough room for the requested fragment on the output ring.
	 * if the fragment needs a template id it is written and the workingHeadPosition is set to the first field. 
	 */
	public static void blockWriteFragment(Pipe ring, int messageTemplateLOC) {
	
	    Pipe.writeTrailingCountOfBytesConsumed(ring, ring.ringWalker.nextWorkingHead -1 ); 	       
		StackStateWalker.blockWriteFragment0(ring, messageTemplateLOC, Pipe.from(ring), ring.ringWalker);
	}

    /*
	 * Return true if there is room for the desired fragment in the output buffer.
	 * Places working head in place for the first field to be written (eg after the template Id, which is written by this method)
	 * 
	 */
	public static boolean tryWriteFragment(Pipe ring, int cursorPosition) {
	    assert(null!=ring);
		return StackStateWalker.tryWriteFragment0(ring, cursorPosition, Pipe.from(ring).fragDataSize[cursorPosition], ring.ringWalker.nextWorkingHead - (ring.sizeOfSlabRing - Pipe.from(ring).fragDataSize[cursorPosition]));
	}

    public static boolean hasRoomForWrite(Pipe ring) {
        return StackStateWalker.hasRoomForFragmentOfSizeX(ring, ring.ringWalker.nextWorkingHead - (ring.sizeOfSlabRing - FieldReferenceOffsetManager.maxFragmentSize( Pipe.from(ring))));
    }
	
    public static boolean hasRoomForFragmentOfSize(Pipe ring, int fragSize) {
	    return StackStateWalker.hasRoomForFragmentOfSizeX(ring, ring.ringWalker.nextWorkingHead - (ring.sizeOfSlabRing - fragSize));
	}

    public static void setPublishBatchSize(Pipe rb, int size) {
		Pipe.setPublishBatchSize(rb, size);
	}
	
    
}
