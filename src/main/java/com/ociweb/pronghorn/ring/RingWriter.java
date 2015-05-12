package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;



public class RingWriter {

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
    
    public static void writeInt(RingBuffer rb, int loc, int value) {
    	//allow for all types of int and for length
    	assert((loc&0x1C<<OFF_BITS)==0 || (loc&0x1F<<OFF_BITS)==(0x14<<OFF_BITS)) : "Expected to write some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		rb.buffer[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }
    
    public static void writeShort(RingBuffer rb, int loc, short value) {
    	assert((loc&0x1C<<OFF_BITS)==0) : "Expected to write some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
    	rb.buffer[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

    public static void writeByte(RingBuffer rb, int loc, byte value) {
    	assert((loc&0x1C<<OFF_BITS)==0) : "Expected to write some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		rb.buffer[rb.mask &((int)rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc))] = value;         
    }

    public static void writeLong(RingBuffer rb, int loc, long value) {
    	assert((loc&0x1C<<OFF_BITS)==(0x4<<OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   	
        int[] buffer = rb.buffer;
		int rbMask = rb.mask;	
		
		long p = (rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc));	
		
		buffer[rbMask & (int)p] = (int)(value >>> 32);
		buffer[rbMask & (int)(p+1)] = (int)(value & 0xFFFFFFFF);		
    }

    public static void writeDecimal(RingBuffer rb, int loc, int exponent, long mantissa) {    	
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);     	
        int[] buffer = rb.buffer;
		int rbMask = rb.mask;

		long p = (rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc));
		
		buffer[rbMask & (int)p++] = exponent;
		buffer[rbMask & (int)p++] = (int) (mantissa >>> 32);
		buffer[rbMask & (int)p] = (int)mantissa & 0xFFFFFFFF;		  
    }
    
    public static void writeFloat(RingBuffer rb, int loc, float value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);   	
    	RingBuffer.setValues(rb.buffer, rb.mask, (rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc)), places, (long)Math.rint(value*powd[64+places]));
    }
    public static void writeDouble(RingBuffer rb, int loc, double value, int places) {
    	assert((loc&0x1E<<OFF_BITS)==(0x0C<<OFF_BITS)) : "Expected to write some type of decimal but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	RingBuffer.setValues(rb.buffer, rb.mask, (rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc)), places, (long)Math.rint(value*powd[64+places]));
    }
    
    public static void writeFloatAsIntBits(RingBuffer rb, int loc, float value) {
    	assert((loc&0x1C<<OFF_BITS)==0) : "Expected to write some type of int but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
    	writeInt(rb, loc, Float.floatToIntBits(value));
    }
    
    public static void writeDoubleAsLongBits(RingBuffer rb, int loc,  double value) {
    	assert((loc&0x1C<<OFF_BITS)==(0x4<<OFF_BITS)) : "Expected to write some type of long but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE); 
    	writeLong(rb, loc, Double.doubleToLongBits(value));
    }    
          //<<OFF_BITS
    private static void finishWriteBytesAlreadyStarted(RingBuffer rb, int loc, int length) {
        int p = RingBuffer.bytesWorkingHeadPosition(rb);
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	RingBuffer.validateVarLength(rb, length);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), p, length, RingBuffer.bytesWriteBase(rb));
        
		RingBuffer.addAndGetBytesWorkingHeadPosition(rb, length);        
    }
    
    public static void writeBytes(RingBuffer rb, int loc, byte[] source, int offset, int length, int mask) {
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	assert(length>=0);
		RingBuffer.copyBytesFromToRing(source, offset, mask, rb.byteBuffer, RingBuffer.bytesWorkingHeadPosition(rb), rb.byteMask, length);		
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), RingBuffer.bytesWorkingHeadPosition(rb), length, RingBuffer.bytesWriteBase(rb));
        RingBuffer.addAndGetBytesWorkingHeadPosition(rb, length);	
    }
        
    public static void writeBytes(RingBuffer rb, int loc, byte[] source) { // 01000
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	int sourceLen = source.length;
    	RingBuffer.validateVarLength(rb, sourceLen);
		
        assert(sourceLen>=0);		
        RingBuffer.copyBytesFromToRing(source, 0, Integer.MAX_VALUE, rb.byteBuffer, RingBuffer.bytesWorkingHeadPosition(rb), rb.byteMask, sourceLen);   	
        RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), RingBuffer.bytesWorkingHeadPosition(rb), sourceLen, RingBuffer.bytesWriteBase(rb));
        RingBuffer.addAndGetBytesWorkingHeadPosition(rb,sourceLen);
    }
        
	public static void writeBytes(RingBuffer rb, int loc, ByteBuffer source, int length) {		
		assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0x5<<OFF_BITS || (loc&0x1E<<OFF_BITS)==TypeMask.ByteArray<<OFF_BITS) : "Expected to write some type of ASCII/UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
		
    	assert(length>=0);
    	int bytePos = RingBuffer.bytesWorkingHeadPosition(rb);
    	RingBuffer.copyByteBuffer(source, length, rb);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[STACK_OFF_MASK&(loc>>STACK_OFF_SHIFT)] + (OFF_MASK&loc), bytePos, length, RingBuffer.bytesWriteBase(rb));
    }
    
    public static void writeUTF8(RingBuffer rb, int loc, CharSequence source) {
    	assert((loc&0x1E<<OFF_BITS)==TypeMask.TextUTF8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==TypeMask.ByteArray<<OFF_BITS) : "Expected to write some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb, source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask,
				rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc),				
				RingBuffer.bytesWorkingHeadPosition(rb), RingBuffer.copyUTF8ToByte(source, source.length(), rb), RingBuffer.bytesWriteBase(rb));
    }

    public static void writeUTF8(RingBuffer rb, int loc, CharSequence source, int offset, int length) {
    	assert((loc&0x1E<<OFF_BITS)==TypeMask.TextUTF8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==TypeMask.ByteArray<<OFF_BITS) : "Expected to write some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb, source.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), RingBuffer.bytesWorkingHeadPosition(rb), RingBuffer.copyUTF8ToByte(source, offset, length, rb), RingBuffer.bytesWriteBase(rb));
    }
        
    public static void writeUTF8(RingBuffer rb, int loc, char[] source) {
    	assert((loc&0x1E<<OFF_BITS)==TypeMask.TextUTF8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==TypeMask.ByteArray<<OFF_BITS) : "Expected to write some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
    	
    	RingBuffer.validateVarLength(rb, source.length<<3); //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)      
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), RingBuffer.bytesWorkingHeadPosition(rb), RingBuffer.copyUTF8ToByte(source, source.length, rb), RingBuffer.bytesWriteBase(rb));
    }
      
    public static void writeUTF8(RingBuffer rb, int loc, char[] source, int offset, int length) {
    	
    	assert((loc&0x1E<<OFF_BITS)==TypeMask.TextUTF8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==TypeMask.ByteArray<<OFF_BITS) : "Expected to write some type of UTF8/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
    	
    	RingBuffer.validateVarLength(rb, length<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)     
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), RingBuffer.bytesWorkingHeadPosition(rb), RingBuffer.copyUTF8ToByte(source, offset, length, rb), RingBuffer.bytesWriteBase(rb));
    }

    public static void writeASCII(RingBuffer rb, int loc, char[] source) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb,source.length);
		int sourceLen = source.length;
        final int p = RingBuffer.copyASCIIToBytes(source, 0, sourceLen,	rb);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), p, sourceLen, RingBuffer.bytesWriteBase(rb));
    }
    
    public static void writeASCII(RingBuffer rb, int loc, char[] source, int offset, int length) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb,length);
        final int p = RingBuffer.copyASCIIToBytes(source, offset, length,	rb);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), p, length, RingBuffer.bytesWriteBase(rb));
    }   
    
    public static void writeASCII(RingBuffer rb, int loc, CharSequence source) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb, source.length());
		int sourceLen = source.length();
        final int p = RingBuffer.copyASCIIToBytes(source, 0, sourceLen, rb);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), p, sourceLen, RingBuffer.bytesWriteBase(rb));
    }
    
    public static void writeASCII(RingBuffer rb, int loc, CharSequence source, int offset, int length) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	RingBuffer.validateVarLength(rb, length);
        final int p = RingBuffer.copyASCIIToBytes(source, offset, length, rb);
		RingBuffer.setBytePosAndLen(rb.buffer, rb.mask, rb.ringWalker.activeWriteFragmentStack[RingWriter.STACK_OFF_MASK&(loc>>RingWriter.STACK_OFF_SHIFT)] + (RingWriter.OFF_MASK&loc), p, length, RingBuffer.bytesWriteBase(rb));
    }
    
    public static void writeIntAsText(RingBuffer rb, int loc, int value) {
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);

    	int max = 12+ RingBuffer.bytesWorkingHeadPosition(rb);
    	int len = RingBuffer.leftConvertIntToASCII(rb, value, max);    	
    	finishWriteBytesAlreadyStarted(rb, loc, len);
        RingBuffer.addAndGetBytesWorkingHeadPosition(rb,len);  	
	}

    public static void writeLongAsText(RingBuffer rb, int loc, long value) { 
    	assert((loc&0x1E<<OFF_BITS)==0x8<<OFF_BITS || (loc&0x1E<<OFF_BITS)==0xE<<OFF_BITS) : "Expected to write some type of ASCII/BYTE but found "+TypeMask.toString((loc>>OFF_BITS)&TokenBuilder.MASK_TYPE);
  
    	int max = 21+RingBuffer.bytesWorkingHeadPosition(rb);
    	int len = RingBuffer.leftConvertLongToASCII(rb, value, max);
    	finishWriteBytesAlreadyStarted(rb, loc, len);
        RingBuffer.addAndGetBytesWorkingHeadPosition(rb,len);   	
	}

	public static void publishEOF(RingBuffer ring) {
		
		assert(RingBuffer.workingHeadPosition(ring)<=ring.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";
		ring.llrTailPosCache = spinBlockOnTail(ring.llrTailPosCache, RingBuffer.workingHeadPosition(ring) - (ring.maxSize - RingBuffer.EOF_SIZE), ring);
		
		assert(RingBuffer.tailPosition(ring)+ring.maxSize>=RingBuffer.headPosition(ring)+RingBuffer.EOF_SIZE) : "Must block first to ensure we have 2 spots for the EOF marker";
		RingBuffer.setBytesHead(ring, RingBuffer.bytesWorkingHeadPosition(ring));
		ring.buffer[ring.mask &((int)ring.ringWalker.nextWorkingHead +  RingBuffer.from(ring).templateOffset)]    = -1;	
		ring.buffer[ring.mask &((int)ring.ringWalker.nextWorkingHead +1 +  RingBuffer.from(ring).templateOffset)] = 0;
		
		RingBuffer.publishWorkingHeadPosition(ring, ring.ringWalker.nextWorkingHead = ring.ringWalker.nextWorkingHead + RingBuffer.EOF_SIZE);	
		
	}
	
	public static boolean tryPublishEOF(RingBuffer ring) {
		
		assert(RingBuffer.workingHeadPosition(ring)<=ring.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";				
		long nextTailTarget = RingBuffer.workingHeadPosition(ring) - (ring.maxSize - RingBuffer.EOF_SIZE);
				
        if (ring.llrTailPosCache < nextTailTarget) {
        	ring.llrTailPosCache = RingBuffer.tailPosition(ring);
			if (ring.llrTailPosCache < nextTailTarget) {
				return false;
			}
		}
		
		assert(RingBuffer.tailPosition(ring)+ring.maxSize>=RingBuffer.headPosition(ring)+RingBuffer.EOF_SIZE) : "Must block first to ensure we have 2 spots for the EOF marker";
		RingBuffer.setBytesHead(ring, RingBuffer.bytesWorkingHeadPosition(ring));
		ring.buffer[ring.mask &((int)ring.ringWalker.nextWorkingHead +  RingBuffer.from(ring).templateOffset)]    = -1;	
		ring.buffer[ring.mask &((int)ring.ringWalker.nextWorkingHead +1 +  RingBuffer.from(ring).templateOffset)] = 0;
		
		RingBuffer.publishWorkingHeadPosition(ring, ring.ringWalker.nextWorkingHead = ring.ringWalker.nextWorkingHead + RingBuffer.EOF_SIZE);	
	
		return true;
		
	}
	

	public static void publishWrites(RingBuffer outputRing) {
	
	    RingBuffer.writeTrailingCountOfBytesConsumed(outputRing, outputRing.ringWalker.nextWorkingHead -1 ); 

		//single length field still needs to move this value up, so this is always done
		outputRing.bytesWriteLastConsumedBytePos = RingBuffer.bytesWorkingHeadPosition(outputRing);
		
		if ((--outputRing.batchPublishCountDown>0)) {		
			RingBuffer.storeUnpublishedHead(outputRing);
			return;
		}
		publishWrites2(outputRing);
		 
	}

	private static void publishWrites2(RingBuffer outputRing) {
		assert(RingBuffer.workingHeadPosition(outputRing)<=outputRing.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";
		//publish writes			
		RingBuffer.setBytesHead(outputRing, RingBuffer.bytesWorkingHeadPosition(outputRing));		
		RingBuffer.publishWorkingHeadPosition(outputRing, RingBuffer.workingHeadPosition(outputRing));
		
		outputRing.batchPublishCountDown = outputRing.batchPublishCountDownInit;
	}

	/*
	 * blocks until there is enough room for the requested fragment on the output ring.
	 * if the fragment needs a template id it is written and the workingHeadPosition is set to the first field. 
	 */
	public static void blockWriteFragment(RingBuffer ring, int messageTemplateLOC) {
	
	    RingBuffer.writeTrailingCountOfBytesConsumed(ring, ring.ringWalker.nextWorkingHead -1 ); 
	       
		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		
		RingWalker consumerData = ring.ringWalker;
		int fragSize = from.fragDataSize[messageTemplateLOC];
		ring.llrTailPosCache = spinBlockOnTail(ring.llrTailPosCache, consumerData.nextWorkingHead - (ring.maxSize - fragSize), ring);
	
		RingWalker.prepWriteFragment(ring, messageTemplateLOC, from, fragSize);
	}

	/*
	 * Return true if there is room for the desired fragment in the output buffer.
	 * Places working head in place for the first field to be written (eg after the template Id, which is written by this method)
	 * 
	 */
	public static boolean tryWriteFragment(RingBuffer ring, int cursorPosition) {
		int fragSize = RingBuffer.from(ring).fragDataSize[cursorPosition];
		long target = ring.ringWalker.nextWorkingHead - (ring.maxSize - fragSize);
		return RingWalker.tryWriteFragment1(ring, cursorPosition, RingBuffer.from(ring), fragSize, target, ring.llrTailPosCache >=  target);
	}

	public static void setPublishBatchSize(RingBuffer rb, int size) {
		RingBuffer.setPublishBatchSize(rb, size);
	}
	
    
}
