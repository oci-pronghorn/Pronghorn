package com.ociweb.pronghorn.ring;

public class RingBufferConfig {
	
	//try to keep all this under 20MB and 1 RB under 64K if possible under 256K is highly encouraged
	public final byte primaryBits;
	public final byte byteBits;
	public final byte[] byteConst;
	public final FieldReferenceOffsetManager from;

	public RingBufferConfig(byte primaryBits, byte byteBits, byte[] byteConst, FieldReferenceOffsetManager from) {
		this.primaryBits = primaryBits;
		this.byteBits = byteBits;
		this.byteConst = byteConst;
		this.from = from;
	}
	
	public RingBufferConfig(FieldReferenceOffsetManager from) {
		//default size which is smaller than half of 64K because this is the L1 cache size on intel haswell.
		this.primaryBits = 7;
		this.byteBits = 14;
		this.byteConst = null;
		this.from = from;
		//validate
    	FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, 1<<primaryBits);
	}
		
	
	//TODO: Build other constructors that take the known maximum string and compute primary and byte bits.

}
