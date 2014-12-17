package com.ociweb.pronghorn.ring;

public class RingBufferConfig {
	
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
	
	//TODO: Build other constructors that take the known maximum string and compute primary and byte bits.

}
