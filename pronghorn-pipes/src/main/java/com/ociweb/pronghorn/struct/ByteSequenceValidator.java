package com.ociweb.pronghorn.struct;

public interface ByteSequenceValidator {

	boolean isValid(byte[] backing, int position, int length, int mask);
	
}
