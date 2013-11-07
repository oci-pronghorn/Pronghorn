package com.ociweb.jfast;

public interface BytesSequence {

	void copyTo(byte[] target, int targetOffset);

	boolean isEqual(byte[] expected);
	
	int length();
	
	byte byteAt(int index);
	
	byte[] toByteArray();
}
