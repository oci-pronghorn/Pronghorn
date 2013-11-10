package com.ociweb.jfast.primitive;


public interface FASTOutput {

	//returns count of bytes written starting from offset
	int flush(byte[] buffer, int offset, int length, int need);

	
}
