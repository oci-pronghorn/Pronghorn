package com.ociweb.jfast.primitive;



public interface FASTOutput {

	//returns count of bytes written starting from offset
	//internal buffers are out of space so flush is required before continuing
	int flush(byte[] buffer, int offset, int length);

	void init(DataTransfer dataTransfer);
	
}
