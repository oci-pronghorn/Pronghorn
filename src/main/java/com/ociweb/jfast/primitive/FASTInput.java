package com.ociweb.jfast.primitive;

public interface FASTInput {

	int fill (byte[] buffer, int offset, int count);
	
	void init(DataTransfer dataTransfer);
}
