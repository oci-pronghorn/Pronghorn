package com.ociweb.jfast.primitive;

public interface FASTInput {

	int fill (byte[] buffer, int offset, int count);
	
	//TODO: build a push version that does parse upon new packets and holds continuation for next group. (event driven)
	
	
}
