//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

//TODO: C, change interfaces to use reactive streams design

public interface FASTInput {

	int fill(int offset, int count);
		
	void init(byte[] targetBuffer);
	
	boolean isEOF();

    int blockingFill(int offset, int count);
    
}
