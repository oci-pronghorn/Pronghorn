//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

public interface FASTInput {

	int fill (byte[] buffer, int offset, int count);
	
	void init(DataTransfer dataTransfer);
}
