//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;


public interface FASTOutput {

	void flush();
	
	void init(DataTransfer dataTransfer);
	
	

	//TODO: C, Add blocking FASTOutput consumer on the complete answer of preamble. 
	//part of write cache from primtiive allowing fixed size modifications before flush, Done as FASTOutput wrapper?
	
	
}
