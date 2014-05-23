//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.error;

public enum FASTError {
	NO_OP_NO_PMAP("Field must not appear in pmap."), 
	CONST_INIT("Constant manditory fields must have an initial value."),
	MANDATORY_CONSTANT_NULL("Mandatory constant must not be null."), 
	TIMEOUT("I/O stream timeout")
	;

	private String text;
	FASTError(String text) {
		this.text = text;
	}
	
	public String toString() {
		return text;
	}
	
}
