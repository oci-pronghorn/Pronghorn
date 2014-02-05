//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.error;


public class FASTException extends RuntimeException {

	private static final long serialVersionUID = 8294717647055016321L;

	public FASTException() {
		
	}

	public FASTException(Throwable e) {
		super(e);
	}

	public FASTException(FASTError error) {
		super(error.toString());
	}

	public FASTException(String error) {
		super(error);
	}
}
