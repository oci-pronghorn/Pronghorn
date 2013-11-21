package com.ociweb.jfast.read;

import java.io.IOException;

public class FASTException extends RuntimeException {

	public FASTException() {
		
	}

	public FASTException(IOException e) {
		super(e);
	}

	public FASTException(FASTError error) {
		super(error.toString());
	}

	public FASTException(String error) {
		super(error);
	}
}
