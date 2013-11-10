package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.read.FASTException;

public class FASTOutputStream implements FASTOutput{

	private final OutputStream ostr;
	public FASTOutputStream(OutputStream ostr) {
		this.ostr = ostr;
	}
	public int flush(byte[] buffer, int offset, int length, int need) {
		try {
			//blocks until length is written so the logic is simple
			ostr.write(buffer, offset, length);
			return length;
		} catch (IOException e) {
			throw new FASTException(e);
		}
	}
	
	
	
	
}
