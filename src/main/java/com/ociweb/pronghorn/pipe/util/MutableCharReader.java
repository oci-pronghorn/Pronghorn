package com.ociweb.pronghorn.pipe.util;

import java.io.IOException;
import java.io.Reader;

public class MutableCharReader extends Reader implements Appendable {

	private final StringBuilder buffer = new StringBuilder();
	

	@Override
	public Appendable append(CharSequence source) throws IOException {
		buffer.append(source);
		return this;
	}


	@Override
	public Appendable append(char source) throws IOException {
		buffer.append(source);
		return this;
	}


	@Override
	public Appendable append(CharSequence source, int start, int end) throws IOException {
		buffer.append(source, start, end);
		return this;
	}
	
	public void clear() {
		buffer.setLength(0);
	}
	
	
	@Override
	public void close() throws IOException {
		buffer.setLength(0);
	}

	@Override
	public int read(char[] target, int off, int len) throws IOException {
		int j = 0;
		for(int i=off; i<off+len; i++) {
			target[i] = buffer.charAt(j++);
		}
		return j;
	}

}
