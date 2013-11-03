package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTInput;


public class FASTInputByteArray implements FASTInput {

	private byte[] buffer;
	private int position;
	
	public FASTInputByteArray(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public void reset() {
		position = 0;
	}
	
	public int fill(byte[] target, int offset, int length) {
		if (length > buffer.length-position) {
			length = buffer.length-position;
		}
		System.arraycopy(buffer, position, target, offset, length);
		position+=length;
		return length;
	}
}
