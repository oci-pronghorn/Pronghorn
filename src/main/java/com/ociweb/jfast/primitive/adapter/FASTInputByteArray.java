//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;


public class FASTInputByteArray implements FASTInput {

	private byte[] buffer;
	private int limit;
	private int position;
	
	public FASTInputByteArray(byte[] buffer) {
		this.buffer = buffer;
		this.limit = buffer.length;
	}
	
	public FASTInputByteArray(byte[] buffer, int limit) {
		this.buffer = buffer;
		this.limit = limit;
	}
	
	public void reset(byte[] buffer) {
		this.position = 0;
		this.buffer = buffer;
		this.limit = buffer.length;
	}
	
	public void reset() {
		position = 0;
	}
	
	public int fill(byte[] target, int offset, int length) {
		if (length > limit-position) {
			length = limit-position;
		}
		System.arraycopy(buffer, position, target, offset, length);
		position+=length;
		return length;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
	}
}
