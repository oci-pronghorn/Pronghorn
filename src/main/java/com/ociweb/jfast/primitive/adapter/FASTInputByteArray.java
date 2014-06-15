//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTInput;


public class FASTInputByteArray implements FASTInput {

	private byte[] buffer;
	private int limit;
	private int position;
	private byte[] targetBuffer;
	
	public FASTInputByteArray(byte[] buffer) {
		this.buffer = buffer;
		this.limit = buffer.length;
	}
	
	public FASTInputByteArray(byte[] buffer, int limit) {
		this.buffer = buffer;
		this.limit = limit;
	}
	
	public int remaining() {
		return limit-position;
	}
	
	public void reset(byte[] buffer) {
		this.position = 0;
		this.buffer = buffer;
		this.limit = buffer.length;
	}
	
	public void reset() {
		position = 0;
	}
	
	public int fill(int offset, int length) {
		if (length > limit-position) {
			length = limit-position;
		}
		System.arraycopy(buffer, position, targetBuffer, offset, length);
		position+=length;
		return length;
	}

	@Override
	public void init(byte[] targetBuffer) {
		this.targetBuffer = targetBuffer;
	}

	@Override
	public boolean isEOF() {
		return position>=limit;
	}

	public byte[] getSource() {
		return buffer;
	}

    @Override
    public int blockingFill(int offset, int count) {
        return fill(offset,count);
    }
}
