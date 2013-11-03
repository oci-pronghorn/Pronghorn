package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputByteBuffer implements FASTInput {

	private ByteBuffer byteBuffer;
	
	public FASTInputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}
	
	public int fill(byte[] target, int offset, int length) {
		if (length > byteBuffer.remaining()) {
			length = byteBuffer.remaining();
		}
		byteBuffer.get(target, offset, length);
		return length;
	}
}
