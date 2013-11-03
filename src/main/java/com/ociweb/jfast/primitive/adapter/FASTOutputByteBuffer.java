package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.FASTOutput;

public class FASTOutputByteBuffer implements FASTOutput {

	private final ByteBuffer byteBuffer;
	
	public FASTOutputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}
	public int flush(byte[] source, int offset, int length) {
		int remain = byteBuffer.remaining();
		if (remain<length) {
			length = remain;
		}		
		byteBuffer.put(source, offset, length);
		return length;
	}

}
