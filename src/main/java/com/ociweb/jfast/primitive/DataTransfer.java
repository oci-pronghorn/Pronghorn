//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;


public final class DataTransfer {

	private ByteBuffer wrappedByteBuffer;
	private PrimitiveWriter writer;
		
	public DataTransfer(PrimitiveWriter writer) {
		this.writer = writer;
		this.wrappedByteBuffer = ByteBuffer.wrap(writer.buffer);
	}

	public ByteBuffer wrap() {
		return wrappedByteBuffer;
	}
	
	public byte[] rawBuffer() {
		return writer.buffer;
	}
	
	public int nextBlockSize() {
		return PrimitiveWriter.nextBlockSize(writer);
	}
	
	public int nextOffset() {
		return PrimitiveWriter.nextOffset(writer);
	}
	
}
