package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;


public final class DataTransfer {

	private PrimitiveWriter writer;
		
	public DataTransfer(PrimitiveWriter writer) {
		this.writer = writer;
	}
	
	byte[] getBuffer() {
		return writer.buffer;
	}

	public ByteBuffer wrap() {
		return ByteBuffer.wrap(writer.buffer);
	}
	
	
	
}
