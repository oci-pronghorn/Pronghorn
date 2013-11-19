package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;


public final class DataTransfer {

	private ByteBuffer wrappedByteBuffer;
		
	public DataTransfer(PrimitiveWriter writer) {
		wrappedByteBuffer = ByteBuffer.wrap(writer.buffer);
	}
	
	public DataTransfer(PrimitiveReader reader) {
		wrappedByteBuffer = ByteBuffer.wrap(reader.buffer);
	}

	public ByteBuffer wrap() {
		return wrappedByteBuffer;
	}
	
	
	
}
