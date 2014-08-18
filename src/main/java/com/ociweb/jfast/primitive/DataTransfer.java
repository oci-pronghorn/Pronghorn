//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.nio.ByteBuffer;


public final class DataTransfer {

	public final ByteBuffer wrappedByteBuffer;
	public final PrimitiveWriter writer;
		
	public DataTransfer(PrimitiveWriter writer) {
		this.writer = writer;
		this.wrappedByteBuffer = ByteBuffer.wrap(writer.buffer);
	}
	
}
