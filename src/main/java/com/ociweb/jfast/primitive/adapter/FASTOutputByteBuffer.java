//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTOutputByteBuffer implements FASTOutput {

	private final ByteBuffer byteBuffer;
	private DataTransfer dataTransfer;
	
	public FASTOutputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	@Override
	public void flush() {

		int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);

		while (size>0) {
			byteBuffer.put(dataTransfer.writer.buffer, 
			     	       PrimitiveWriter.nextOffset(dataTransfer.writer), size);

			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		}
	}

	public void reset() {
		byteBuffer.clear();
		
	}

}
