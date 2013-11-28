package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;

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

		int size = dataTransfer.nextBlockSize();

		while (size>0) {
			byteBuffer.put(dataTransfer.rawBuffer(), 
			     	       dataTransfer.nextOffset(), size);

			size = dataTransfer.nextBlockSize();
			
		}
	}

	public void reset() {
		byteBuffer.clear();
		
	}

}
