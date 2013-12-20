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

		int total = 0;
		int c = 0;
		while (size>0) {
			byteBuffer.put(dataTransfer.rawBuffer(), 
			     	       dataTransfer.nextOffset(), size);

			size = dataTransfer.nextBlockSize();
			total+=size;
			c++;
		}
//		System.out.println("flush out :"+total+" in "+c+" parts");
		
	}

	public void reset() {
		byteBuffer.clear();
		
	}

}
