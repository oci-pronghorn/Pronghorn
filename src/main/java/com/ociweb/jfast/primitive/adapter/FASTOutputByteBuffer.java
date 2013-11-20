package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;

public class FASTOutputByteBuffer implements FASTOutput {

	private final ByteBuffer byteBuffer;
	private ByteBuffer sourceBuffer;
	private DataTransfer dataTransfer;
	
	public FASTOutputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}
	public int flush(byte[] source, int offset, int length) {
		//if need is >= length then this call must block until length is written
		//else this call can return early after need is written			

		int remain = byteBuffer.remaining(); //final method

		if (remain<length) {
			length = remain;
		}	
		
		sourceBuffer.clear();
		sourceBuffer.position(offset);
		sourceBuffer.limit(offset+length);
		byteBuffer.put(sourceBuffer);
		
		//byteBuffer.put(source, offset, length);
		
		return length;
	}
	@Override
	public void init(DataTransfer dataTransfer) {
		this.sourceBuffer = dataTransfer.wrap();
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

}
