package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputByteBuffer implements FASTInput {

	private ByteBuffer byteBuffer;
	private ByteBuffer targetBuffer;
	
	public FASTInputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}
	
	public int fill(byte[] target, int offset, int length) {
		if (length > byteBuffer.remaining()) {
			length = byteBuffer.remaining();
			
			if (length==0) {
				return 0;
			}
		}
		targetBuffer.clear();
		targetBuffer.position(offset);
		targetBuffer.limit(offset+length);
		int temp = byteBuffer.limit();
		byteBuffer.limit(byteBuffer.position()+length);
		targetBuffer.put(byteBuffer);
		byteBuffer.limit(temp);
		
		return length;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		targetBuffer = dataTransfer.wrap();
	}

	public void reset() {
		byteBuffer.flip();
	}
}
