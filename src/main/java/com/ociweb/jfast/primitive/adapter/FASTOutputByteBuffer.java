package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.FASTOutput;

public class FASTOutputByteBuffer implements FASTOutput {

	private final ByteBuffer byteBuffer;
	
	public FASTOutputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}
	public int flush(byte[] source, int offset, int length, int need) {
		//if need is >= length then this call must block until length is written
		//else this call can return early after need is written			
		int requiredFlush = need>=length? length : need;
		
		int remain = byteBuffer.remaining();
		if (remain<requiredFlush) {
			
			//TODO: we have a problem and the byte buffer needs to be swapped.
			
		}		
		
		if (remain<length) {
			length = remain;
			
			
		}		
		byteBuffer.put(source, offset, length);
		return length;
	}

}
