//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
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
	
	public int fill(int offset, int length) {
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
	public void init(byte[] targetBuffer) {
		this.targetBuffer = ByteBuffer.wrap(targetBuffer);
	}

	public void reset() {
		byteBuffer.flip();
	}

	@Override
	public boolean isEOF() {
		return byteBuffer.remaining()==0;
	}

    @Override
    public int blockingFill(int offset, int count) {
        return fill(offset,count);
    }
}
