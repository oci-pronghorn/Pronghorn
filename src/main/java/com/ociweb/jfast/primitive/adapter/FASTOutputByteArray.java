package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTOutput;


public class FASTOutputByteArray implements FASTOutput {

	private final byte[] buffer;
	private int position;
	
	
	public FASTOutputByteArray(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public void reset() {
		position = 0;
	}
	
	public final int flush(byte[] source, int offset, int length) {

		if (length+position >= buffer.length) {
			length = buffer.length - position;
		}	
						
		System.arraycopy(source,offset,buffer,position,length);
		position+=length;
		return length;
	}

	public int position() {
		return position;
	}

}
