package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.BytesSequence;

public class BytesShadow implements BytesSequence {

	byte[] buffer;
	int offset;
	int length;
	
	//ensures that we provide read only access to the buffer data.
	public BytesShadow() {
	}
	
	public BytesShadow(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}
	
	public void setBacking(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}
	
	public int length() {
		return length;
	}
	
	public byte byteAt(int index) {
		return buffer[offset+index];
	}
	
	public void copyTo(byte[] target, int targetOffset) {
		System.arraycopy(buffer, offset, target, targetOffset, length);
	}

	@Override
	public boolean isEqual(byte[] expected) {
		
		int i = expected.length;
		if (i!=length) {
			return false;
		}
		while (--i>=0) {
			if (expected[i]!=buffer[offset+i]) {
				return false;
			}
		}
		return true;
		
	}
	
	
}
