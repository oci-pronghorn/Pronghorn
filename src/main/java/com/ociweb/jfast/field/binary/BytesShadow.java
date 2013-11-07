package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.BytesSequence;

//if we end up with performance problems here delete the interface.
public final class BytesShadow implements BytesSequence {

	byte[] buffer;
	int offset;
	int length;
	
	//ensures that we provide read only access to the buffer data.
	BytesShadow() {
	}
	
	BytesShadow(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}
	
	void setBacking(byte[] buffer, int offset, int length) {
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
	
	public final void copyTo(byte[] target, int targetOffset) {
		System.arraycopy(buffer, offset, target, targetOffset, length);
	}
	
	public final byte[] toByteArray() {
		byte[] result = new byte[length];
		copyTo(result,0);
		return result;
	}

	@Override
	public boolean isEqual(byte[] that) {
		
		int i = that.length;
		if (i!=length) {
			return false;
		}
		while (--i>=0) {
			if (that[i]!=buffer[offset+i]) {
				return false;
			}
		}
		return true;
		
	}
	
	
}
