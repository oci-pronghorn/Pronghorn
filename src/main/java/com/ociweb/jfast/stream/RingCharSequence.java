package com.ociweb.jfast.stream;

public class RingCharSequence implements CharSequence {

	int length;
	byte[] charBuffer;
	int pos;
	int mask;
	
	public CharSequence set(byte[] buffer, int pos, int mask, int length) {
		
	    if (null==buffer) {
	        throw new NullPointerException();
	    }
		this.length = length;
		this.charBuffer = buffer;
		this.pos = pos;
		this.mask = mask;
		
		return this;
	}
	
	@Override
	public int length() {
		return length;
	}

	@Override
	public char charAt(int at) {
		return (char)charBuffer[(pos+at)&mask];
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		throw new UnsupportedOperationException();
	}

	public String toString() {
	    return new String(charBuffer,pos,length);
	}
}
