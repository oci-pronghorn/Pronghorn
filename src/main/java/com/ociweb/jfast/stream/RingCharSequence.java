package com.ociweb.jfast.stream;

public class RingCharSequence implements CharSequence {

	int length;
	char[] charBuffer;
	int pos;
	int mask;
	
	public CharSequence set(char[] buffer, int pos, int mask, int length) {
		
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
		return charBuffer[(pos+at)&mask];
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		throw new UnsupportedOperationException();
	}

}
