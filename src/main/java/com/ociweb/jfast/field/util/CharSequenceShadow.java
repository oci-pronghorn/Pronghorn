package com.ociweb.jfast.field.util;

import java.util.Arrays;

public final class CharSequenceShadow implements CharSequence {

	byte[] buffer;
	int offset;
	int length;
	
	public CharSequenceShadow() {
	}
	
	private CharSequenceShadow(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}
	
	public void setBacking(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}
	
	@Override
	public int length() {
		return length;
	}

	@Override
	public char charAt(int index) {
		return (char) (0x7F&buffer[index+offset]);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return new CharSequenceShadow(Arrays.copyOfRange(buffer, offset+start, offset+end),0,end-start);
	}
	
	@Override
	public String toString() {
		int j = length();
		char[] chars = new char[j];
		int i = offset+j;
		byte[] b = buffer;
		while (--j>=0) {
			chars[j] = (char)(0x7F&b[--i]);
		}
		return new String(chars);
	}

}
