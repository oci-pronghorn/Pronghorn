package com.ociweb.jfast;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class MyCharSequnce implements CharSequence, ByteConsumer {

	private char[] seq;
	private int limit;
	//private CharSequence prefix;//NONE
	
	//TODO: perhaps another interface may be good for streaming the write here
	public MyCharSequnce() {
		seq = new char[PrimitiveReader.optimizeStringMask];//make the same optimization assumption here
		limit = 0;
	}
	
	public final void put(byte value) {
		//grow to the biggest string found in stream then stop growing.
		if (limit >= seq.length) {
			grow(limit);
		}
		seq[limit++]=(char)value;
	}
	

	public final void reset(byte[] source, int offset, int total, int plusOne) {
		
		//reset always sets limit back to zero.
		if(total >= seq.length) {
			grow(total);
		}

		int i = total-1;
		int j = offset+i;
		seq[i] = (char) plusOne;
		while (--i>=0) {//can not use array copy due to cast
			seq[i] = (char) source[--j];
		}
		limit = total;
	}

	private void grow(int target) {
		int newCapacity = target*2;
		char[] newSeq = new char[newCapacity];
		System.arraycopy(seq, 0, newSeq, 0, seq.length);
		seq = newSeq;
	}
	
	//package protect this method for write.
	public final void put(char value) {
		//grow to the biggest string found in stream then stop growing.
		if (limit>=seq.length) {
			grow(limit);
		}
		seq[limit++]=value;
	}

	
	public final int length() {
		return limit;
	}

	public final char charAt(int index) {
		return seq[index];
	}

	public CharSequence subSequence(int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}

	public String toString() {
		return new String(seq, 0, limit);
	}
	
	private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	
	public void test() {
		
		//TODO: must test NIO vs intputSteam vs String implementations for fastest converter.
		
		CharsetDecoder decoder = UTF8_CHARSET.newDecoder();
		CharsetEncoder encoder = UTF8_CHARSET.newEncoder();
		//encoder.
		//decoder.
	}

	public final void clear() {
		limit = 0;
	}

	public CharSequence setValue(CharSequence value) {
		if (null==value) {//not sure this belongs here.
			clear();
			return null;
		}
		int i = value.length();
		limit = i;
		if (limit>=seq.length) {
			grow(limit);
		}
		while (--i>=0) {
			seq[i]=value.charAt(i);
		}
		return this;
	}


}
