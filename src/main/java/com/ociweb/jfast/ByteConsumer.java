package com.ociweb.jfast;

public interface ByteConsumer {

	public void clear();
	public void put(byte data);
	public void reset(byte[] source, int offset, int length, int plusOne);
	public int length();
	
}
