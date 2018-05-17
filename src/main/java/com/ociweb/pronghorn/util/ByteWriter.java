package com.ociweb.pronghorn.util;

public interface ByteWriter {
	
	/**
	 * Drop everything written so far and return cursor to beginning of the stream
	 */
	void reset();
	
	void write(byte[] encodedBlock, int pos, int len);
	void write(byte[] encodedBlock);
	void writeByte(int singleByte);
}
