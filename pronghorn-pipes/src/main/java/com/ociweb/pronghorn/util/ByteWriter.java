package com.ociweb.pronghorn.util;

public interface ByteWriter {
	

	void write(byte[] encodedBlock, int pos, int len);
	void write(byte[] encodedBlock);
	void writeByte(int singleByte);
}
