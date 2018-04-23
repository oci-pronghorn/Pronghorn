package com.ociweb.pronghorn.util;

//@FunctionalInterface
public interface ByteWriter {
	void write(byte b[], int pos, int len);
	void write(byte[] b);
}
