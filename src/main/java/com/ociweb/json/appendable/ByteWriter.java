package com.ociweb.json.appendable;

//@FunctionalInterface
public interface ByteWriter {
	void write(byte b[], int pos, int len);
	void write(byte[] b);
}
