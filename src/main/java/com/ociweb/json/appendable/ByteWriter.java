package com.ociweb.json.appendable;

//@FunctionalInterface
public interface ByteWriter {
	void write(byte b[], int pos, int len);

	default void write(byte[] b) {
		write(b, 0, b.length);
	}
}
