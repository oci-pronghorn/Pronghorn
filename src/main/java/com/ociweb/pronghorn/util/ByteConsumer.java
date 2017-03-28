package com.ociweb.pronghorn.util;

public interface ByteConsumer {

	void consume(byte[] backing, int pos, int len, int mask);
	void consume(byte value);

}
