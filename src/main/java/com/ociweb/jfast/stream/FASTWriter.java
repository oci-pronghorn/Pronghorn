package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;


public interface FASTWriter {
	
	//all the supported basic types
	void write(int id, long value);
	void write(int id, int value);
	void write(int id, int exponent, long mantissa);
	
	//multiple ways to send bytes
	void write(int id, byte[] value, int offset, int length);
	void write(int id, ByteBuffer buffer);
	
	//multiple ways to send chars
	void write(int id, CharSequence value); //Strings & CharBuffers are CharSequence
	void write(int id, char[] value, int offset, int length);
	//TODO: add write(id, textHeap, textId)
	//TODO: add write(id, byte[] value, int offset, int length, UTF8)
	
	
	
	void write(int id);//null, takes exponent id for decimal
	
	void openGroup(int id);
	void openGroup(int id, int repeat);
	void closeGroup(int id);
	void flush();
	
}
