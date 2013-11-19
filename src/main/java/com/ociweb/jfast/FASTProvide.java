package com.ociweb.jfast;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public interface FASTProvide {

	int readInt(int id);
	int readInt(int id, int valueOfOptional);
	
	long readLong(int id);
	long readLong(int id, long valueOfOptional);
	
	void readBytes(int id, ByteBuffer target);
	int readBytes(int id, byte[] target, int offset); //returns count of bytes written
	//void readBytes(int id, ByteIterator ); //TODO: new api providing, length and next 
	
	//as long as leaf is used these get in-lined.
	int readDecimalExponent(int id, int valueOfOptional);	
	int readDecimalExponent(int id);
	long readDecimalMantissa(int id);
	
	void readChars(int id, CharBuffer target);
	int readChars(int id, char[] target, int offset);
	void readChars(int id, StringBuilder target);
	//void readChars(int id, CharIterator iterator); //TODO: new api providing, length and next 
	
	void openGroup(int maxPMapBytes);
	void closeGroup();
	
}
