package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

//as long as leaf implementation is used these get in-lined.
public interface FASTReader {

	//single method for reading all int types. 
	//forcing all callers to provide a value for optional allows the
	//dynamic modification of templates with respect to the optional field.
	
	//Note that caller need not know anything about the encoding.
	//each of these can be encoded in different ways and use different 
	//compression styles.  None of that information is knowable here.
	//This allows the code to continue even after template changes.
	
	int readInt(int id, int valueOfOptional);
	long readLong(int id, long valueOfOptional);
	int readDecimalExponent(int id, int valueOfOptional);	
	long readDecimalMantissa(int id, long valueOfOptional);
	
	//if bytes are optionally missing the target will not be modified.
	void readBytes(int id, ByteBuffer target);	
	int readBytes(int id, byte[] target, int offset); //returns count of bytes written

	//if chars are optionally missing the target will not be modified.
	void readChars(int id, Appendable target);
	int readChars(int id, char[] target, int offset);
	
	void openGroup(int id);
	void closeGroup(int id);
	
	//TODO: new future api ideas
	//void readBytes(int id, ByteIterator iterator); //iterator knows length and next 
	//void readChars(int id, CharIterator iterator); //iterator knows length and next 
	/////Other ideas for managment of strings.
	//TODO: if new string is "same" as old return same instance
	//String readChars(int id, String oldValue);
	//TODO: modify builder as needed to match new value
	//void readChars(int id, StringBuilder value);
	
}
