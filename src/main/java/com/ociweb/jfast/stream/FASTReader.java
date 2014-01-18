package com.ociweb.jfast.stream;


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
	int readBytes(int id);	
	int readText(int id);
	
	void openGroup(int id);
	void closeGroup(int id);
		
}
