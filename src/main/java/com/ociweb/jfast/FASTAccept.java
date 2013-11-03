package com.ociweb.jfast;


public interface FASTAccept {
	
	//all the supported basic types
	void accept(int id, long value);
	void accept(int id, int value);
	void accept(int id, int exponent, long manissa);
	void accept(int id, byte[] buffer, int offset, int length);//copy from must never write
	void accept(int id, CharSequence value);
	void accept(int id);//null
	
}
