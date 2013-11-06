package com.ociweb.jfast;


public interface FASTAccept {
	
	//all the supported basic types
	void accept(int id, long value);
	void accept(int id, int value);
	void accept(int id, int exponent, long manissa);
	void accept(int id, BytesSequence value);
	//Must consume CharSequence and not hold it! It is backed by the incoming buffer and is about to change.
	//Caller should use toString() or append it to StringBuilder or iterate over the chars or call subSequence.
	void accept(int id, CharSequence value);
	void accept(int id);//null
	
}
