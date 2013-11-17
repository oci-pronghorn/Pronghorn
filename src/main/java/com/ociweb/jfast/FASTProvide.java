package com.ociweb.jfast;

public interface FASTProvide {

	//all the supported basic types
	boolean provideNull(int id);//must call first to know if we have a value

	long provideLong(int id);
	int provideInt(int id);
	byte[] provideBytes(int id);
	CharSequence provideCharSequence(int id);
	void provideDecimal(int id, DecimalDTO target);//TODO: test change to pull both values

	void openGroup(int maxPMapBytes);
	void closeGroup();
	
}
