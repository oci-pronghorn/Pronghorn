package com.ociweb.pronghorn.struct;

public interface StructBooleanListener {

	//TODO: add int[] position and int[] size
	void value(boolean value, boolean isNull, int instance, int totalCount);
	
}
