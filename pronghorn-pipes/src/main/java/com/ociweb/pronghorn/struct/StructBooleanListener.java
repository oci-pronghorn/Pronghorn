package com.ociweb.pronghorn.struct;

public interface StructBooleanListener {

	void value(boolean value, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
