package com.ociweb.pronghorn.struct;

public interface StructLongListener {

	//TODO: add int[] position and int[] size
	void value(long value, boolean isNull, int instance, int totalCount);
	
}
