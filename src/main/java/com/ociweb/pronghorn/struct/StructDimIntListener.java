package com.ociweb.pronghorn.struct;

public interface StructDimIntListener {

	//TODO: add int[] position and int[] size
	void value(int value, boolean isNull, int[] position, int instance, int totalCount);
	
}
