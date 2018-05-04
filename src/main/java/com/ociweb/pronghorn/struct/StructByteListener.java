package com.ociweb.pronghorn.struct;

public interface StructByteListener {
	
	//TODO: add int[] position and int[] size
	void value(byte value, boolean isNull, int instance, int totalCount);
	
}
