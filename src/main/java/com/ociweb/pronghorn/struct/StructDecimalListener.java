package com.ociweb.pronghorn.struct;

public interface StructDecimalListener {

	//TODO: add int[] position and int[] size
	void value(byte e, long m, boolean isNull, int instance, int totalCount);
	
}
