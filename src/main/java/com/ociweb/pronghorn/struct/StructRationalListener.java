package com.ociweb.pronghorn.struct;

public interface StructRationalListener {

	//TODO: add int[] position and int[] size
	void value(long numerator, long denominator, boolean isNull, int instance, int totalCount);
	
}
