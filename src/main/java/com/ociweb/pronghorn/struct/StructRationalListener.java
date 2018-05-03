package com.ociweb.pronghorn.struct;

public interface StructRationalListener {

	void value(long numerator, long denominator, boolean isNull, int instance, int totalCount);
	
}
