package com.ociweb.pronghorn.struct;

public interface BStructDimRationalListener {

	void value(long numerator, long denominator, boolean isNull, int[] position, int instance, int totalCount);
	
}
