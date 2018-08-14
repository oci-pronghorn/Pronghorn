package com.ociweb.pronghorn.struct;

public interface StructDoubleListener {

	void value(double value, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
