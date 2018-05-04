package com.ociweb.pronghorn.struct;

public interface StructFloatListener {

	//TODO: add int[] position and int[] size
	void value(float value, boolean isNull, int instance, int totalCount);
	
}
