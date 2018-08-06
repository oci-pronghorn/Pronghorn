package com.ociweb.pronghorn.struct;

public interface DecimalValidator {

	boolean isValid(boolean isNull, long mantissa, byte expondent);
	
}
