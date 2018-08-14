package com.ociweb.pronghorn.util.field;

public interface BytesFieldProcessor {

	public boolean process(byte[] backing, int position, int length, int mask);
	
}
