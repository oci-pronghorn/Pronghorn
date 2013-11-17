package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderInteger {
	
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveReader reader;
	
	private final int[]  intValues;
	private final byte[] intValueFlags;


	public FieldReaderInteger(PrimitiveReader reader, int fields) {
		this.reader = reader;
		this.intValues = new int[fields];
		this.intValueFlags = new byte[fields];
	}
	
	public void reset() {
		int i = intValueFlags.length;
		while (--i>=0) {
			intValueFlags[i] = 0;
		}
	}
	
	//only used when -ea is on to validate the field order
	private boolean isExpected(int token) {
		
		// TODO can we write a method that knows the template and can expect the next field to be written?
		return true;
	}
	
	
	
	
	
}
