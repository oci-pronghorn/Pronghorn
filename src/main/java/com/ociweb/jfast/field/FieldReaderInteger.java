package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderInteger {
	
	//crazy big value?
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
			intValueFlags[i] = UNSET;
		}
	}

	public int readUnsignedInteger(int token) {
		//no need to set initValueFlags for field that can never be null
		return intValues[token & INSTANCE_MASK] = reader.readUnsignedInteger();
	}

	public int readUnsignedIntegerOptional(int token, int valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			intValueFlags[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			intValueFlags[instance] = SET_VALUE;
			return intValues[instance] = reader.readUnsignedIntegerNullable();
		}
	}
	
	public int readSignedInteger(int token) {
		//no need to set initValueFlags for field that can never be null
		return intValues[token & INSTANCE_MASK] = reader.readSignedInteger();
	}

	public int readSignedIntegerOptional(int token, int valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			intValueFlags[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			intValueFlags[instance] = SET_VALUE;
			return intValues[instance] = reader.readSignedIntegerNullable();
		}
	}

	public int readUnsignedIntegerConstant(int token, int valueOfOptional) {
		return (reader.popPMapBit()==0 ? valueOfOptional : intValues[token & INSTANCE_MASK]);
	}
	
	
	
	
}
