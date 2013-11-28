package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterInteger {

	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveWriter writer;
	
	private final int[]  intValues;
	private final byte[] intValueFlags;


	public FieldWriterInteger(PrimitiveWriter writer, int fields) {
		this.writer = writer;
		this.intValues = new int[fields];
		this.intValueFlags = new byte[fields];
	}
	
	public void reset() {
		int i = intValueFlags.length;
		while (--i>=0) {
			intValueFlags[i] = 0;
		}
	}

	/*
	 * Method name convention to group the work 
	 *  write <FIELD_TYPE><OPERATOR>
	 *  
	 *  example FIELD_TYPES 
	 *  IntegerSigned
	 *  IntegerUnsigned
	 *  IntegerSingedOptional
	 *  IntegerUnsignedOptional
	 * 
	 */
	
	public void writeIntegerUnsigned(int value, int token) {
		int idx = token & INSTANCE_MASK;
		intValues[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeUnsignedInteger(value);
	}
	
	public void writeIntegerUnsignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeUnsignedInteger(value);
			intValues[idx] = value;
		}
	}
	
	public void writeIntegerUnsignedOptionalCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeUnsignedIntegerNullable(value);
			intValues[idx] = value;
			intValueFlags[idx] = SET_VALUE;
		}
	}
	
	public void writeIntegerUnsignedOptionalCopy(int token) {
		int idx = token & INSTANCE_MASK;

		if (intValueFlags[idx]<0) { //stored value was null;
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeNull();
			intValueFlags[idx] = SET_NULL;
		}
	}
	
	public void writeIntegerUnsignedConstant(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeUnsignedInteger(intValues[idx]);
	}
	
	public void writeIntegerUnsignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeUnsignedInteger(value);
		}
	}
	
	public void writeIntegerUnsignedOptionalDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeUnsignedIntegerNullable(value);
		}
	}
	
	public void writeIntegerUnignedOptionalDefault(int token) {
		int idx = token & INSTANCE_MASK;

		if (intValueFlags[idx]<0) { //stored value was null;
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeNull();
		}
	}
	
	public void writeIntegerUnsignedIncrement(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == ++intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeUnsignedInteger(value);
			intValues[idx] = value;
		}
	}
	
	public void writeIntegerUnsignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeSignedInteger(value - intValues[idx]);
	}
	
	
	public void writeIntegerUnsignedOptionalDelta(int token) {
		writer.writePMapBit(0);
		writer.writeNull();
	}

	public void flush() {
		writer.flush();
	}

	public void writeIntegerUnsignedIncrementOptional(int value, int token) {
		// TODO Auto-generated method stub
		
	}

	public void writeIntegerUnsignedOptionalDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;

		writer.writePMapBit(1);
		writer.writeUnsignedIntegerNullable(value - intValues[idx]);
		
	}

	
}
