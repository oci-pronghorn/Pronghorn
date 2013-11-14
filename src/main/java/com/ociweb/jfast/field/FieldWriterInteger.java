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
	
	//only used when -ea is on to validate the field order
	private boolean isExpected(int token) {
		
		// TODO can we write a method that knows the template and can expect the next field to be written?
		return true;
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
	
	
	public void writeIntegerSigned(int value, int token) {
		assert(isExpected(token));
		writer.writeSignedInteger(value);
	}
	
	public void writeIntegerSignedOptional(int value, int token) {
		assert(isExpected(token));
		writer.writeSignedIntegerNullable(value);
	}
	
	public void writeIntegerSignedOptional(int fieldId) {
		assert(isExpected(fieldId));
		writer.writeNull();
	}
	
	public void writeIntegerSignedCopy(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeSignedInteger(value);
			intValues[idx] = value;
		}
	}
	
	public void writeIntegerSignedOptionalCopy(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeSignedIntegerNullable(value);
			intValues[idx] = value;
			intValueFlags[idx] = SET_VALUE;
		}
	}
	
	public void writeIntegerSignedOptionalCopy(int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (intValueFlags[idx]<0) { //stored value was null;
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeNull();
			intValueFlags[idx] = SET_NULL;
		}
	}
	
	public void writeIntegerSignedConstant(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;
		writer.writeSignedInteger(intValues[idx]);
	}
	
	public void writeIntegerSignedDefault(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeSignedInteger(value);
		}
	}
	
	public void writeIntegerSignedOptionalDefault(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeSignedIntegerNullable(value);
		}
	}
	
	public void writeIntegerSignedOptionalDefault(int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (intValueFlags[idx]<0) { //stored value was null;
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeNull();
		}
	}
	
	public void writeIntegerSignedIncrement(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]+1) {
			writer.writePMapBit(0);
			intValues[idx]++;
		} else {
			writer.writePMapBit(1);
			writer.writeSignedInteger(value);
			intValues[idx] = value;
		}
	}
	
	public void writeIntegerSignedDelta(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;
		writer.writeSignedInteger(value - intValues[idx]);
	}
	
	public void writeIntegerSignedOptionalDelta(int value, int token) {
		assert(isExpected(token));
		int idx = token & INSTANCE_MASK;

		writer.writePMapBit(1);
		writer.writeSignedIntegerNullable(value - intValues[idx]);
		
	}
	
	public void writeIntegerSignedOptionalDelta(int token) {
		assert(isExpected(token));
		writer.writePMapBit(0);
		writer.writeNull();
	}
	
}
