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
		writer.writeIntegerUnsigned(value);
	}
	
	public void writeIntegerUnsignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsigned(value);
			intValues[idx] = value;
		}
	}
	
	public void writeIntegerUnsignedCopyOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsignedOptional(intValues[idx] = value);
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
		
		//value must equal constant
		if (intValueFlags[idx]>0 && value==intValues[idx] ) {
			writer.writePMapBit(0);//use constant value
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsigned(intValues[idx]=value);
		}	
		
	}
	
	public void writeIntegerUnsignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx]) {
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsigned(value);
		}
	}
	
	public void writeIntegerUnsignedDefaultOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == intValues[idx] && intValueFlags[idx]>0) {//not null and matches
			writer.writePMapBit(0);
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsignedOptional(value);
		}
	}
	
	public void writeIntegerUnignedDefaultOptional(int token) {
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
			writer.writeIntegerUnsigned(value);
			intValues[idx] = value;
		}
	}
	

	public void writeIntegerUnsignedIncrementOptional(int value, int token) {

		int idx = token & INSTANCE_MASK;
		int old = intValues[idx];

		if (intValueFlags[idx]>0 && value == old+1) {//not null and matches
			writer.writePMapBit(0);
			intValues[idx] = old+1;
			//use the last value plus 1
		} else {
			writer.writePMapBit(1);
			writer.writeIntegerUnsignedOptional(value);
			intValues[idx] = value;
			intValueFlags[idx] = SET_VALUE;
		}
	}
	
	public void writeIntegerUnsignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeIntegerSigned(value - intValues[idx]);
		intValues[idx] = value;		
		//no need to set because this is never optional
		//intValueFlags[idx] = SET_VALUE;
	}
	


	public void flush() {
		writer.flush();
	}


	public void writeIntegerUnsignedDeltaOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - intValues[idx]);
		intValueFlags[idx] = SET_VALUE;
		intValues[idx] = value;	
	}

	public void writeIntegerNull(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeNull();
		intValueFlags[idx] = SET_NULL;
		
	}
	
	public void writeIntegerUnsignedIncrementOptional(int token) {
			int idx = token & INSTANCE_MASK;
			writer.writePMapBit(1);
			writer.writeNull();
			intValueFlags[idx] = SET_NULL;
	}

	public void writeIntegerUnsignedCopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writePMapBit(1);
		writer.writeNull();
		intValueFlags[idx] = SET_NULL;
	}

	public void writeIntegerUnsignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writePMapBit(1);
		writer.writeNull();
		intValueFlags[idx] = SET_NULL;
	}

	
}
