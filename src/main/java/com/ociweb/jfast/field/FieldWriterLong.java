package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterLong {

	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveWriter writer;
	
	private final long[] lastValue;
	private final byte[] lastValueFlag;


	public FieldWriterLong(PrimitiveWriter writer, int fields) {
		this.writer = writer;
		this.lastValue = new long[fields];
		this.lastValueFlag = new byte[fields];
	}
	
	public void initValue(int token, long value) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;
		assert(lastValueFlag[idx]==UNSET);
	}
	
	public void reset() {
		int i = lastValueFlag.length;
		while (--i>=0) {
			lastValueFlag[i] = 0;
		}
	}

	
	
	public void flush() {
		writer.flush();
	}
	

	public void writeLongNull(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeNull();
		lastValueFlag[idx] = SET_NULL;
		
	}
	
	public void writeLongNullPMap(int token) {
			int idx = token & INSTANCE_MASK;
			writer.writePMapBit((byte)1);
			writer.writeNull();
			lastValueFlag[idx] = SET_NULL;
	}
	
	/*
	 * Method name convention to group the work 
	 *  write <FIELD_TYPE><OPERATOR>
	 *  
	 *  example FIELD_TYPES 
	 *  LongSigned
	 *  LongUnsigned
	 *  LongSingedOptional
	 *  LongUnsignedOptional
	 * 
	 */
	
	public void writeLongUnsigned(int value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeLongUnsigned(value);
	}
	
	public void writeLongUnsignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeLongUnsignedCopyOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeLongUnsignedConstant(int value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (lastValueFlag[idx]>0 && value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeLongUnsignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsignedOptional(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValueFlag[idx]<0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeLongUnsignedIncrement(int value, int token) {
		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
			lastValue[idx] = value;
		}
	}
	

	public void writeLongUnsignedIncrementOptional(int value, int token) {

		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;

		if (lastValueFlag[idx]>0 && value == incVal) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeLongUnsignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongUnsignedDeltaOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}

	////////////////
	///////////////
	////////////////
	
	public void writeLongSigned(int value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeLongSigned(value);
	}
	
	public void writeLongSignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeLongSignedCopyOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeLongSignedConstant(int value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (lastValueFlag[idx]>0 && value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeLongSignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSignedOptional(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValueFlag[idx]<0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeLongSignedIncrement(int value, int token) {
		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
			lastValue[idx] = value;
		}
	}
	

	public void writeLongSignedIncrementOptional(int value, int token) {

		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;

		if (lastValueFlag[idx]>0 && value == incVal) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeLongSignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongSignedDeltaOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}
	
}
