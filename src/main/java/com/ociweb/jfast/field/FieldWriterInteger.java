package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.DictionaryFactory;

public final class FieldWriterInteger {

	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (and/or default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveWriter writer;
	
	private final int[]  lastValue;
	private final byte[] lastValueFlag;


	public FieldWriterInteger(PrimitiveWriter writer, int[] values, byte[] flags) {
		this.writer = writer;
		this.lastValue = values;
		this.lastValueFlag = flags;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue,lastValueFlag);
	}	
	
	public void flush() {
		writer.flush();
	}
	

	public void writeIntegerNull(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeNull();
		lastValueFlag[idx] = SET_NULL;
		
	}
	
	public void writeIntegerNullPMap(int token) {
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
	 *  IntegerSigned
	 *  IntegerUnsigned
	 *  IntegerSingedOptional
	 *  IntegerUnsignedOptional
	 * 
	 */
	
	public void writeIntegerUnsigned(int value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeIntegerUnsigned(value);
	}
	
	public void writeIntegerUnsignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeIntegerUnsignedCopyOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeIntegerUnsignedConstant(int value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (lastValueFlag[idx]>0 && value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeIntegerUnsignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value);
		}
	}
	
	public void writeIntegerUnsignedDefaultOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsignedOptional(value);
		}
	}
	
	public void writeIntegerUnsignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValueFlag[idx]<0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeIntegerUnsignedIncrement(int value, int token) {
		int idx = token & INSTANCE_MASK;
		int incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(lastValue[idx] = value);
		}
	}
	

	public void writeIntegerUnsignedIncrementOptional(int value, int token) {

		int idx = token & INSTANCE_MASK;
		int incVal = lastValue[idx]+1;

		if (value == incVal && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeIntegerUnsignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeIntegerSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeIntegerUnsignedDeltaOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}

	////////////////
	///////////////
	////////////////
	
	public void writeIntegerSigned(int value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeIntegerSigned(value);
	}
	
	public void writeIntegerSignedCopy(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeIntegerSignedCopyOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeIntegerSignedConstant(int value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (lastValueFlag[idx]>0 && value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeIntegerSignedDefault(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSigned(value);
		}
	}
	
	public void writeIntegerSignedDefaultOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSignedOptional(value);
		}
	}
	
	public void writeIntegerSignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValueFlag[idx]<0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeIntegerSignedIncrement(int value, int token) {
		int idx = token & INSTANCE_MASK;
		int incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSigned(lastValue[idx] = value);
		}
	}
	

	public void writeIntegerSignedIncrementOptional(int value, int token) {

		int idx = token & INSTANCE_MASK;
		int incVal = lastValue[idx]+1;

		if (value == incVal && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeIntegerSignedDelta(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeIntegerSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeIntegerSignedDeltaOptional(int value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}
	
}
