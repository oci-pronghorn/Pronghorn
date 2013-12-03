package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.DictionaryFactory;

public final class FieldWriterLong {

	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveWriter writer;
	
	private final long[] lastValue;
	private final byte[] lastValueFlag;


	public FieldWriterLong(PrimitiveWriter writer, long[] values, byte[] flags) {
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
	

	public void writeLongNull(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeNull();
		lastValueFlag[idx] = SET_NULL;
		
	}
	
	public void writeLongNullPMap(int token, byte bit) {
			int idx = token & INSTANCE_MASK;
			writer.writePMapBit(bit);
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
	
	public void writeLongUnsigned(long value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeLongUnsigned(value);
	}
	
	public void writeLongUnsignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeLongUnsignedCopyOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeLongUnsignedConstant(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (value==lastValue[idx] && lastValueFlag[idx]>0) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeLongUnsignedDefault(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(long value, int token) {
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
	
	public void writeLongUnsignedIncrement(long value, int token) {
		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx] = value);
		}
	}
	

	public void writeLongUnsignedIncrementOptional(long value, int token) {

		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;

		if (value == incVal && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeLongUnsignedDelta(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongUnsignedDeltaOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}

	////////////////
	///////////////
	////////////////
	
	public void writeLongSigned(long value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;//TODO: not sure if this feature will be needed.
		writer.writeLongSigned(value);
	}
	
	public void writeLongSignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
			lastValue[idx] = value;
		}
	}
	
	public void writeLongSignedCopyOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx] && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}
	

	
	public void writeLongSignedConstant(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (lastValueFlag[idx]>0 && value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(lastValue[idx]=value);
		}	
		
	}
	
	public void writeLongSignedDefault(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(long value, int token) {
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
	
	public void writeLongSignedIncrement(long value, int token) {
		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;
		
		if (value == incVal) {
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(lastValue[idx] = value);
		}
	}
	

	public void writeLongSignedIncrementOptional(long value, int token) {

		int idx = token & INSTANCE_MASK;
		long incVal = lastValue[idx]+1;

		if (value == incVal && lastValueFlag[idx]>0) {//not null and matches
			writer.writePMapBit((byte)0);
			lastValue[idx] = incVal;
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSignedOptional(lastValue[idx] = value);
			lastValueFlag[idx] = SET_VALUE;
		}
	}

	public void writeLongSignedDelta(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongSignedDeltaOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSignedOptional(value - lastValue[idx]);
		lastValueFlag[idx] = SET_VALUE;
		lastValue[idx] = value;	
	}
	
}
