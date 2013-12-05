package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.DictionaryFactory;

public final class FieldWriterLong {

	//crazy big value? TODO: make smaller mask based on exact length of array.
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final PrimitiveWriter writer;
	
	//for optional fields it is still in the optional format so 
	//zero represents null for those fields.  
	private final long[]  lastValue;

	public FieldWriterLong(PrimitiveWriter writer, long[] values) {
		this.writer = writer;
		this.lastValue = values;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue);
	}	
	
	public void flush() {
		writer.flush();
	}
	

	public void writeLongNull(int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeNull();
		lastValue[idx] = 0;
	}
	
	public void writeLongNullPMap(int token, byte bit) {
		int idx = token & INSTANCE_MASK;
		writer.writePMapBit(bit);
		writer.writeNull();
		lastValue[idx] = 0;
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

		value++;//zero is held for null
		
		if (value == lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx] = value);
		}
	}
	

	
	public void writeLongUnsignedConstant(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
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

		value++;//room for zero
		if (value == lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(0);
			//writer.writeNull(); //TODO: confirm these are equal?
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

		value++;
		if (0!=lastValue[idx] && value == ++lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx] = value);
		}
	}
	
	public void writeLongUnsignedIncrementOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			//writer.writeNull(); //TODO: confirm these are equal?
			writer.writeLongUnsigned(0);
			lastValue[idx] = 0;
		}
	}
	

	public void writeLongUnsignedDelta(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongUnsignedDeltaOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writePMapBit((byte)1);	
		writer.writeLongSigned(1+(value - lastValue[idx]));
		lastValue[idx] = value;	
	}
	
	public void writeLongUnsignedDeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (lastValue[idx]==0) {
			writer.writePMapBit((byte)0);
		}else {
			writer.writePMapBit((byte)1);	
		    writer.writeLongSigned(0);
			lastValue[idx] = 0;	
		}
		
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

		if (value>=0) {
			value++;
		}
		
		if (value == lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(lastValue[idx] = value);
		}
	}
	

	
	public void writeLongSignedConstant(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		//value must equal constant
		if (value==lastValue[idx] ) {
			writer.writePMapBit((byte)0);//use constant value
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
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

		if (value>=0) {
			value++;//room for null
		}
		if (value == lastValue[idx]) {//matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
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

		if (value>=0) {
			value++;
		}
		if (0!=lastValue[idx] && value == ++lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(lastValue[idx] = value);
		}
			
	}
	
	public void writeLongSignedIncrementOptional(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
		//	System.err.println("A write zero");
			writer.writePMapBit((byte)0);
		} else {
		//	System.err.println("B write zero");
			writer.writePMapBit((byte)1);
			//writer.writeNull(); //TODO: confirm these are equal?
			writer.writeLongSigned(0);
			lastValue[idx] = 0;
		}
		
	}

	public void writeLongSignedDelta(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongSignedDeltaOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;
		writer.writePMapBit((byte)1);	
		writer.writeLongSigned(1+(value - lastValue[idx]));
		lastValue[idx] = value;	
	}
}
