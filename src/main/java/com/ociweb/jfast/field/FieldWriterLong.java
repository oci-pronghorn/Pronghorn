package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.DictionaryFactory;

public final class FieldWriterLong {
	
	//for optional fields it is still in the optional format so 
	//zero represents null for those fields.  
	private final long[]  lastValue;
	private final int INSTANCE_MASK;
	private final PrimitiveWriter writer;

	public FieldWriterLong(PrimitiveWriter writer, long[] values) {
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = (values.length-1);
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

		if (0!=lastValue[idx] && value == lastValue[idx]++) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(lastValue[idx] = 1+value);
		}
	}

	public void writeLongUnsignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongUnsignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		long delta = value - lastValue[idx];
		writer.writeLongSigned(delta>=0 ? 1+delta : delta);
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
	
	public void writeLongSignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		writer.writeLongSigned(value - lastValue[idx]);
		lastValue[idx] = value;		
	}
	
	public void writeLongSignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		long delta = value - lastValue[idx];
		writer.writeLongSigned(delta>=0 ? 1+delta : delta);
		lastValue[idx] = value;	
	}

	public void writeNull(int token) {
		
		if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//None and Delta (both do not use pmap)
				writeClearNull(token);              //no pmap, yes change to last value
			} else {
				//Copy and Increment
				writePMapAndClearNull(token);  //yes pmap, yes change to last value	
			}
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
					//const
					writer.writeNull();                 //no pmap,  no change to last value  
				} else {
					//const optional
					writer.writePMapBit((byte)0);       //pmap only
				}			
			} else {	
				//default
				writePMapNull(token);  //yes pmap,  no change to last value
			}	
		}
		
	}
	
	private void writeClearNull(int token) {
		writer.writeNull();
		lastValue[token & INSTANCE_MASK] = 0;
	}
	
	
	private void writePMapAndClearNull(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
			lastValue[idx] =0;
		}
	}
	
	
	private void writePMapNull(int token) {
		if (lastValue[token & INSTANCE_MASK]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
}
