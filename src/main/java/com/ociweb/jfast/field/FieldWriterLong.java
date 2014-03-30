//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterLong {
	
	//for optional fields it is still in the optional format so 
	//zero represents null for those fields.  
	final long[]  lastValue;
	final long[]  init;
	private final int INSTANCE_MASK;
	private final PrimitiveWriter writer;

	public FieldWriterLong(PrimitiveWriter writer, long[] values, long[] init) {
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.writer = writer;
		this.lastValue = values;
		this.init = init;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue);
	}	
	public void copy(int sourceToken, int targetToken) {
		lastValue[targetToken & INSTANCE_MASK] = lastValue[sourceToken & INSTANCE_MASK];
	}
	
	public void flush() {
		writer.flush();
	}
	

	public void writeLongNull(int token) {
		lastValue[token & INSTANCE_MASK] = 0;
		writer.writeNull();
	}
	
	public void writeLongNullPMap(int token, byte bit) {
		lastValue[token & INSTANCE_MASK] = 0;
		writer.writePMapBit(bit);
		writer.writeNull();
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
		lastValue[idx] = value;
		writer.writeLongUnsigned(value);
	}
	
	public void writeLongUnsignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			lastValue[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedCopyOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;

		value++;//zero is held for null
		
		if (value == lastValue[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			lastValue[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	

	public void writeLongUnsignedConstant(long value, int token) {
		assert(lastValue[ token & INSTANCE_MASK]==value) : "Only the constant value from the template may be sent";
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}
	
	public void writeLongUnsignedConstantOptional(long value, int token) {
		assert(lastValue[ token & INSTANCE_MASK]==value) : "Only the constant value from the template may be sent";
		writer.writePMapBit((byte)1);
		//the writeNull will take care of the rest.
	}
	
	
	public void writeLongSignedConstant(long value, int token) {
		assert(lastValue[ token & INSTANCE_MASK]==value) : "Only the constant value from the template may be sent";
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}
	
	public void writeLongSignedConstantOptional(long value, int token) {
		assert(lastValue[ token & INSTANCE_MASK]==value) : "Only the constant value from the template may be sent";
		writer.writePMapBit((byte)1);
		//the writeNull will take care of the rest.
	}
	
	public void writeLongUnsignedDefault(long value, int token) {
		if (value == lastValue[token & INSTANCE_MASK]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(long value, int token) {
		//room for zero
		if (++value == lastValue[token & INSTANCE_MASK]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
		
	public void writeLongUnsignedIncrement(long value, int token) {
		int idx;
		long incVal = lastValue[idx = token & INSTANCE_MASK]+1;
		
		if (value == incVal) {
			lastValue[idx] = incVal;
			writer.writePMapBit((byte)0);
		} else {
			lastValue[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	

	public void writeLongUnsignedIncrementOptional(long value, int token) {

		int idx = token & INSTANCE_MASK;

		if (0!=lastValue[idx] && value == lastValue[idx]++) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			long tmp = lastValue[idx] = 1+value;
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(tmp);
		}
	}

	public void writeLongUnsignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		long tmp = value - lastValue[idx];
		lastValue[idx] = value;		
		//System.err.println("*** long-delta "+value);
		writer.writeLongSigned(tmp);
	}
	
	public void writeLongUnsignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		long delta = value - lastValue[idx];
		lastValue[idx] = value;
		//System.err.println("long-delta-optional "+value);
		writer.writeLongSigned(delta>=0 ? 1+delta : delta);
	}

	////////////////
	///////////////
	////////////////
	
	public void writeLongSigned(long value, int token) {
		int idx = token & INSTANCE_MASK;
		lastValue[idx] = value;
		writer.writeLongSigned(value);
	}
	
	public void writeLongSignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == lastValue[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			lastValue[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
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
			lastValue[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefault(long value, int token) {
		if (value == lastValue[token & INSTANCE_MASK]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(long value, int token) {
		if (value>=0) {
			value++;//room for null
		}
		if (value == lastValue[token & INSTANCE_MASK]) {//matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(int token) {
		if (lastValue[token & INSTANCE_MASK]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeLongSignedIncrement(long value, int token) {
		int idx;
		
		lastValue[idx = token & INSTANCE_MASK] = value;
		if (value == (lastValue[idx]+1)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	

	public void writeLongSignedIncrementOptional(long value, int token) {

		int idx;

		if (value>=0) {
			value++;
		}
		long last = lastValue[idx = token & INSTANCE_MASK];
		lastValue[idx] = value;
		if (0!=last && value == 1+last) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
			
	}
	
	public void writeLongSignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx;
		long tmp = value - lastValue[idx = token & INSTANCE_MASK];
		lastValue[idx] = value;		
		writer.writeLongSigned(tmp);
	}
	
	public void writeLongSignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx;
		long delta = value - lastValue[idx = token & INSTANCE_MASK];
		lastValue[idx] = value;	
		writer.writeLongSigned(((delta+(delta>>>63))+1));
		//writer.writeLongSigned(delta>=0 ? 1+delta : delta);
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
		lastValue[token & INSTANCE_MASK] = 0;
		writer.writeNull();
	}
	
	
	private void writePMapAndClearNull(int token) {
		int idx = token & INSTANCE_MASK;

		if (lastValue[idx]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			lastValue[idx] =0;
			writer.writePMapBit((byte)1);
			writer.writeNull();
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

	public void writeLongUnsignedOptional(long value, int token) {
		writer.writeLongUnsigned(value+1);
	}

	public void writeLongSignedOptional(long value, int token) {
		writer.writeLongSignedOptional(value);
	}

	public void reset(int idx) {
		lastValue[idx] = init[idx];
	}
}
