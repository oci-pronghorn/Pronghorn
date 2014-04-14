//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterLong {
	
	//for optional fields it is still in the optional format so 
	//zero represents null for those fields.  
	public final long[]  dictionary;
	public final long[]  init;
	public final int INSTANCE_MASK;
	final PrimitiveWriter writer;

	public FieldWriterLong(PrimitiveWriter writer, long[] values, long[] init) {
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.writer = writer;
		this.dictionary = values;
		this.init = init;
	}
	
	
	//TODO: B, Refactor all Field writes to split logic between dispatch and primitive
	
	
	
	public void writeLongUnsignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		writer.writeLongUnsignedCopy(value, idx, dictionary);
		
	}
	
	public void writeLongUnsignedCopyOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;

		writer.writeLongUnsignedCopyOptional(value, idx, dictionary);
		
	}
	

	public void writeLongUnsignedDefault(long value, int token) {
		int idx = token & INSTANCE_MASK;
		long constDefault = dictionary[idx];
		
		if (value == constDefault) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	
	public void writeLongUnsignedDefaultOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;
		long constDefault = dictionary[idx];
		
		//room for zero
		if (++value == constDefault) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
		
	public void writeLongUnsignedIncrement(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		long incVal = dictionary[idx]+1;
		if (value == incVal) {
			dictionary[idx] = incVal;
			writer.writePMapBit((byte)0);
		} else {
			dictionary[idx] = value;
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(value);
		}
	}
	

	public void writeLongUnsignedIncrementOptional(long value, int token) {

		int idx = token & INSTANCE_MASK;

		if (0!=dictionary[idx] && value == dictionary[idx]++) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongUnsigned(dictionary[idx] = 1+value);
		}
	}

	public void writeLongUnsignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		
		writer.writeLongSigned(value - dictionary[idx]);
		dictionary[idx] = value;		
	}
	
	public void writeLongUnsignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		
		long delta = value - dictionary[idx];
		dictionary[idx] = value;
		//System.err.println("long-delta-optional "+value);
		writer.writeLongSigned(delta>=0 ? 1+delta : delta);
	}

	////////////////
	///////////////
	////////////////
	
	public void writeLongSignedCopy(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value == dictionary[idx]) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(dictionary[idx] = value);
		}
	}
	
	public void writeLongSignedCopyOptional(long value, int token) {
		int idx = token & INSTANCE_MASK;

		if (value>=0) {
			value++;
		}
		
		if (value == dictionary[idx]) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(dictionary[idx] = value);
		}
	}
	
	public void writeLongSignedDefault(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		
		if (value == dictionary[idx]) {
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
		if (value == dictionary[idx]) {//matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	
	public void writeLongSignedDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (dictionary[idx]==0) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	public void writeLongSignedIncrement(long value, int token) {
		int idx = token & INSTANCE_MASK;
		
		dictionary[idx] = value;
		if (value == (dictionary[idx]+1)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
	}
	

	public void writeLongSignedIncrementOptional(long value, int token) {

		int idx = token & INSTANCE_MASK;

		
		if (value>=0) {
			value++;
		}
		long last = dictionary[idx];
		dictionary[idx] = value;
		if (0!=last && value == 1+last) {//not null and matches
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeLongSigned(value);
		}
			
	}
	
	public void writeLongSignedDelta(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		
		writer.writeLongSigned(value - dictionary[idx]);
		dictionary[idx] = value;		
	}
	
	public void writeLongSignedDeltaOptional(long value, int token) {
		//Delta opp never uses PMAP
		int idx = token & INSTANCE_MASK;
		
		long delta = value - dictionary[idx];
		writer.writeLongSigned(((delta+(delta>>>63))+1));
		//writer.writeLongSigned(delta>=0 ? 1+delta : delta);
		dictionary[idx] = value;	
	}

	public void writeNull(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//None and Delta (both do not use pmap)
				dictionary[idx] = 0;
				writer.writeNull();              //no pmap, yes change to last value
			} else {
				//Copy and Increment
				
				if (dictionary[idx]==0) { //stored value was null;
					writer.writePMapBit((byte)0);
				} else {
					dictionary[idx] =0;
					writer.writePMapBit((byte)1);
					writer.writeNull();
				}  //yes pmap, yes change to last value	
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
				if (dictionary[token & INSTANCE_MASK]==0) { //stored value was null;
					writer.writePMapBit((byte)0);
				} else {
					writer.writePMapBit((byte)1);
					writer.writeNull();
				}  //yes pmap,  no change to last value
			}	
		}
		
	}
	
	

	
	
}
