//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTReaderDispatch;

public class FieldReaderInteger {

	private final int INSTANCE_MASK;
	private final PrimitiveReader reader;	
	final int[]  lastValue;

	public FieldReaderInteger(PrimitiveReader reader, int[] values) {

		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.reader = reader;
		this.lastValue = values;
	}
	
	public static boolean isPowerOfTwo(int length) {
		
		while (0==(length&1)) {
			length = length>>1;
		}
		return length==1;
	}

	public void reset(DictionaryFactory df) {
		df.reset(lastValue);
	}
	public void copy(int sourceToken, int targetToken) {
		lastValue[targetToken & INSTANCE_MASK] = lastValue[sourceToken & INSTANCE_MASK];
	}

	public int readIntegerUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
	}

	public int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		int value = reader.readIntegerUnsigned();
		return value==0 ? valueOfOptional : value-1;
	}

	public int readIntegerUnsignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public int readIntegerUnsignedConstantOptional(int token, int valueOfOptional) {
		return (reader.popPMapBit()==0 ? valueOfOptional : lastValue[token & INSTANCE_MASK]);
	}

	public int readIntegerSignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public int readIntegerSignedConstantOptional(int token, int valueOfOptional) {
		return (reader.popPMapBit()==0 ? valueOfOptional : lastValue[token & INSTANCE_MASK]);
	}
	
	//TODO: refactor required to support complex dictionary configuration where different ID is used.
	//We need to read from one IDX and write to another, this is a required feature.
	
	
	public int readIntegerUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned()));
	}

	public int readIntegerUnsignedCopyOptional(int token, int valueOfOptional) {
		int value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readIntegerUnsigned();
		}
		return (0 == value ? valueOfOptional: value-1);
	}
	
	
	public int readIntegerUnsignedDelta(int token) {
		//Delta opp never uses PMAP
		return (lastValue[token & INSTANCE_MASK] += reader.readLongSigned());
		
	}
	
	public int readIntegerUnsignedDeltaOptional(int token, int valueOfOptional) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return valueOfOptional;
		} else {
			return lastValue[token & INSTANCE_MASK] += (value>0 ? value-1 : value);
			
		}
	}

	public int readIntegerUnsignedDefault(int token) {
		
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerUnsigned();
		}
	}

	public int readIntegerUnsignedDefaultOptional(int token, int valueOfOptional) {
		if (reader.popPMapBit()==0) {
			
			int last = lastValue[token & INSTANCE_MASK];
			return last == 0 ?
					//default value is null so return optional.
					valueOfOptional : 
					//default value 
					last;
	
		} else {
			int value = reader.readIntegerUnsigned();
			return value==0 ? valueOfOptional : value-1;
		}
	}

	public int readIntegerUnsignedIncrement(int token) {
		
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
		}
	}


	public int readIntegerUnsignedIncrementOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? valueOfOptional: ++lastValue[instance]);
		} else {
			int value;
			if ((value = reader.readIntegerUnsigned())==0) {
				lastValue[instance] = 0;
				return valueOfOptional;
			} else {
				return (lastValue[instance] = value)-1;
			}
		}
	}

	//////////////
	//////////////
	//////////////
	
	public int readIntegerSigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned();
	}

	public int readIntegerSignedOptional(int token, int valueOfOptional) {
		int value = reader.readIntegerSigned();
		lastValue[token & INSTANCE_MASK] = value;//needed for dynamic read behavior.
		return value==0 ? valueOfOptional : value-1;
	}


	public int readIntegerSignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned()));
	}

	public int readIntegerSignedCopyOptional(int token, int valueOfOptional) {
		//if zero then use old value.
		int value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readIntegerSigned();
		}
		return (0 == value ? valueOfOptional: (value>0 ? value-1 : value));
	}
	
	
	public int readIntegerSignedDelta(int token) {
		//Delta opp never uses PMAP
		return (lastValue[token & INSTANCE_MASK] += reader.readLongSigned());
	}
	
	public int readIntegerSignedDeltaOptional(int token, int valueOfOptional) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return valueOfOptional;
		} else {
			return lastValue[token & INSTANCE_MASK] += 
					(value + ((value>>>63)-1) );
					//(value>0?value-1:value);
		}
	}

	public int readIntegerSignedDefault(int token) {
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerSigned();
		}
	}

	public int readIntegerSignedDefaultOptional(int token, int valueOfOptional) {
		if (reader.popPMapBit()==0) {
			int last = lastValue[token & INSTANCE_MASK];
			return 0==last?valueOfOptional:last;			
		} else {
			int value;
			if ((value = reader.readIntegerSigned())==0) {
				return valueOfOptional;
			} else {
				return value>0 ? value-1 : value;
			}
		}
	}

	public int readIntegerSignedIncrement(int token) {
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned();
		}
	}


	public int readIntegerSignedIncrementOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? valueOfOptional: ++lastValue[instance]);
		} else {
			int value;
			if ((lastValue[instance] = value = reader.readIntegerSigned())==0) {
				return valueOfOptional;
			} else {
				//lastValue[instance] = value;
				//return (value + (value>>>31)) -1;
				return value>0 ? value-1 : value;
			}
		}
	}

	public void setReadFrom(int readFromIdx) {
		// TODO Auto-generated method stub
		
	}
}
