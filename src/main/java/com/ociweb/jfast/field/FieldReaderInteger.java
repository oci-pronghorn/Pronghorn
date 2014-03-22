//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

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

    /**
     * Computes the absent values as needed.
     * 00 ->  1
     * 01 ->  0
     * 10 -> -1
     * 11 -> TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT
     * 
     * 0
     * 1
     * 11111111111111111111111111111111
     * 1111111111111111111111111111111
     * 
     * @param b
     * @return
     */
    private static final int absentValue(int b) {
    	return ((1|(0-(b>>1)))>>>(1&b));  
    }

	public void reset(int idx) {
		lastValue[idx] = 0;
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

	public int readIntegerUnsignedOptional(int token) {
		int value = reader.readIntegerUnsigned();
		return value==0 ? absentValue(TokenBuilder.extractAbsent(token)) : value-1;
	}

	public int readIntegerUnsignedConstant(int token, int readFromIdx) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public int readIntegerUnsignedConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()==0 ? absentValue(TokenBuilder.extractAbsent(token)) : lastValue[token & INSTANCE_MASK]);
	}

	public int readIntegerSignedConstant(int token, int readFromIdx) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public int readIntegerSignedConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()==0 ? absentValue(TokenBuilder.extractAbsent(token)) : lastValue[token & INSTANCE_MASK]);
	}
	
	//TODO: refactor required to support complex dictionary configuration where different ID is used.
	//We need to read from one IDX and write to another, this is a required feature.
	
	
	public int readIntegerUnsignedCopy(int token, int readFromIdx) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[(readFromIdx>=0 ? readFromIdx : token) & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned()));
	}

	public int readIntegerUnsignedCopyOptional(int token, int readFromIdx) {
		int value;
		if (reader.popPMapBit()==0) {
			value = lastValue[(readFromIdx>=0 ? readFromIdx : token) & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readIntegerUnsigned();
		}
		return (0 == value ? absentValue(TokenBuilder.extractAbsent(token)): value-1);
	}
	
	
	public int readIntegerUnsignedDelta(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		return (lastValue[token & INSTANCE_MASK] += reader.readLongSigned());
		
	}
	
	public int readIntegerUnsignedDeltaOptional(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return absentValue(TokenBuilder.extractAbsent(token));
		} else {
			return lastValue[token & INSTANCE_MASK] += (value>0 ? value-1 : value);
			
		}
	}

	public int readIntegerUnsignedDefault(int token, int readFromIdx) {
		
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerUnsigned();
		}
	}

	public int readIntegerUnsignedDefaultOptional(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			
			int last = lastValue[(readFromIdx>=0 ? readFromIdx : token) & INSTANCE_MASK];
			return last == 0 ?
					//default value is null so return optional.
					absentValue(TokenBuilder.extractAbsent(token)) : //TODO: experiment with compute vs lookup.
//					absentInts[TokenBuilder.extractAbsent(token)] : 
						//default value 
					last;
	
		} else {
			int value = reader.readIntegerUnsigned();
			return value==0 ? absentValue(TokenBuilder.extractAbsent(token)) : value-1;
		}
	}

	public int readIntegerUnsignedIncrement(int token, int readFromIdx) {
		
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
		}
	}


	public int readIntegerUnsignedIncrementOptional(int token, int readFromIdx) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? absentValue(TokenBuilder.extractAbsent(token)): ++lastValue[instance]);
		} else {
			int value;
			if ((value = reader.readIntegerUnsigned())==0) {
				lastValue[instance] = 0;
				return absentValue(TokenBuilder.extractAbsent(token));
			} else {
				return (lastValue[instance] = value)-1;
			}
		}
	}

	//////////////
	//////////////
	//////////////
	
	public int readIntegerSigned(int token, int readFromIdx) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned();
	}

	public int readIntegerSignedOptional(int token, int readFromIdx) {
		int value = reader.readIntegerSigned();
		lastValue[token & INSTANCE_MASK] = value;//needed for dynamic read behavior.
		return value==0 ? absentValue(TokenBuilder.extractAbsent(token)) : value-1;
	}


	public int readIntegerSignedCopy(int token, int readFromIdx) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned()));
	}

	public int readIntegerSignedCopyOptional(int token, int readFromIdx) {
		//if zero then use old value.
		int value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readIntegerSigned();
		}
		return (0 == value ? absentValue(TokenBuilder.extractAbsent(token)): (value>0 ? value-1 : value));
	}
	
	
	public int readIntegerSignedDelta(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		return (lastValue[token & INSTANCE_MASK] += reader.readLongSigned());
	}
	
	public int readIntegerSignedDeltaOptional(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return absentValue(TokenBuilder.extractAbsent(token));
		} else {
			return lastValue[token & INSTANCE_MASK] += 
					//(value + ((value>>>63)-1) );
			        (value>0?value-1:value);
		}
	}

	public int readIntegerSignedDefault(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerSigned();
		}
	}

	public int readIntegerSignedDefaultOptional(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			int last = lastValue[token & INSTANCE_MASK];
			return 0==last?absentValue(TokenBuilder.extractAbsent(token)):last;			
		} else {
			int value;
			if ((value = reader.readIntegerSigned())==0) {
				return absentValue(TokenBuilder.extractAbsent(token));
			} else {
				return value>0 ? value-1 : value;
			}
		}
	}

	public int readIntegerSignedIncrement(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned();
		}
	}


	public int readIntegerSignedIncrementOptional(int token, int readFromIdx) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? absentValue(TokenBuilder.extractAbsent(token)): ++lastValue[instance]);
		} else {
			int value;
			if ((lastValue[instance] = value = reader.readIntegerSigned())==0) {
				return absentValue(TokenBuilder.extractAbsent(token));
			} else {
				//lastValue[instance] = value;
				//return (value + (value>>>31)) -1;
				return value>0 ? value-1 : value;
			}
		}
	}


}
