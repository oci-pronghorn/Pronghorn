package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.DictionaryFactory;

public class FieldReaderLong {
	
	//crazy big value?
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveReader reader;
	
	private final long[]  lastValue;
	private final byte[] lastValueFlag;


	public FieldReaderLong(PrimitiveReader reader, long[] values, byte[] flags) {
		this.reader = reader;
		this.lastValue = values;
		this.lastValueFlag = flags;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue,lastValueFlag);
	}

	public long readLongUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned();
	}

	public long readLongUnsignedOptional(int token, long valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = reader.readLongUnsignedOptional();
		}
	}

	public long readLongUnsignedConstant(int token, long valueOfOptional) {
		int idx = token & INSTANCE_MASK;
		return (reader.popPMapBit()==0 ? 
					(lastValueFlag[idx]<=0?valueOfOptional:lastValue[idx]):
					reader.readLongUnsigned()	
				);
	}

	public long readLongUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned()));
	}

	public long readLongUnsignedCopyOptional(int token, long valueOfOptional) {
		//if zero then use old value.
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()==0) {
			return (lastValueFlag[idx] < 0 ? valueOfOptional: lastValue[idx]);
		} else {
			if (reader.peekNull()) {
				reader.incPosition();
				lastValueFlag[idx] = SET_NULL;
				return valueOfOptional;
			} else {
				lastValueFlag[idx] = SET_VALUE;
				return lastValue[idx] = reader.readLongUnsignedOptional();
			}
		}
	}
	
	
	public long readLongUnsignedDelta(int token) {
		
		int index = token & INSTANCE_MASK;
		return (lastValue[index] = (lastValue[index]+reader.readLongSigned()));
		
	}
	
	public long readLongUnsignedDeltaOptional(int token, long valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[instance] = SET_NULL;
			return valueOfOptional;
		} else {
			int prevFlag = lastValueFlag[instance];
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = ((prevFlag <= 0) ? reader.readLongSignedOptional() : lastValue[instance]+reader.readLongSignedOptional());
		}
	}

	public long readLongUnsignedDefault(int token) {
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readLongUnsigned();
		}
	}

	public long readLongUnsignedDefaultOptional(int token, long valueOfOptional) {
		if (reader.popPMapBit()==0) {
			
			if (lastValueFlag[token & INSTANCE_MASK] < 0) {
				//default value is null so return optional.
				return valueOfOptional;
			} else {
				//default value 
				return lastValue[token & INSTANCE_MASK];
			}
			
		} else {
			if (reader.peekNull()) {
				
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				//override value, but do not replace the default
				return reader.readLongUnsignedOptional();
			}
		}
	}

	public long readLongUnsignedIncrement(int token) {
		
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned();
		}
	}


	public long readLongUnsignedIncrementOptional(int token, long valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValueFlag[instance] <= 0?valueOfOptional: ++lastValue[instance]);
		} else {
			if (reader.peekNull()) {
				lastValueFlag[instance] = SET_NULL;
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				lastValueFlag[instance] = SET_VALUE;
				//override value, but do not replace the default
				return lastValue[instance] =reader.readLongUnsignedOptional();
			}
		}
	}

	//////////////
	//////////////
	//////////////
	
	public long readLongSigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readLongSigned();
	}

	public long readLongSignedOptional(int token, long valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = reader.readLongSignedOptional();
		}
	}

	public long readLongSignedConstant(int token, long valueOfOptional) {
		int idx = token & INSTANCE_MASK;
		return (reader.popPMapBit()==0 ? 
					(lastValueFlag[idx]<=0?valueOfOptional:lastValue[idx]):
					reader.readLongSigned()	
				);
	}

	public long readLongSignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongSigned()));
	}

	public long readLongSignedCopyOptional(int token, long valueOfOptional) {
		//if zero then use old value.
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()==0) {
			return (lastValueFlag[idx] < 0 ? valueOfOptional: lastValue[idx]);
		} else {
			if (reader.peekNull()) {
				reader.incPosition();
				lastValueFlag[idx] = SET_NULL;
				return valueOfOptional;
			} else {
				lastValueFlag[idx] = SET_VALUE;
				return lastValue[idx] = reader.readLongSignedOptional();
			}
		}
	}
	
	
	public long readLongSignedDelta(int token) {
		
		int index = token & INSTANCE_MASK;
		return (lastValue[index] = (lastValue[index]+reader.readLongSigned()));
		
	}
	
	public long readLongSignedDeltaOptional(int token, long valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[instance] = SET_NULL;
			return valueOfOptional;
		} else {
			int prevFlag = lastValueFlag[instance];
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = ((prevFlag <= 0) ? reader.readLongSignedOptional() : lastValue[instance]+reader.readLongSignedOptional());
		}
	}

	public long readLongSignedDefault(int token) {
		if (reader.popPMapBit()==0) {
			//default value 
			return lastValue[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readLongSigned();
		}
	}

	public long readLongSignedDefaultOptional(int token, long valueOfOptional) {
		if (reader.popPMapBit()==0) {
			
			if (lastValueFlag[token & INSTANCE_MASK] < 0) {
				//default value is null so return optional.
				return valueOfOptional;
			} else {
				//default value 
				return lastValue[token & INSTANCE_MASK];
			}
			
		} else {
			if (reader.peekNull()) {
				
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				//override value, but do not replace the default
				return reader.readLongSignedOptional();
			}
		}
	}

	public long readLongSignedIncrement(int token) {
		
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++lastValue[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return lastValue[token & INSTANCE_MASK] = reader.readLongSigned();
		}
	}


	public long readLongSignedIncrementOptional(int token, long valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValueFlag[instance] <= 0?valueOfOptional: ++lastValue[instance]);
		} else {
			if (reader.peekNull()) {
				lastValueFlag[instance] = SET_NULL;
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				lastValueFlag[instance] = SET_VALUE;
				//override value, but do not replace the default
				return lastValue[instance] =reader.readLongSignedOptional();
			}
		}
	}
	
	
	
	
}
