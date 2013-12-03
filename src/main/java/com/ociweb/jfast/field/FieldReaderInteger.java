package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.DictionaryFactory;

public class FieldReaderInteger {
	
	//crazy big value?
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveReader reader;
	
	private final int[]  lastValue;
	private final byte[] lastValueFlag;


	public FieldReaderInteger(PrimitiveReader reader, int[] values, byte[] flags) {
		this.reader = reader;
		this.lastValue = values;
		this.lastValueFlag = flags;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue,lastValueFlag);
	}

	public int readIntegerUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
	}

	public int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = reader.readIntegerUnsignedOptional();
		}
	}

	public int readIntegerUnsignedConstant(int token) {
		int idx = token & INSTANCE_MASK;
		return (reader.popPMapBit()==0 ? 
					lastValue[idx]:
					reader.readIntegerUnsigned()	
				);
	}

	public int readIntegerUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerUnsigned()));
	}

	public int readIntegerUnsignedCopyOptional(int token, int valueOfOptional) {
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
				return lastValue[idx] = reader.readIntegerUnsignedOptional();
			}
		}
	}
	
	
	public int readIntegerUnsignedDelta(int token) {
		
		int index = token & INSTANCE_MASK;
		return (lastValue[index] = (lastValue[index]+reader.readIntegerSigned()));
		
	}
	
	public int readIntegerUnsignedDeltaOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		if (reader.popPMapBit()==0) {
			return valueOfOptional;
		} else {
			int prevFlag = lastValueFlag[instance];
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = (int)((prevFlag <= 0) ? reader.readLongSigned() : lastValue[instance]+reader.readLongSigned());
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
				return reader.readIntegerUnsignedOptional();
			}
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
			return (lastValueFlag[instance] <= 0?valueOfOptional: ++lastValue[instance]);
		} else {
			if (reader.peekNull()) {
				lastValueFlag[instance] = SET_NULL;
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				lastValueFlag[instance] = SET_VALUE;
				//override value, but do not replace the default
				return lastValue[instance] =reader.readIntegerUnsignedOptional();
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
		if (reader.peekNull()) {
			reader.incPosition();
			lastValueFlag[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = reader.readIntegerSignedOptional();
		}
	}

	public int readIntegerSignedConstant(int token) {
		int idx = token & INSTANCE_MASK;
		return (reader.popPMapBit()==0 ? 
					lastValue[idx]:
					reader.readIntegerSigned()	
				);
	}

	public int readIntegerSignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readIntegerSigned()));
	}

	public int readIntegerSignedCopyOptional(int token, int valueOfOptional) {
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
				return lastValue[idx] = reader.readIntegerSignedOptional();
			}
		}
	}
	
	
	public int readIntegerSignedDelta(int token) {
		
		int index = token & INSTANCE_MASK;
		return (lastValue[index] = (lastValue[index]+reader.readIntegerSigned()));
		
	}
	
	public int readIntegerSignedDeltaOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		if (reader.popPMapBit()==0) {
			return valueOfOptional;
		} else {
			int prevFlag = lastValueFlag[instance];
			lastValueFlag[instance] = SET_VALUE;
			return lastValue[instance] = (int)((prevFlag <= 0) ? reader.readLongSigned() : lastValue[instance]+reader.readLongSigned());
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
			
			return lastValue[token & INSTANCE_MASK];
			
		} else {
			int value = reader.readIntegerSigned();
			if (value==0) {
				return valueOfOptional;
			} else {
				return --value;
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
			return (lastValueFlag[instance] <= 0?valueOfOptional: ++lastValue[instance]);
		} else {
			if (reader.peekNull()) {
				lastValueFlag[instance] = SET_NULL;
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				lastValueFlag[instance] = SET_VALUE;
				//override value, but do not replace the default
				return lastValue[instance] =reader.readIntegerSignedOptional();
			}
		}
	}
	
	
	
	
}
