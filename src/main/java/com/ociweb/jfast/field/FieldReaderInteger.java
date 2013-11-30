package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderInteger {
	
	//crazy big value?
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	private final static byte UNSET     = 0;  //use == 0 to detect (default value)
	private final static byte SET_NULL  = -1; //use < 0 to detect
	private final static byte SET_VALUE = 1;  //use > 0 to detect
	
	private final PrimitiveReader reader;
	
	private final int[]  intValues;
	private final byte[] intValueFlags;


	public FieldReaderInteger(PrimitiveReader reader, int fields) {
		this.reader = reader;
		this.intValues = new int[fields];
		this.intValueFlags = new byte[fields];
	}
	
	public void reset() {
		int i = intValueFlags.length;
		while (--i>=0) {
			intValueFlags[i] = UNSET;
		}
	}

	public int readIntegerUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return intValues[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
	}

	public int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			intValueFlags[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			intValueFlags[instance] = SET_VALUE;
			return intValues[instance] = reader.readIntegerUnsignedOptional();
		}
	}
	
	public int readIntegerSigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return intValues[token & INSTANCE_MASK] = reader.readIntegerSigned();
	}

	public int readIntegerSignedOptional(int token, int valueOfOptional) {
		if (reader.peekNull()) {
			reader.incPosition();
			intValueFlags[token & INSTANCE_MASK] = SET_NULL;
			return valueOfOptional;
		} else {
			int instance = token & INSTANCE_MASK;
			intValueFlags[instance] = SET_VALUE;
			return intValues[instance] = reader.readIntegerSignedOptional();
		}
	}

	public int readIntegerUnsignedConstant(int token, int valueOfOptional) {
		int idx = token & INSTANCE_MASK;
		return (reader.popPMapBit()==0 ? 
					(intValueFlags[idx]<=0?valueOfOptional:intValues[idx]):
					reader.readIntegerUnsigned()	
				);
	}

	public int readIntegerUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 intValues[token & INSTANCE_MASK] : 
			     (intValues[token & INSTANCE_MASK] = reader.readIntegerUnsigned()));
	}

	public int readIntegerUnsignedCopyOptional(int token, int valueOfOptional) {
		//if zero then use old value.
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()==0) {
			return (intValueFlags[idx] < 0 ? valueOfOptional: intValues[idx]);
		} else {
			if (reader.peekNull()) {
				reader.incPosition();
				intValueFlags[idx] = SET_NULL;
				return valueOfOptional;
			} else {
				intValueFlags[idx] = SET_VALUE;
				return intValues[idx] = reader.readIntegerUnsignedOptional();
			}
		}
	}
	
	
	public int readIntegerUnsignedDelta(int token) {
		
		int index = token & INSTANCE_MASK;
		return (intValues[index] = (intValues[index]+reader.readIntegerSigned()));
		
	}
	
	public int readIntegerUnsignedDeltaOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		if (reader.peekNull()) {
			reader.incPosition();
			intValueFlags[instance] = SET_NULL;
			return valueOfOptional;
		} else {
			int prevFlag = intValueFlags[instance];
			intValueFlags[instance] = SET_VALUE;
			return intValues[instance] = (int)((prevFlag <= 0) ? reader.readLongSignedOptional() : intValues[instance]+reader.readLongSignedOptional());
		}
	}

	public int readIntegerUnsignedDefault(int token) {
		if (reader.popPMapBit()==0) {
			//default value 
			return intValues[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerUnsigned();
		}
	}

	public int readIntegerUnsignedDefaultOptional(int token, int valueOfOptional) {
		if (reader.popPMapBit()==0) {
			
			if (intValueFlags[token & INSTANCE_MASK] < 0) {
				//default value is null so return optional.
				return valueOfOptional;
			} else {
				//default value 
				return intValues[token & INSTANCE_MASK];
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
			return ++intValues[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return intValues[token & INSTANCE_MASK] = reader.readIntegerUnsigned();
		}
	}


	public int readIntegerUnsignedIncrementOptional(int token, int valueOfOptional) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (intValueFlags[instance] <= 0?valueOfOptional: ++intValues[instance]);
		} else {
			if (reader.peekNull()) {
				intValueFlags[instance] = SET_NULL;
				reader.incPosition();
				return valueOfOptional;
				
			} else {
				intValueFlags[instance] = SET_VALUE;
				//override value, but do not replace the default
				return intValues[instance] =reader.readIntegerUnsignedOptional();
			}
		}
	}

	
	
}
