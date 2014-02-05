//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderLong {
	
	private final int INSTANCE_MASK;
	private final PrimitiveReader reader;
	private final long[]  lastValue;


	public FieldReaderLong(PrimitiveReader reader, long[] values) {
		
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = (values.length-1);
		
		this.reader = reader;
		this.lastValue = values;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue);
	}

	public long readLongUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned();
	}

	public long readLongUnsignedOptional(int token, long valueOfOptional) {
		long value = reader.readLongUnsigned();
		if (0==value) {
			return valueOfOptional;
		} else {
			return --value;
		}
	}

	public long readLongUnsignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public long readLongUnsignedConstantOptional(int token, long valueOfOptional) {
		return (reader.popPMapBit()==0 ? valueOfOptional : lastValue[token & INSTANCE_MASK]);
	}

	public long readLongSignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public long readLongSignedConstantOptional(int token, long valueOfOptional) {
		return (reader.popPMapBit()==0 ? valueOfOptional : lastValue[token & INSTANCE_MASK]);
	}

	public long readLongUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned()));
	}

	public long readLongUnsignedCopyOptional(int token, long valueOfOptional) {
		long value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readLongUnsigned();
		}
		return (0 == value ? valueOfOptional: value-1);
	}
	
	
	public long readLongUnsignedDelta(int token) {
		//Delta opp never uses PMAP
		return lastValue[token & INSTANCE_MASK] += reader.readLongSigned();
	}
	
	public long readLongUnsignedDeltaOptional(int token, long valueOfOptional) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return valueOfOptional;
		} else {
			return lastValue[token & INSTANCE_MASK] += (value-1);
			
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
			
			int idx;
			if (lastValue[idx = token & INSTANCE_MASK] == 0) {
				//default value is null so return optional.
				return valueOfOptional;
			} else {
				//default value 
				return lastValue[idx];
			}
			
		} else {
			long value;
			if ((value = reader.readLongUnsigned())==0) {
				return valueOfOptional;
			} else {
				return value-1;
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
			return (lastValue[instance] == 0 ? valueOfOptional: ++lastValue[instance]);
		} else {
			long value = reader.readLongUnsigned();
			if (value==0) {
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
	
	public long readLongSigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readLongSigned();
	}

	public long readLongSignedOptional(int token, long valueOfOptional) {
		long value = reader.readLongSigned();
		lastValue[token & INSTANCE_MASK] = value;//needed for dynamic read behavior.
		if (0==value) {
			return valueOfOptional;
		} else {
			return value-1;
		}
	}

	public long readLongSignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongSigned()));
	}

	public long readLongSignedCopyOptional(int token, long valueOfOptional) {
		//if zero then use old value.
		long value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readLongSigned();
		}
		return (0 == value ? valueOfOptional: (value>0 ? value-1 : value));
	}
	
	
	public long readLongSignedDelta(int token) {
		//Delta opp never uses PMAP
		return lastValue[token & INSTANCE_MASK]+=reader.readLongSigned();
		
	}
	
	public long readLongSignedDeltaOptional(int token, long valueOfOptional) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK] = 0;
			return valueOfOptional;
		} else {
			return lastValue[token & INSTANCE_MASK] += (value>0 ? value-1 : value);
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
			int idx;
			if (lastValue[idx = token & INSTANCE_MASK] == 0) {
				//default value is null so return optional.
				return valueOfOptional;
			} else {
				//default value 
				return lastValue[idx];
			}
			
		} else {
			long value;
			if ((value = reader.readLongSigned())==0) {
				return valueOfOptional;
			} else {
				return value>0? value-1 : value;
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
			return (lastValue[instance] == 0 ? valueOfOptional: ++lastValue[instance]);
		} else {
			long value = reader.readLongSigned();
			if (value==0) {
				lastValue[instance] = 0;
				return valueOfOptional;
			} else {
				return value>0 ? (lastValue[instance] = value)-1 : (lastValue[instance] = value);
			}
		}
		
	}
	
	
}
