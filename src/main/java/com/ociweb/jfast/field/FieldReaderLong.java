//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderLong {
	
	private final int INSTANCE_MASK;
	private final PrimitiveReader reader;
	final long[]  lastValue;

	//TODO: add advanced API for modification
	private long[] absentLongs = new long[]{0,1,-1,TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG};
	

	public FieldReaderLong(PrimitiveReader reader, long[] values) {
		
		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		
		this.reader = reader;
		this.lastValue = values;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(lastValue);
	}
	public void copy(int sourceToken, int targetToken) {
		lastValue[targetToken & INSTANCE_MASK] = lastValue[sourceToken & INSTANCE_MASK];
	}

	public long readLongUnsigned(int token) {
		//no need to set initValueFlags for field that can never be null
		return lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned();
	}

	public long readLongUnsignedOptional(int token) {
		long value = reader.readLongUnsigned();
		if (0==value) {
			return absentLongs[TokenBuilder.extractAbsent(token)];
		} else {
			return --value;
		}
	}

	public long readLongUnsignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public long readLongUnsignedConstantOptional(int token) {
		return (reader.popPMapBit()==0 ? absentLongs[TokenBuilder.extractAbsent(token)] : lastValue[token & INSTANCE_MASK]);
	}

	public long readLongSignedConstant(int token) {
		//always return this required value.
		return lastValue[token & INSTANCE_MASK];
	}
	
	public long readLongSignedConstantOptional(int token) {
		return (reader.popPMapBit()==0 ? absentLongs[TokenBuilder.extractAbsent(token)] : lastValue[token & INSTANCE_MASK]);
	}

	public long readLongUnsignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongUnsigned()));
	}

	public long readLongUnsignedCopyOptional(int token) {
		long value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readLongUnsigned();
		}
		return (0 == value ? absentLongs[TokenBuilder.extractAbsent(token)]: value-1);
	}
	
	
	public long readLongUnsignedDelta(int token) {
		//Delta opp never uses PMAP
		return lastValue[token & INSTANCE_MASK] += reader.readLongSigned();
	}
	
	public long readLongUnsignedDeltaOptional(int token) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK]=0;
			return absentLongs[TokenBuilder.extractAbsent(token)];
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

	public long readLongUnsignedDefaultOptional(int token) {
		if (reader.popPMapBit()==0) {
			
			long last = lastValue[token & INSTANCE_MASK];
			return 0==last?absentLongs[TokenBuilder.extractAbsent(token)]:last;
		} else {
			long value;
			if ((value = reader.readLongUnsigned())==0) {
				return absentLongs[TokenBuilder.extractAbsent(token)];
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


	public long readLongUnsignedIncrementOptional(int token) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? absentLongs[TokenBuilder.extractAbsent(token)]: ++lastValue[instance]);
		} else {
			long value = reader.readLongUnsigned();
			if (value==0) {
				lastValue[instance] = 0;
				return absentLongs[TokenBuilder.extractAbsent(token)];
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

	public long readLongSignedOptional(int token) {
		long value = reader.readLongSigned();
		lastValue[token & INSTANCE_MASK] = value;//needed for dynamic read behavior.
		if (0==value) {
			return absentLongs[TokenBuilder.extractAbsent(token)];
		} else {
			return value-1;
		}
	}

	public long readLongSignedCopy(int token) {
		return (reader.popPMapBit()==0 ? 
				 lastValue[token & INSTANCE_MASK] : 
			     (lastValue[token & INSTANCE_MASK] = reader.readLongSigned()));
	}

	public long readLongSignedCopyOptional(int token) {
		//if zero then use old value.
		long value;
		if (reader.popPMapBit()==0) {
			value = lastValue[token & INSTANCE_MASK];
		} else {
			lastValue[token & INSTANCE_MASK] = value = reader.readLongSigned();
		}
		return (0 == value ? absentLongs[TokenBuilder.extractAbsent(token)]: (value>0 ? value-1 : value));
	}
	
	
	public long readLongSignedDelta(int token) {
		//Delta opp never uses PMAP
		return lastValue[token & INSTANCE_MASK]+=reader.readLongSigned();
		
	}
	
	public long readLongSignedDeltaOptional(int token) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			lastValue[token & INSTANCE_MASK] = 0;
			return absentLongs[TokenBuilder.extractAbsent(token)];
		} else {
			return lastValue[token & INSTANCE_MASK] += 
					(value + ((value>>>63)-1) );
					//(value>0 ? value-1 : value);
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

	public long readLongSignedDefaultOptional(int token) {
		if (reader.popPMapBit()==0) {
			long last = lastValue[token & INSTANCE_MASK];
			return 0==last?absentLongs[TokenBuilder.extractAbsent(token)]:last;			
		} else {
			long value;
			if ((value = reader.readLongSigned())==0) {
				return absentLongs[TokenBuilder.extractAbsent(token)];
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


	public long readLongSignedIncrementOptional(int token) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (lastValue[instance] == 0 ? absentLongs[TokenBuilder.extractAbsent(token)]: ++lastValue[instance]);
		} else {
			long value;
			if ((lastValue[instance] = value = reader.readLongSigned())==0) {
				return absentLongs[TokenBuilder.extractAbsent(token)];
			} else {
				//lastValue[instance] = value;
				//return (value + (value>>>63))-1;
				return value>0 ? value-1 : value;
			}
		
		}
		
	}

	public void setReadFrom(int readFromIdx) {
		// TODO Auto-generated method stub
		
	}
	
	
}
