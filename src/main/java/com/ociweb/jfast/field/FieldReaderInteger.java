//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderInteger {

	public final int INSTANCE_MASK;
	public final PrimitiveReader reader;	
	public final int[]  dictionary;
	public final int[]  init;
   
	public FieldReaderInteger(PrimitiveReader reader, int[] values, int[] init) {

		assert(values.length<TokenBuilder.MAX_INSTANCE);
		assert(isPowerOfTwo(values.length));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length-1));
		this.reader = reader;
		this.dictionary = values;
		this.init = init;
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
    public static final int absentValue32(int b) {
    	return ((1|(0-(b>>1)))>>>(1&b));  
    }

	public void reset(int idx) {
		dictionary[idx] = init[idx];
	}
	
	//TODO: inline
	public void reset(DictionaryFactory df) {
		df.reset(dictionary);
	}

	//TODO: inline
	public int readIntegerUnsigned(int token, int readFromIdx) {
		return readIntegerUnsigned(token);
	}
	
	//TODO: inline
	public int readIntegerUnsigned(int token) {
		int target = token & INSTANCE_MASK;
		
		return reader.readIntegerUnsigned(target, dictionary);
	}

	//TODO: inline
	public final int readIntegerUnsignedOptional(int token, int readFromIdx) {
		return readIntegerUnsignedOptional(token);
	}
	
	//TODO: inline
	public int readIntegerUnsignedOptional(int token) {
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));
		
		int value = reader.readIntegerUnsigned();
		return value==0 ? constAbsent : value-1;
	}

	//TODO: inline
	public final int readIntegerUnsignedConstant(int token, int readFromIdx) {
		//always return this required value.
		return dictionary[token & INSTANCE_MASK];
	}
	
	//TODO: inline
	public int readIntegerUnsignedConstantOptional(int token, int readFromIdx) {
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));
		int constConst = dictionary[token & INSTANCE_MASK];
		
		return reader.readIntegerUnsignedConstantOptional(constAbsent, constConst);
	}

	//TODO: inline
	public int readIntegerSignedConstant(int token, int readFromIdx) {
		//always return this required value.
		return dictionary[token & INSTANCE_MASK];
	}
	
	//TODO: inline
	public int readIntegerSignedConstantOptional(int token, int readFromIdx) {
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));
		int constConst = dictionary[token & INSTANCE_MASK];
		
		return reader.readIntegerSignedConstantOptional(constAbsent, constConst);
	}
	
	//TODO: refactor required to support complex dictionary configuration where different ID is used.
	//We need to read from one IDX and write to another, this is a required feature.
	//generated code will not need read from conditional or idx masks they are constants against array? may need.
	
	//TODO: inline
	public final int readIntegerUnsignedCopy(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
				
		//TODO: do each refactor like this . then inline each of these wrapping methods. ReadFrom/To will be implemented this way.
		return reader.readIntegerUnsignedCopy(target, source, dictionary);
	}
	

	//TODO: inline
	public final int readIntegerUnsignedCopyOptional(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		
		int value = reader.readIntegerUnsignedCopy(target, source, dictionary);
		
		return (0 == value ? absentValue32(TokenBuilder.extractAbsent(token)): value-1);
	}
		
	//TODO: inline
	public int readIntegerUnsignedDelta(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		
		return reader.readIntegerUnsignedDelta(target, source, dictionary);
	}
	
	//TODO: inline
	public int readIntegerUnsignedDeltaOptional(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));//TODO: runtime constant		
		
		return reader.readIntegerUnsignedDeltaOptional(target, source, dictionary, constAbsent);
	}

	//TODO: inline
	public int readIntegerUnsignedDefault(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		int constDefault = dictionary[source];//TODO: runtime constant to be injected by code generator.
		
		return reader.readIntegerUnsignedDefault(constDefault);

	}

	//TODO: inline
	public final int readIntegerUnsignedDefaultOptional(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));//TODO: runtime constant.
		int t = dictionary[source];//TODO: runtime constant
		int constDefault = t == 0 ? constAbsent : t-1; //TODO: runtime constant;
		
		return reader.readIntegerUnsignedDefaultOptional(constDefault, constAbsent);

	}

	//TODO: inline
	public int readIntegerUnsignedIncrement(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		
		return reader.readIntegerUnsignedIncrement(target, source, dictionary);

	}

	//TODO: inline
	public int readIntegerUnsignedIncrementOptional(int token, int readFromIdx) {
		int target = token & INSTANCE_MASK;
		int source = readFromIdx>=0 ? readFromIdx&INSTANCE_MASK : target;
		int constAbsent = absentValue32(TokenBuilder.extractAbsent(token));
		
		return reader.readIntegerUnsignedIncrementOptional(target, source, dictionary, constAbsent);

	}

	//////////////
	//////////////
	//////////////
	
	public int readIntegerSigned(int token, int readFromIdx) {
		//no need to set initValueFlags for field that can never be null
		return dictionary[token & INSTANCE_MASK] = reader.readIntegerSigned();
	}

	public int readIntegerSignedOptional(int token, int readFromIdx) {
		int value = reader.readIntegerSigned();
		dictionary[token & INSTANCE_MASK] = value;//needed for dynamic read behavior.
		return value==0 ? absentValue32(TokenBuilder.extractAbsent(token)) : value-1;
	}


	public int readIntegerSignedCopy(int token, int readFromIdx) {
		return (reader.popPMapBit()==0 ? 
				 dictionary[token & INSTANCE_MASK] : 
			     (dictionary[token & INSTANCE_MASK] = reader.readIntegerSigned()));
	}

	public int readIntegerSignedCopyOptional(int token, int readFromIdx) {
		//if zero then use old value.
		int value;
		if (reader.popPMapBit()==0) {
			value = dictionary[token & INSTANCE_MASK];
		} else {
			dictionary[token & INSTANCE_MASK] = value = reader.readIntegerSigned();
		}
		return (0 == value ? absentValue32(TokenBuilder.extractAbsent(token)): (value>0 ? value-1 : value));
	}
	
	
	public int readIntegerSignedDelta(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		return (dictionary[token & INSTANCE_MASK] += reader.readLongSigned());
	}
	
	public final int readIntegerSignedDeltaOptional(int token, int readFromIdx) {
		//Delta opp never uses PMAP
		long value = reader.readLongSigned();
		if (0==value) {
			dictionary[token & INSTANCE_MASK]=0;
			return absentValue32(TokenBuilder.extractAbsent(token));
		} else {
			return dictionary[token & INSTANCE_MASK] += 
					//(value + ((value>>>63)-1) );
			        (value>0?value-1:value);
		}
	}

	public int readIntegerSignedDefault(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			//default value 
			return dictionary[token & INSTANCE_MASK];
		} else {
			//override value, but do not replace the default
			return reader.readIntegerSigned();
		}
	}

	public int readIntegerSignedDefaultOptional(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			int last = dictionary[token & INSTANCE_MASK];
			return 0==last?absentValue32(TokenBuilder.extractAbsent(token)):last;			
		} else {
			int value;
			if ((value = reader.readIntegerSigned())==0) {
				return absentValue32(TokenBuilder.extractAbsent(token));
			} else {
				return value>0 ? value-1 : value;
			}
		}
	}

	public int readIntegerSignedIncrement(int token, int readFromIdx) {
		if (reader.popPMapBit()==0) {
			//increment old value
			return ++dictionary[token & INSTANCE_MASK];
		} else {
			//assign and return new value
			return dictionary[token & INSTANCE_MASK] = reader.readIntegerSigned();
		}
	}


	public int readIntegerSignedIncrementOptional(int token, int readFromIdx) {
		int instance = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return (dictionary[instance] == 0 ? absentValue32(TokenBuilder.extractAbsent(token)): ++dictionary[instance]);
		} else {
			int value;
			if ((dictionary[instance] = value = reader.readIntegerSigned())==0) {
				return absentValue32(TokenBuilder.extractAbsent(token));
			} else {
				//lastValue[instance] = value;
				//return (value + (value>>>31)) -1;
				return value>0 ? value-1 : value;
			}
		}
	}


}
