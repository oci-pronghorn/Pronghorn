//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterChar;
import com.ociweb.jfast.field.FieldWriterDecimal;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.FieldWriterLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTWriterDispatch {
	

	private int templateStackHead = 0;
	private final int[] templateStack;
	
	
	private final PrimitiveWriter writer;
		
	
	private final FieldWriterInteger writerInteger;
	private final FieldWriterLong writerLong;
	private final FieldWriterDecimal writerDecimal;
	private final FieldWriterChar writerChar;
	private final FieldWriterBytes writerBytes;
	
	//template specific dictionaries
	private final int maxTemplates = 10;
	private final FieldWriterInteger[] templateWriterInteger;
	private final FieldWriterLong[] templateWriterLong;
	private final FieldWriterDecimal[] templateWriterDecimal;
	private final FieldWriterChar[] templateWriterChar;
	private final FieldWriterBytes[] templateWriterBytes;
	
			
	private final int[] tokenLookup; //array of tokens as field id locations
	

	
	
	public FASTWriterDispatch(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates, int[] tokenLookup) {
		//TODO: must set the initial values for default/constants from the template here.
		
		this.writer = writer;
		this.tokenLookup = tokenLookup;
		
		this.writerInteger 			= new FieldWriterInteger(writer, dcr.integerDictionary());
		this.writerLong    			= new FieldWriterLong(writer,dcr.longDictionary());
		//
		this.writerDecimal         = new FieldWriterDecimal(writer,dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.writerChar 			= new FieldWriterChar(writer,dcr.charDictionary());
		this.writerBytes 			= new FieldWriterBytes(writer,dcr.byteDictionary());
		
		this.templateWriterInteger = new FieldWriterInteger[maxTemplates];
		this.templateWriterLong    = new FieldWriterLong[maxTemplates];
		this.templateWriterDecimal = new FieldWriterDecimal[maxTemplates];
		this.templateWriterChar    = new FieldWriterChar[maxTemplates];
		this.templateWriterBytes   = new FieldWriterBytes[maxTemplates];
		
		this.templateStack = new int[maxTemplates];
	}
	
	private FieldWriterLong longDictionary(int token) {
		
		return (0==(token&(3<<TokenBuilder.SHIFT_DICT))) ?
				writerLong :  longDictionarySpecial(token);
		
	}

	private FieldWriterLong longDictionarySpecial(int token) {
		//these also take an extra lookup we are optimized for the global above			
		if (0==(token&(2<<TokenBuilder.SHIFT_DICT))) {
			int templateId = templateStack[templateStackHead];
			//AppType
			//FASTDynamic MUST know the template and therefore the type.
			//The template id is the first byte inside the group if pmap indicates.
			//that value must be read by unsignedInteger but can be done by open/close group!!
			throw new UnsupportedOperationException();
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_DICT))) {
				//Template
				throw new UnsupportedOperationException();
			} else {
				//Custom
				throw new UnsupportedOperationException();
			}
		}
	}
	
	private FieldWriterInteger integerDictionary(int token) {
		
		return (0==(token&(3<<TokenBuilder.SHIFT_DICT))) ?
				writerInteger : intDictionarySpecial(token);
		
	}
	
	private FieldWriterInteger intDictionarySpecial(int token) {
		//these also take an extra lookup we are optimized for the global above			
		if (0==(token&(2<<TokenBuilder.SHIFT_DICT))) {
			int templateId = templateStack[templateStackHead];
			//AppType
			//FASTDynamic MUST know the template and therefore the type.
			//The template id is the first byte inside the group if pmap indicates.
			//that value must be read by unsignedInteger but can be done by open/close group!!
			throw new UnsupportedOperationException();
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_DICT))) {
				//Template
				throw new UnsupportedOperationException();
			} else {
				//Custom
				throw new UnsupportedOperationException();
			}
		}
	}
	
	private FieldWriterDecimal decimalDictionary(int token) {
		
		return (0==(token&(3<<TokenBuilder.SHIFT_DICT))) ?
				writerDecimal : decimalDictionarySpecial(token);

	}
	
	private FieldWriterDecimal decimalDictionarySpecial(int token) {
		//these also take an extra lookup we are optimized for the global above			
		if (0==(token&(2<<TokenBuilder.SHIFT_DICT))) {
			int templateId = templateStack[templateStackHead];
			//AppType
			//FASTDynamic MUST know the template and therefore the type.
			//The template id is the first byte inside the group if pmap indicates.
			//that value must be read by unsignedInteger but can be done by open/close group!!
			throw new UnsupportedOperationException();
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_DICT))) {
				//Template
				throw new UnsupportedOperationException();
			} else {
				//Custom
				throw new UnsupportedOperationException();
			}
		}
	}
	
	private FieldWriterChar charDictionary(int token) {
		
		return (0==(token&(3<<TokenBuilder.SHIFT_DICT))) ?
				writerChar : charDictionarySpecial(token);
		
	}
	
	private FieldWriterChar charDictionarySpecial(int token) {
		//these also take an extra lookup we are optimized for the global above			
		if (0==(token&(2<<TokenBuilder.SHIFT_DICT))) {
			int templateId = templateStack[templateStackHead];
			//AppType
			//FASTDynamic MUST know the template and therefore the type.
			//The template id is the first byte inside the group if pmap indicates.
			//that value must be read by unsignedInteger but can be done by open/close group!!
			throw new UnsupportedOperationException();
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_DICT))) {
				//Template
				throw new UnsupportedOperationException();
			} else {
				//Custom
				throw new UnsupportedOperationException();
			}
		}
	}
	
	private FieldWriterBytes byteDictionary(int token) {
		
		return (0==(token&(3<<TokenBuilder.SHIFT_DICT))) ?
			writerBytes : bytesDictionarySpecial(token);
		
	}
	
	private FieldWriterBytes bytesDictionarySpecial(int token) {
		//these also take an extra lookup we are optimized for the global above			
		if (0==(token&(2<<TokenBuilder.SHIFT_DICT))) {
			int templateId = templateStack[templateStackHead];
			//AppType
			//FASTDynamic MUST know the template and therefore the type.
			//The template id is the first byte inside the group if pmap indicates.
			//that value must be read by unsignedInteger but can be done by open/close group!!
			throw new UnsupportedOperationException();
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_DICT))) {
				//Template
				throw new UnsupportedOperationException();
			} else {
				//Custom
				throw new UnsupportedOperationException();
			}
		}
	}
	
	/**
	 * Write null value, must only be used if the field id is one
	 * of optional type.
	 */
	public void write(int id) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		
		//only optional field types can use this method.
		assert(0!=(token&(1<<TokenBuilder.SHIFT_TYPE))); //TODO: in testing assert(failOnBadArg()) 
		
		//select on type, each dictionary will need to remember the null was written
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			// int long
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				// int
				integerDictionary(token).writeNull(token);
			} else {
				// long
				longDictionary(token).writeNull(token);
			}	
		} else {
			// text decimal bytes
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				// text
				charDictionary(token).writeNull(token);
			} else {
				// decimal bytes
				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
					// decimal
					decimalDictionary(token).writeNull(token);					
				} else {
					// byte
					byteDictionary(token).writeNull(token);
				}	
			}	
		}
		
	}

	
	/**
	 * Method for writing signed unsigned and/or optional longs.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	public void write(int id, long value) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			//not optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) { 
				acceptLongUnsigned(token, value);
			} else {
				acceptLongSigned(token, value);
			}
		} else {
			//optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				acceptLongUnsignedOptional(token, value);
			} else {
				acceptLongSignedOptional(token, value);
			}	
		}
	}

	private void acceptLongSignedOptional(int token, long value) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					longDictionary(token).writeLongSignedOptional(value,token);
				} else {
					//delta
					longDictionary(token).writeLongSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				longDictionary(token).writeLongSignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					longDictionary(token).writeLongSignedCopyOptional(value, token);
				} else {
					//increment
					longDictionary(token).writeLongSignedIncrementOptional(value, token);
				}	
			} else {
				// default
				longDictionary(token).writeLongSignedDefaultOptional(value, token);
			}		
		}
	}

	private void acceptLongSigned(int token, long value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					longDictionary(token).writeLongSigned(value, token);
				} else {
					//delta
					longDictionary(token).writeLongSignedDelta(value, token);
				}	
			} else {
				//constant
				longDictionary(token).writeLongSignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					longDictionary(token).writeLongSignedCopy(value, token);
				} else {
					//increment
					longDictionary(token).writeLongSignedIncrement(value, token);
				}	
			} else {
				// default
				longDictionary(token).writeLongSignedDefault(value, token);
			}		
		}
		
	}

	private void acceptLongUnsignedOptional(int token, long value) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					longDictionary(token).writeLongUnsignedOptional(value, token);
				} else {
					//delta
					longDictionary(token).writeLongUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				longDictionary(token).writeLongUnsignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					longDictionary(token).writeLongUnsignedCopyOptional(value, token);
				} else {
					//increment
					longDictionary(token).writeLongUnsignedIncrementOptional(value, token);
				}	
			} else {
				// default
				longDictionary(token).writeLongUnsignedDefaultOptional(value, token);
			}		
		}
	}
	
	private void acceptLongUnsigned(int token, long value) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					longDictionary(token).writeLongUnsigned(value, token);
				} else {
					//delta
					longDictionary(token).writeLongUnsignedDelta(value, token);
				}	
			} else {
				//constant
				longDictionary(token).writeLongUnsignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					longDictionary(token).writeLongUnsignedCopy(value, token);
				} else {
					//increment
					longDictionary(token).writeLongUnsignedIncrement(value, token);
				}	
			} else {
				// default
				longDictionary(token).writeLongUnsignedDefault(value, token);
			}		
		}
	}

	/**
	 * Method for writing signed unsigned and/or optional integers.
	 * To write the "null" or absence of an integer use 
	 *    void write(int id) 
	 */
	public void write(int id, int value) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			//not optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) { 
				acceptIntegerUnsigned(token, value);
			} else {
				acceptIntegerSigned(token, value);
			}
		} else {
			//optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				acceptIntegerUnsignedOptional(token, value);
			} else {
				acceptIntegerSignedOptional(token, value);
			}	
		}
	}
	
	private void acceptIntegerSigned(int token, int value) {
			
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					integerDictionary(token).writeIntegerSigned(value, token);
				} else {
					//delta
					integerDictionary(token).writeIntegerSignedDelta(value, token);
				}	
			} else {
				//constant
				integerDictionary(token).writeIntegerSignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					integerDictionary(token).writeIntegerSignedCopy(value, token);
				} else {
					//increment
					integerDictionary(token).writeIntegerSignedIncrement(value, token);
				}	
			} else {
				// default
				integerDictionary(token).writeIntegerSignedDefault(value, token);
			}		
		}
	}
	
	private void acceptIntegerUnsigned(int token, int value) {
						
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					integerDictionary(token).writeIntegerUnsigned(value, token);
				} else {
					//delta
					integerDictionary(token).writeIntegerUnsignedDelta(value, token);
				}	
			} else {
				//constant
				integerDictionary(token).writeIntegerUnsignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					integerDictionary(token).writeIntegerUnsignedCopy(value, token);
				} else {
					//increment
					integerDictionary(token).writeIntegerUnsignedIncrement(value, token);
				}	
			} else {
				// default
				integerDictionary(token).writeIntegerUnsignedDefault(value, token);
			}		
		}
	}

	private void acceptIntegerSignedOptional(int token, int value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					integerDictionary(token).writeIntegerSignedOptional(value, token);
				} else {
					//delta
					integerDictionary(token).writeIntegerSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				integerDictionary(token).writeIntegerSignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					integerDictionary(token).writeIntegerSignedCopyOptional(value, token);
				} else {
					//increment
					integerDictionary(token).writeIntegerSignedIncrementOptional(value, token);
				}	
			} else {
				// default
				integerDictionary(token).writeIntegerSignedDefaultOptional(value, token);
			}		
		}
	}
	
	private void acceptIntegerUnsignedOptional(int token, int value) {
				
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					integerDictionary(token).writerIntegerUnsignedOptional(value, token);
				} else {
					//delta
					integerDictionary(token).writeIntegerUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				integerDictionary(token).writeIntegerUnsignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					integerDictionary(token).writeIntegerUnsignedCopyOptional(value, token);
				} else {
					//increment
					integerDictionary(token).writeIntegerUnsignedIncrementOptional(value, token);
				}	
			} else {
				// default
				integerDictionary(token).writeIntegerUnsignedDefaultOptional(value, token);
			}		
		}
	}
	
	/**
	 * Method for writing decimals required or optional.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	public void write(int id, int exponent, long mantissa) {
				
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			decimalDictionary(token).writeDecimal(token, exponent, mantissa);			
		} else {
			decimalDictionary(token).writeDecimalOptional(token, exponent, mantissa);			
		}
	}

	public void write(int id, byte[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0!=(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			acceptByteArray(token, value, offset, length);			
		} else {
			acceptByteArrayOptional(token, value, offset, length);
		}
	}

	private void acceptByteArrayOptional(int token, byte[] value, int offset, int length) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					byteDictionary(token).writeBytesOptional(value, offset, length);
				} else {
					//tail
					byteDictionary(token).writeBytesTailOptional(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					byteDictionary(token).writeBytesConstantOptional(token);					
				} else {
					//delta
					byteDictionary(token).writeBytesDeltaOptional(token, value, offset, length);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				byteDictionary(token).writeBytesCopyOptional(token, value, offset, length);			
			} else {
				//default
				byteDictionary(token).writeBytesDefaultOptional(token, value, offset, length);				
			}
		}
	}

	private void acceptByteArray(int token, byte[] value, int offset, int length) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					byteDictionary(token).writeBytes(value, offset, length);
				} else {
					//tail
					byteDictionary(token).writeBytesTail(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					byteDictionary(token).writeBytesConstant(token);					
				} else {
					//delta
					byteDictionary(token).writeBytesDelta(token, value, offset, length);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				byteDictionary(token).writeBytesCopy(token, value, offset, length);			
			} else {
				//default
				byteDictionary(token).writeBytesDefault(token, value, offset, length);				
			}
		}
	}

	//TODO: add writeDup(int id) for repeating the last value sent,
	//this can avoid string check for copy operation if its already known that we are sending the same value.
	
	
	public void write(int id, ByteBuffer buffer) {
		
		int token = id>=0 ? tokenLookup[id] : id;
				
		assert(0!=(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			acceptByteBuffer(token, buffer);
		} else {
			acceptByteBufferOptional(token, buffer);
		}
	}


	private void acceptByteBufferOptional(int token, ByteBuffer value) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					byteDictionary(token).writeBytesOptional(value);
				} else {
					//tail
					byteDictionary(token).writeBytesTailOptional(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					byteDictionary(token).writeBytesConstantOptional(token);					
				} else {
					//delta
					byteDictionary(token).writeBytesDeltaOptional(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				byteDictionary(token).writeBytesCopyOptional(token,value);			
			} else {
				//default
				byteDictionary(token).writeBytesDefaultOptional(token,value);				
			}
		}
	}

	private void acceptByteBuffer(int token, ByteBuffer value) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					byteDictionary(token).writeBytes(value);
				} else {
					//tail
					byteDictionary(token).writeBytesTail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					byteDictionary(token).writeBytesConstant(token);					
				} else {
					//delta
					byteDictionary(token).writeBytesDelta(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				byteDictionary(token).writeBytesCopy(token,value);			
			} else {
				//default
				byteDictionary(token).writeBytesDefault(token,value);				
			}
		}
	}

	public void write(int id, CharSequence value) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii
				acceptCharSequenceASCII(token, value);
			} else {
				//utf8
				acceptCharSequenceUTF8(token, value);
			}
		} else {
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii optional
				acceptCharSequenceASCIIOptional(token, value);
			} else {
				//utf8 optional
				acceptCharSequenceUTF8Optional(token, value);
			}
		}
	}
	

	private void acceptCharSequenceUTF8Optional(int token, CharSequence value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					charDictionary(token).writeUTF8Optional(value);
				} else {
					//tail
					charDictionary(token).writeUTF8TailOptional(token,value);					
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					charDictionary(token).writeUTF8ConstantOptional(token);	
				} else {
					//delta
					charDictionary(token).writeUTF8DeltaOptional(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				charDictionary(token).writeUTF8CopyOptional(token,value);				
			} else {
				//default
				charDictionary(token).writeUTF8DefaultOptional(token,value);				
			}
		}
	}

	private void acceptCharSequenceUTF8(int token, CharSequence value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					charDictionary(token).writeUTF8(value);
				} else {
					//tail
					charDictionary(token).writeUTF8Tail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					charDictionary(token).writeUTF8Constant(token);					
				} else {
					//delta
					charDictionary(token).writeUTF8Delta(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				charDictionary(token).writeUTF8Copy(token,value);			
			} else {
				//default
				charDictionary(token).writeUTF8Default(token,value);				
			}
		}

	}

	private void acceptCharSequenceASCIIOptional(int token, CharSequence value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_None)) : "Found "+TokenBuilder.tokenToString(token);
					charDictionary(token).writeASCIITextOptional(token, value);
				} else {
					//tail
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "+TokenBuilder.tokenToString(token);
					charDictionary(token).writeASCIITailOptional(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "+TokenBuilder.tokenToString(token);
					charDictionary(token).writeASCIIConstantOptional(token);
				} else {
					//delta
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "+TokenBuilder.tokenToString(token);
					charDictionary(token).writeASCIIDeltaOptional(token,value);
					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "+TokenBuilder.tokenToString(token);
				charDictionary(token).writeASCIICopyOptional(token,value);
				
			} else {
				//default
				assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "+TokenBuilder.tokenToString(token);
				charDictionary(token).writeASCIIDefaultOptional(token,value);
				
			}
		}

	}

	private void acceptCharSequenceASCII(int token, CharSequence value) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none					
					charDictionary(token).writeASCII(value);
				} else {
					//tail
					charDictionary(token).writeASCIITail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					charDictionary(token).writeASCIIConstant(token);
				} else {
					//delta
					charDictionary(token).writeASCIIDelta(token,value);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				charDictionary(token).writeASCIICopy(token,value);
			} else {
				//default
				charDictionary(token).writeASCIIDefault(token,value);
			}
		}

	}

	public void write(int id, char[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii
				acceptCharArrayASCII(token,value,offset,length);
			} else {
				//utf8
				acceptCharArrayUTF8(token,value,offset,length);
			}
		} else {
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii optional
				acceptCharArrayASCIIOptional(token,value,offset,length);
			} else {
				//utf8 optional
				acceptCharArrayUTF8Optional(token,value,offset,length);
			}
		}
	}



	private void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerChar.writeUTF8Optional(value, offset, length);

				} else {
					//tail
					writerChar.writeUTF8TailOptional(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeUTF8ConstantOptional(token);
				} else {
					//delta
					writerChar.writeUTF8DeltaOptional(token, value, offset, length);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeUTF8CopyOptional(token, value, offset, length);
			} else {
				//default
				writerChar.writeUTF8DefaultOptional(token, value, offset, length);
			}
		}
		
	}

	private void acceptCharArrayUTF8(int token, char[] value, int offset, int length) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerChar.writeUTF8(value,offset,length);

				} else {
					//tail
					writerChar.writeUTF8Tail(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeUTF8Constant(token);
				} else {
					//delta
					writerChar.writeUTF8Delta(token, value, offset, length);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeUTF8Copy(token, value, offset, length);
			} else {
				//default
				writerChar.writeUTF8Default(token, value, offset, length);
			}
		}

	}

	private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerChar.writeASCIITextOptional(token, value, offset, length);
				} else {
					//tail
					writerChar.writeASCIITailOptional(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeASCIIConstantOptional(token);
				} else {
					//delta
					writerChar.writeASCIIDeltaOptional(token, value, offset, length);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeASCIICopyOptional(token, value, offset, length);
			} else {
				//default
				writerChar.writeASCIIDefaultOptional(token, value, offset, length);
			}
		}

	}

	private void acceptCharArrayASCII(int token, char[] value, int offset, int length) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					writerChar.writeASCIIText(token, value, offset, length);
				} else {
					//tail
					writerChar.writeASCIITail(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeASCIIConstant(token);
				} else {
					//delta
					writerChar.writeASCIIDelta(token, value, offset, length);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeASCIICopy(token, value, offset, length);
			} else {
				//default
				writerChar.writeASCIIDefault(token, value, offset, length);
			}
		}
	}

	public void openGroup(int token, int pmapSize) {	
		assert(token<0);
		assert(0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		assert(0==(token&(OperatorMask.Group_Bit_Templ<<TokenBuilder.SHIFT_OPER)));
		
		if (0!=(token&(OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER))) {
			writer.openPMap(pmapSize);
		}
		
	}
	
	public void openGroup(int token, int templateId, int pmapSize) {	
		assert(token<0);
		assert(0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		assert(0!=(token&(OperatorMask.Group_Bit_Templ<<TokenBuilder.SHIFT_OPER)));
	
		if (pmapSize>0) {
			writer.openPMap(pmapSize);
		}
		//done here for safety to ensure it is always done at group open.
		pushTemplate(templateId);
	}
	
	//must happen just before Group so the Group in question must always have 
	//an outer group. 
	private void pushTemplate(int templateId) {
		int top = templateStack[templateStackHead]; 
		if (top==templateId) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(templateId);
			top = templateId;
		}
		
		templateStack[templateStackHead++] = top;
	}


	public void closeGroup(int token) {
		assert(token<0);
		assert(0!=(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		
		if (0!=(token&(OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER))) {
			writer.closePMap();
		}
		
		if (0!=(token&(OperatorMask.Group_Bit_Templ<<TokenBuilder.SHIFT_OPER))) {
    		//must always pop because open will always push
			templateStackHead--;
		}
		
	}

	public void flush() {
		writer.flush();
	}

	
	public void reset(DictionaryFactory df) {
		//reset all values to unset
		writerInteger.reset(df);
		writerLong.reset(df);
	}




}
