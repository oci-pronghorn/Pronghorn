package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterChar;
import com.ociweb.jfast.field.FieldWriterDecimal;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.FieldWriterLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTStaticWriter implements FASTWriter {
	

	private int templateStackHead = 0;
	private int[] templateStack = new int[100];// //TODO: need max depth?
	
	//TODO: add assert at beginning of every method that calls a FASTAccept
	//       object which is only created from the template when assert is on
	//       this will validate each FieldId/Token is the expected one in that order.
	
	private final PrimitiveWriter writer;
	
	
	//TODO: each of these instances represent a specific dictionary.
	
	
	private final FieldWriterInteger writerInteger;
	private final FieldWriterLong writerLong;
	private final FieldWriterDecimal writerDecimal;
	private final FieldWriterChar writerChar;
	private final FieldWriterBytes writerBytes;
		
	//TODO: constant logic is not built for the optional/pmap case
	
	private final int[] tokenLookup; //array of tokens as field id locations
	

	
	
	public FASTStaticWriter(PrimitiveWriter writer, DictionaryFactory dcr) {
		//TODO: must set the initial values for default/constants from the template here.
		//TODO: perhaps the arrays should be allocated external so template parser can manage it?
		
		this.writer = writer;
		this.tokenLookup = dcr.getTokenLookup();
		
		this.writerInteger 			= new FieldWriterInteger(writer, dcr.integerDictionary());
		this.writerLong    			= new FieldWriterLong(writer,dcr.longDictionary());
		//
		this.writerDecimal         = new FieldWriterDecimal(writer,dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.writerChar 			= new FieldWriterChar(writer,dcr.charDictionary());
		this.writerBytes 			= new FieldWriterBytes(writer,dcr.byteDictionary());
		
	}
	
	/**
	 * Write null value, must only be used if the field id is one
	 * of optional type.
	 */
	@Override
	public void write(int id) {
		//TODO: write null value into this optional type.
		
		int token = id>=0 ? tokenLookup[id] : id;
		
		//only optional field types can use this method.
		assert(0!=(token&(1<<TokenBuilder.SHIFT_TYPE)));
		
		//select on type, each dictionary will need to remember the null was written
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			// int long
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				// int
				writerInteger.writeNull(token);
			} else {
				// long
				writerLong.writeNull(token);
			}	
		} else {
			// text decimal bytes
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				// text
				writerChar.writeNull(token);
			} else {
				// decimal bytes
				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
					// decimal
					writerDecimal.writeNull(token);					
				} else {
					// byte
					writerBytes.writeNull(token);
				}	
			}	
		}
		
	}

	
	/**
	 * Method for writing signed unsigned and/or optional longs.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	@Override
	public void write(int id, long value) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			//not optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) { 
				acceptLongUnsigned(token, value, writerLong);
			} else {
				acceptLongSigned(token, value, writerLong);
			}
		} else {
			//optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				acceptLongUnsignedOptional(token, value, writerLong);
			} else {
				acceptLongSignedOptional(token, value, writerLong);
			}	
		}
	}

	private void acceptLongSignedOptional(int token, long value, FieldWriterLong fieldWriterLong) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					fieldWriterLong.writeLongSignedOptional(value,token);
				} else {
					//delta
					fieldWriterLong.writeLongSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				fieldWriterLong.writeLongSignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					fieldWriterLong.writeLongSignedCopyOptional(value, token);
				} else {
					//increment
					fieldWriterLong.writeLongSignedIncrementOptional(value, token);
				}	
			} else {
				// default
				fieldWriterLong.writeLongSignedDefaultOptional(value, token);
			}		
		}
	}

	private void acceptLongSigned(int token, long value, FieldWriterLong fieldWriterLong) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					fieldWriterLong.writeLongSigned(value, token);
				} else {
					//delta
					fieldWriterLong.writeLongSignedDelta(value, token);
				}	
			} else {
				//constant
				fieldWriterLong.writeLongSignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					fieldWriterLong.writeLongSignedCopy(value, token);
				} else {
					//increment
					fieldWriterLong.writeLongSignedIncrement(value, token);
				}	
			} else {
				// default
				fieldWriterLong.writeLongSignedDefault(value, token);
			}		
		}
		
	}

	private void acceptLongUnsignedOptional(int token, long value, FieldWriterLong fieldWriterLong) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					fieldWriterLong.writeLongUnsignedOptional(value, token);
				} else {
					//delta
					fieldWriterLong.writeLongUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				fieldWriterLong.writeLongUnsignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					fieldWriterLong.writeLongUnsignedCopyOptional(value, token);
				} else {
					//increment
					fieldWriterLong.writeLongUnsignedIncrementOptional(value, token);
				}	
			} else {
				// default
				fieldWriterLong.writeLongUnsignedDefaultOptional(value, token);
			}		
		}
	}

	//TODO: pass in writerLong instance to this private method.
	
	private void acceptLongUnsigned(int token, long value, FieldWriterLong fieldWriterLong) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					fieldWriterLong.writeLongUnsigned(value, token);
				} else {
					//delta
					fieldWriterLong.writeLongUnsignedDelta(value, token);
				}	
			} else {
				//constant
				fieldWriterLong.writeLongUnsignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					fieldWriterLong.writeLongUnsignedCopy(value, token);
				} else {
					//increment
					fieldWriterLong.writeLongUnsignedIncrement(value, token);
				}	
			} else {
				// default
				fieldWriterLong.writeLongUnsignedDefault(value, token);
			}		
		}
	}

	/**
	 * Method for writing signed unsigned and/or optional integers.
	 * To write the "null" or absence of an integer use 
	 *    void write(int id) 
	 */
	@Override
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
					writer.writeIntegerSigned(value);
				} else {
					//delta
					writerInteger.writeIntegerSignedDelta(value, token);
				}	
			} else {
				//constant
				writerInteger.writeIntegerSignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerInteger.writeIntegerSignedCopy(value, token);
				} else {
					//increment
					writerInteger.writeIntegerSignedIncrement(value, token);
				}	
			} else {
				// default
				writerInteger.writeIntegerSignedDefault(value, token);
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
					writer.writeIntegerUnsigned(value);
				} else {
					//delta
					writerInteger.writeIntegerUnsignedDelta(value, token);
				}	
			} else {
				//constant
				writerInteger.writeIntegerUnsignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerInteger.writeIntegerUnsignedCopy(value, token);
				} else {
					//increment
					writerInteger.writeIntegerUnsignedIncrement(value, token);
				}	
			} else {
				// default
				writerInteger.writeIntegerUnsignedDefault(value, token);
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
					writer.writeIntegerSignedOptional(value);
				} else {
					//delta
					writerInteger.writeIntegerSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				writerInteger.writeIntegerSignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerInteger.writeIntegerSignedCopyOptional(value, token);
				} else {
					//increment
					writerInteger.writeIntegerSignedIncrementOptional(value, token);
				}	
			} else {
				// default
				writerInteger.writeIntegerSignedDefaultOptional(value, token);
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
					writer.writeIntegerUnsigned(value+1);
				} else {
					//delta
					writerInteger.writeIntegerUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				writerInteger.writeIntegerUnsignedConstantOptional(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerInteger.writeIntegerUnsignedCopyOptional(value, token);
				} else {
					//increment
					writerInteger.writeIntegerUnsignedIncrementOptional(value, token);
				}	
			} else {
				// default
				writerInteger.writeIntegerUnsignedDefaultOptional(value, token);
			}		
		}
	}
	
	/**
	 * Method for writing decimals required or optional.
	 * To write the "null" or absence of a value use 
	 *    void write(int id) 
	 */
	@Override
	public void write(int id, int exponent, long mantissa) {
				
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			writerDecimal.writeDecimal(token, exponent, mantissa);			
		} else {
			writerDecimal.writeDecimalOptional(token, exponent, mantissa);			
		}
	}

	@Override
	public void write(int id, byte[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE)));
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
					writerBytes.writeBytesOptional(value, offset, length);
				} else {
					//tail
					writerBytes.writeBytesTailOptional(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerBytes.writeBytesConstantOptional(token);					
				} else {
					//delta
					writerBytes.writeBytesDeltaOptional(token, value, offset, length);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerBytes.writeBytesCopyOptional(token, value, offset, length);			
			} else {
				//default
				writerBytes.writeBytesDefaultOptional(token, value, offset, length);				
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
					writerBytes.writeBytes(value, offset, length);
				} else {
					//tail
					writerBytes.writeBytesTail(token, value, offset, length);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerBytes.writeBytesConstant(token);					
				} else {
					//delta
					writerBytes.writeBytesDelta(token, value, offset, length);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerBytes.writeBytesCopy(token, value, offset, length);			
			} else {
				//default
				writerBytes.writeBytesDefault(token, value, offset, length);				
			}
		}
	}

	//TODO: add writeDup(int id) for repeating the last value sent,
	//this can avoid string check for copy operation if its already known that we are sending the same value.
	
	@Override
	public void write(int id, ByteBuffer buffer) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
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
					writerBytes.writeBytesOptional(value);
				} else {
					//tail
					writerBytes.writeBytesTailOptional(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerBytes.writeBytesConstantOptional(token);					
				} else {
					//delta
					writerBytes.writeBytesDeltaOptional(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerBytes.writeBytesCopyOptional(token,value);			
			} else {
				//default
				writerBytes.writeBytesDefaultOptional(token,value);				
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
					writerBytes.writeBytes(value);
				} else {
					//tail
					writerBytes.writeBytesTail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerBytes.writeBytesConstant(token);					
				} else {
					//delta
					writerBytes.writeBytesDelta(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerBytes.writeBytesCopy(token,value);			
			} else {
				//default
				writerBytes.writeBytesDefault(token,value);				
			}
		}
	}

	@Override
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
					writerChar.writeUTF8Optional(value);
				} else {
					//tail
					writerChar.writeUTF8TailOptional(token,value);					
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeUTF8ConstantOptional(token);	
				} else {
					//delta
					writerChar.writeUTF8DeltaOptional(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeUTF8CopyOptional(token,value);				
			} else {
				//default
				writerChar.writeUTF8DefaultOptional(token,value);				
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
					writerChar.writeUTF8(value);
				} else {
					//tail
					writerChar.writeUTF8Tail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeUTF8Constant(token);					
				} else {
					//delta
					writerChar.writeUTF8Delta(token,value);					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeUTF8Copy(token,value);			
			} else {
				//default
				writerChar.writeUTF8Default(token,value);				
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
					writer.writeTextASCII(value);
				} else {
					//tail
					writerChar.writeASCIITailOptional(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeASCIIConstantOptional(token);
				} else {
					//delta
					writerChar.writeASCIIDeltaOptional(token,value);
					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeASCIICopyOptional(token,value);
				
			} else {
				//default
				writerChar.writeASCIIDefaultOptional(token,value);
				
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
					writerChar.writeASCII(value);
				} else {
					//tail
					writerChar.writeASCIITail(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					writerChar.writeASCIIConstant(token);
				} else {
					//delta
					writerChar.writeASCIIDelta(token,value);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				writerChar.writeASCIICopy(token,value);
			} else {
				//default
				writerChar.writeASCIIDefault(token,value);
			}
		}

	}

	@Override
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
					writer.writeTextASCII(value,offset,length);
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
					writer.writeTextASCII(value,offset,length);
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

	@Override
	public void openGroup(int id, int template) {	
		int token = id>=0 ? tokenLookup[id] : id;
		
		//TODO: do we need a two more open group methods for dynamic template ids?
		
		
		//int repeat = 
		//if sequence is not set by writer must use sequence provided
	    //0 equals 1	
		int maxBytes = TokenBuilder.extractMaxBytes(token);
		if (maxBytes>0) {
			writer.openPMap(maxBytes);
		}
		
		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
			//always push something on to the stack
									
			int top = templateStack[templateStackHead]; 
			if (top==template) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeIntegerUnsigned(template);
				top = template;
			}
			
			templateStack[templateStackHead++] = top;
		}
	}

	@Override
	public void openGroup(int id, int repeat, int template) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		//repeat count provided
		
		int maxBytes = TokenBuilder.extractMaxBytes(token);
		if (maxBytes>0) {
			writer.openPMap(maxBytes);
		}
		//TODO: is this the point when we write the repeat?
		
		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
			//always push something on to the stack
						
			int top = templateStack[templateStackHead]; 
			if (top==template) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeIntegerUnsigned(template);
				top = template;
			}
			
			templateStack[templateStackHead++] = top;

		}
	}

	@Override
	public void closeGroup(int id) {
		
		//must have same token used for opening the group.
		int token = id>=0 ? tokenLookup[id] : id;

		int maxBytes = TokenBuilder.extractMaxBytes(token);
		if (maxBytes>0) {
			writer.closePMap();
		}
		
		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
			//must always pop because open will always push
			templateStackHead--;
		}
	}

	@Override
	public void flush() {
		writer.flush();
	}

	
	public void reset(DictionaryFactory df) {
		//reset all values to unset
		writerInteger.reset(df);
		writerLong.reset(df);
	}




}
