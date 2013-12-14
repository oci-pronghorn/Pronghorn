package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

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
	

	
	
	public FASTStaticWriter(PrimitiveWriter writer, DictionaryFactory dcr, int[] tokenLookup) {
		//TODO: must set the initial values for default/constants from the template here.
		//TODO: perhaps the arrays should be allocated external so template parser can manage it?
		
		this.writer = writer;
		this.tokenLookup = tokenLookup;
		
		this.writerInteger 			= new FieldWriterInteger(writer, dcr.integerDictionary());
		this.writerLong    			= new FieldWriterLong(writer,dcr.longDictionary());
		//
		this.writerDecimal         = new FieldWriterDecimal(writer,dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.writerChar 			= new FieldWriterChar(writer,dcr.charDictionary());
		this.writerBytes 			= null;
		
		//TODO: add the Text and Bytes
		
		
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
					writer.writeLongSignedOptional(value);
				} else {
					//delta
					writerLong.writeLongSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				//err
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerLong.writeLongSignedCopyOptional(value, token);
				} else {
					//increment
					writerLong.writeLongSignedIncrementOptional(value, token);
				}	
			} else {
				// default
				writerLong.writeLongSignedDefaultOptional(value, token);
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
					writer.writeLongSigned(value);
				} else {
					//delta
					writerLong.writeLongSignedDelta(value, token);
				}	
			} else {
				//constant
				writerLong.writeLongSignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerLong.writeLongSignedCopy(value, token);
				} else {
					//increment
					writerLong.writeLongSignedIncrement(value, token);
				}	
			} else {
				// default
				writerLong.writeLongSignedDefault(value, token);
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
					writer.writeLongUnsigned(value+1);//should be in writerLong
				} else {
					//delta
					writerLong.writeLongUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				//ERR
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerLong.writeLongUnsignedCopyOptional(value, token);
				} else {
					//increment
					writerLong.writeLongUnsignedIncrementOptional(value, token);
				}	
			} else {
				// default
				writerLong.writeLongUnsignedDefaultOptional(value, token);
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
					writer.writeLongUnsigned(value);
				} else {
					//delta
					writerLong.writeLongUnsignedDelta(value, token);
				}	
			} else {
				//constant
				writerLong.writeLongUnsignedConstant(value, token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					writerLong.writeLongUnsignedCopy(value, token);
				} else {
					//increment
					writerLong.writeLongUnsignedIncrement(value, token);
				}	
			} else {
				// default
				writerLong.writeLongUnsignedDefault(value, token);
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
				//writerInteger.writeIntegerSignedConstantOptional(value, token);
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
				//writerInteger.writeIntegerUnsignedConstantOptional(value, token);
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
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptByteArray(int token, byte[] value, int offset, int length) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, ByteBuffer buffer) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.ByteArray: 
				acceptByteBuffer(token, buffer);
				break;
			case TypeMask.ByteArrayOptional:
				acceptByteBufferOptional(token, buffer);
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}


	private void acceptByteBufferOptional(int token, ByteBuffer buffer) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptByteBuffer(int token, ByteBuffer buffer) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				throw new UnsupportedOperationException();
				//break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, CharSequence value) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII: 
				acceptCharSequenceASCII(token, value);
				break;
			case TypeMask.TextASCIIOptional:
				acceptCharSequenceASCIIOptional(token, value);
				break;
			case TypeMask.TextUTF8: 
				acceptCharSequenceUTF8(token, value);
				break;
			case TypeMask.TextUTF8Optional:
				acceptCharSequenceUTF8Optional(token, value);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}
	

	private void acceptCharSequenceUTF8Optional(int token, CharSequence value) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
				break;
			case OperatorMask.Copy:
				writerChar.writeUTF8CopyOptional(token,value);
				break;
			case OperatorMask.Default:
				writerChar.writeUTF8DefaultOptional(token,value);
				break;
			case OperatorMask.Delta:
				writerChar.writeUTF8DeltaOptional(token,value);
				break;
			case OperatorMask.Tail:
				writerChar.writeUTF8TailOptional(token,value);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceUTF8(int token, CharSequence value) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(value.length());
				writer.writeTextUTF(value);
				break;
			case OperatorMask.Copy:
				writerChar.writeUTF8Copy(token,value);
				break;
			case OperatorMask.Constant:
				writerChar.writeUTF8Constant(token,value);
				break;
			case OperatorMask.Default:
				writerChar.writeUTF8Default(token,value);
				break;
			case OperatorMask.Delta:
				writerChar.writeUTF8Delta(token,value);
				break;	
			case OperatorMask.Tail:
				writerChar.writeUTF8Tail(token,value);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCIIOptional(int token, CharSequence value) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value);
				break;
			case OperatorMask.Copy:
				writerChar.writeASCIICopyOptional(token,value);
				break;
			case OperatorMask.Default:
				writerChar.writeASCIIDefaultOptional(token,value);
				break;
			case OperatorMask.Delta:
				writerChar.writeASCIIDeltaOptional(token,value);
				break;	
			case OperatorMask.Tail:
				writerChar.writeASCIITailOptional(token,value);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharSequenceASCII(int token, CharSequence value) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value);
				break;
			case OperatorMask.Copy:
				writerChar.writeASCIICopy(token,value);
				break;
			case OperatorMask.Constant:
				writerChar.writeASCIIConstant(token,value);
				break;
			case OperatorMask.Default:
				writerChar.writeASCIIDefault(token,value);
				break;
			case OperatorMask.Delta:
				writerChar.writeASCIIDelta(token,value);
				break;	
			case OperatorMask.Tail:
				writerChar.writeASCIITail(token,value);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void write(int id, char[] value, int offset, int length) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII: 
				acceptCharArrayASCII(token,value,offset,length);
				break;
			case TypeMask.TextASCIIOptional:
				acceptCharArrayASCIIOptional(token,value,offset,length);
				break;
			case TypeMask.TextUTF8: 
				acceptCharArrayUTF8(token,value,offset,length);
				break;
			case TypeMask.TextUTF8Optional:
				acceptCharArrayUTF8Optional(token,value,offset,length);
				break;
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}



	private void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(length+1);
				writer.writeTextUTF(value,offset,length);
				break;
			case OperatorMask.Copy:
				writerChar.writeUTF8CopyOptional(token, value, offset, length);
				break;
			case OperatorMask.Default:
				writerChar.writeUTF8DefaultOptional(token, value, offset, length);
				break;
			case OperatorMask.Delta:
				writerChar.writeUTF8DeltaOptional(token, value, offset, length);
				break;	
			case OperatorMask.Tail:
				writerChar.writeUTF8TailOptional(token, value, offset, length);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayUTF8(int token, char[] value, int offset, int length) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeIntegerUnsigned(length);
				writer.writeTextUTF(value,offset,length);
				break;
			case OperatorMask.Copy:
				writerChar.writeUTF8Copy(token, value, offset, length);
				break;
			case OperatorMask.Constant:
				writerChar.writeUTF8Constant(token, value, offset, length);
				break;
			case OperatorMask.Default:
				writerChar.writeUTF8Default(token, value, offset, length);
				break;
			case OperatorMask.Delta:
				writerChar.writeUTF8Delta(token, value, offset, length);
				break;
			case OperatorMask.Tail:
				writerChar.writeUTF8Tail(token, value, offset, length);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value,offset,length);
				break;
			case OperatorMask.Copy:
				writerChar.writeASCIICopyOptional(token, value, offset, length);
				break;
			case OperatorMask.Default:
				writerChar.writeASCIIDefaultOptional(token, value, offset, length);
				break;
			case OperatorMask.Delta:
				writerChar.writeASCIIDeltaOptional(token, value, offset, length);
				break;	
			case OperatorMask.Tail:
				writerChar.writeASCIITailOptional(token, value, offset, length);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void acceptCharArrayASCII(int token, char[] value, int offset, int length) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				writer.writeTextASCII(value,offset,length);
				break;
			case OperatorMask.Copy:
				writerChar.writeASCIICopy(token, value, offset, length);
				break;
			case OperatorMask.Constant:
				writerChar.writeASCIIConstant(token, value, offset, length);
				break;
			case OperatorMask.Default:
				writerChar.writeASCIIDefault(token, value, offset, length);
				break;
			case OperatorMask.Delta:
				writerChar.writeASCIIDelta(token, value, offset, length);
				break;	
			case OperatorMask.Tail:
				writerChar.writeASCIITail(token, value, offset, length);
				break;	
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void openGroup(int id) {	
		int token = id>=0 ? tokenLookup[id] : id;
		
		//TODO: do we need a two more open group methods for dynamic template ids?
		
		
		//int repeat = 
		//if sequence is not set by writer must use sequence provided
	    //0 equals 1	
		int maxBytes = TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK);
		if (maxBytes>0) {
			writer.openPMap(maxBytes);
		}
	}
	
	@Override
	public void openGroup(int id, int repeat) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		//repeat count provided
		
		int maxBytes = TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK);
		if (maxBytes>0) {
			writer.openPMap(maxBytes);
		}
		//TODO: is this the point when we write the repeat?
		
	}

	@Override
	public void closeGroup(int id) {
		//must have same token used for opening the group.
		int token = id>=0 ? tokenLookup[id] : id;
		int maxBytes = TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK);
		if (maxBytes>0) {
			writer.closePMap();
		}
	}

	@Override
	public void flush() {
		writer.flush();
	}

	public boolean isGroupOpen() {//TODO: is this feature really needed?
		return writer.isPMapOpen();
	}

	
	public void reset(DictionaryFactory df) {
		//reset all values to unset
		writerInteger.reset(df);
		writerLong.reset(df);
	}




}
