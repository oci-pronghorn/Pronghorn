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
	
	final int nonTemplatePMapSize;
		
			
	private int readFromIdx = -1;
	
	private final DictionaryFactory dictionaryFactory;
	private final FASTRingBuffer queue;
	private final int[][] dictionaryMembers;
	
	private final int[] sequenceCountStack;
	private int sequenceCountStackHead = -1;
	private boolean isFirstSequenceItem = false;
	private boolean isSkippedSequence = false;
		
	public FASTWriterDispatch(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates, 
			                   int maxCharSize, int maxBytesSize, int gapChars, int gapBytes,
			                   FASTRingBuffer queue, int nonTemplatePMapSize, int[][] dictionaryMembers) {

		this.writer = writer;
		this.dictionaryFactory = dcr;
		this.nonTemplatePMapSize = nonTemplatePMapSize;
		
		this.sequenceCountStack = new int[100];//TODO: find the right size
		
		this.writerInteger 			= new FieldWriterInteger(writer, dcr.integerDictionary());
		this.writerLong    			= new FieldWriterLong(writer,dcr.longDictionary());
		//
		this.writerDecimal         = new FieldWriterDecimal(writer,dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.writerChar 			= new FieldWriterChar(writer,dcr.charDictionary(maxCharSize,gapChars)); 
		this.writerBytes 			= new FieldWriterBytes(writer,dcr.byteDictionary(maxBytesSize,gapBytes));
				
		this.templateStack = new int[maxTemplates];
		this.queue = queue;
		this.dictionaryMembers = dictionaryMembers;
	}
	
	/**
	 * Write null value, must only be used if the field id is one
	 * of optional type.
	 */
	public void write(int token) {
				
		//only optional field types can use this method.
		assert(0!=(token&(1<<TokenBuilder.SHIFT_TYPE))); //TODO: in testing assert(failOnBadArg()) 
		
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
	public void write(int token, long value) {
		
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
					writerLong.writeLongSignedOptional(value, token);
				} else {
					//delta
					writerLong.writeLongSignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				writerLong.writeLongSignedConstantOptional(value, token);
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
					writerLong.writeLongSigned(value, token);
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
					writerLong.writeLongUnsignedOptional(value, token);
				} else {
					//delta
					writerLong.writeLongUnsignedDeltaOptional(value, token);
				}	
			} else {
				//constant
				writerLong.writeLongUnsignedConstantOptional(value, token);
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
					writerLong.writeLongUnsigned(value, token);
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
	public void write(int token, int value) {
		
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
					writerInteger.writeIntegerSigned(value, token);
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
					writerInteger.writeIntegerUnsigned(value, token);
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
					writerInteger.writeIntegerSignedOptional(value, token);
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
					writerInteger.writerIntegerUnsignedOptional(value, token);
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
	public void write(int token, int exponent, long mantissa) {
						
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			writerDecimal.writeDecimal(token, exponent, mantissa);			
		} else {
			writerDecimal.writeDecimalOptional(token, exponent, mantissa);			
		}
	}

	public void write(int token, byte[] value, int offset, int length) {
		
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
	
	
	public void write(int token, ByteBuffer buffer) {
						
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

	public void write(int token, CharSequence value) {
		
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
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_None)) : "Found "+TokenBuilder.tokenToString(token);
					writerChar.writeASCIITextOptional(value);
				} else {
					//tail
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "+TokenBuilder.tokenToString(token);
					writerChar.writeASCIITailOptional(token,value);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "+TokenBuilder.tokenToString(token);
					writerChar.writeASCIIConstantOptional(token);
				} else {
					//delta
					assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "+TokenBuilder.tokenToString(token);
					writerChar.writeASCIIDeltaOptional(token,value);
					
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "+TokenBuilder.tokenToString(token);
				writerChar.writeASCIICopyOptional(token,value);
				
			} else {
				//default
				assert(	TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "+TokenBuilder.tokenToString(token);
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

	public void write(int token, char[] value, int offset, int length) {
		
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
					writerChar.writeASCIITextOptional(value, offset, length);
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

	
	public void reset() {
		
		System.err.println("wrote fields count:"+fieldCount);
		fieldCount=0;
		
		//reset all values to unset
		//TODO: must find faster way to do this for both writer and reader!
		writerInteger.reset(dictionaryFactory);
		writerLong.reset(dictionaryFactory);
		writerDecimal.reset(dictionaryFactory);
		writerChar.reset(dictionaryFactory);
		writerBytes.reset(dictionaryFactory);
		templateStackHead = 0;
	}

	
	public boolean isFirstSequenceItem() {
		return isFirstSequenceItem;
	}
	
	public boolean isSkippedSequence() {
		return isSkippedSequence;
	}	
	
    long fieldCount = 0;
	
	public boolean dispatchWriteByToken(int token, int fieldPos) {
	
		
		System.err.println("Dispatch "+TokenBuilder.tokenToString(token)+" fieldPos "+fieldPos+" ringIdx:"+(queue.remPos+fieldPos) );

		
		fieldCount++;
		
		if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
			//0????
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//00???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					write(token,queue.readInteger(fieldPos));
				} else {
					write(token,queue.readLong(fieldPos));
				}
			} else {
				//01???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					
					queue.selectCharSequence(fieldPos);
					//NOTE: Use CharSequence implementation today, direct ring buffer tomorrow.
					write(token,queue.length()<0?null:queue);
									
				} else {
					//011??
					if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
						//0110? Decimal and DecimalOptional
						write(token,queue.readInteger(fieldPos),queue.readLong(fieldPos+1));
					} else {
//						//0111? ByteArray
						if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
							//01110 ByteArray
							//queue.selectByteSequence(fieldPos);
							//write(token,queue); TODO: copy the text implementation
						} else {
							//01111 ByteArrayOptional
							//queue.selectByteSequence(fieldPos);
							//write(token,queue); TODO: copy the text implementation
						}
					}
				}
			}
		} else {
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//10???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//100??
					//Group Type, no others defined so no need to keep checking
					if (0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER))) {

						isSkippedSequence = false;
						isFirstSequenceItem = false;
						//this is NOT a message/template so the non-template pmapSize is used.			
				//		System.err.println("open group:"+TokenBuilder.tokenToString(token));
						openGroup(token, nonTemplatePMapSize);
					} else {
					//	System.err.println("close group:"+TokenBuilder.tokenToString(token));
						closeGroup(token);//closing this seq causing throw!!
						if (0!=(token&(OperatorMask.Group_Bit_Seq<<TokenBuilder.SHIFT_OPER))) {
				    		//must always pop because open will always push
							if (0 == --sequenceCountStack[sequenceCountStackHead]) {
								sequenceCountStackHead--;//pop sequence off because they have all been used.
							}
							return true;
						}
					}
					
				} else {
					//101??
					//Length Type, no others defined so no need to keep checking
					//Only happens once before a node sequence so push it on the count stack
					int length=queue.readInteger(fieldPos);
					write(token, length);
					
					if (length==0) {
						isFirstSequenceItem = false;
						isSkippedSequence = true;
					} else {		
						isFirstSequenceItem = true;
						isSkippedSequence = false;
						sequenceCountStack[++sequenceCountStackHead] = length;
					}
					return true;
				}
			} else {
				//11???
				//Dictionary Type, no others defined so no need to keep checking
				if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
					//reset the values
					int dictionary = TokenBuilder.MAX_INSTANCE&token;

					int[] members = dictionaryMembers[dictionary];
					//System.err.println(members.length+" "+Arrays.toString(members));
					
					int m = 0;
					int limit = members.length;
					if (limit>0) {
						int idx = members[m++];
						while (m<limit) {
							assert(idx<0);
							
							if (0==(idx&8)) {
								if (0==(idx&4)) {
									//integer
									while (m<limit && (idx = members[m++])>=0) {
										writerInteger.reset(idx);
									}
								} else {
									//long
									while (m<limit && (idx = members[m++])>=0) {
										writerLong.reset(idx);
									}
								}
							} else {
								if (0==(idx&4)) {							
									//text
									while (m<limit && (idx = members[m++])>=0) {
										writerChar.reset(idx);
									}
								} else {
									if (0==(idx&2)) {								
										//decimal
										while (m<limit && (idx = members[m++])>=0) {
											writerDecimal.reset(idx);
										}
									} else {
										//bytes
										while (m<limit && (idx = members[m++])>=0) {
											writerBytes.reset(idx);
										}
									}
								}
							}	
						}
					}
				} else {
					//use last value from this location
					readFromIdx = TokenBuilder.MAX_INSTANCE&token;
				}
					
			}
			
		}
		return false;
	}



}
