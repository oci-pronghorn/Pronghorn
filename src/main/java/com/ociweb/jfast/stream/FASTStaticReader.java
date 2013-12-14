package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderChar;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTStaticReader implements FASTReader {

	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	//package protected so DynamicReader can use these instances
	private final FieldReaderInteger readerInteger;
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderChar readerChar;
	private final FieldReaderBytes readerBytes;
	

	
		
	public FASTStaticReader(PrimitiveReader reader, DictionaryFactory dcr, int[] tokenLookup) {
		this.reader = reader;
		this.tokenLookup = tokenLookup;
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.readerChar = new FieldReaderChar(reader,dcr.charDictionary());
		this.readerBytes = null;
		
		
		
	}
	

	public void reset(DictionaryFactory df) {
		//clear all previous values to unset
		readerInteger.reset(df);
		readerLong.reset(df);
	}

	
	//package protected, unless we find a need to expose it?
	void readToken(int token) {
		//used by groups which hold list of tokens
		//at end of each group call back may be done and FASTDynamicReader used.

	    switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.IntegerUnsigned:
				readIntegerUnsigned(token);
				break;
			case TypeMask.IntegerUnsignedOptional:
				readIntegerUnsignedOptional(token,0);
				break;
			case TypeMask.IntegerSigned:
				readIntegerSigned(token);
				break;
			case TypeMask.IntegerSignedOptional:
				readIntegerSignedOptional(token,0);
				break;
			case TypeMask.LongUnsigned:
				readLongUnsigned(token);
				break;
			case TypeMask.LongUnsignedOptional:
				readLongUnsignedOptional(token,0);
				break;
			case TypeMask.LongSigned:
				readLongSigned(token);
				break;
			case TypeMask.LongSignedOptional:
				readLongSignedOptional(token,0);
				break;
			case TypeMask.TextASCII:
				readTextASCII(token, null); //TODO: these nulls are not corect but we do not need the result.
			    break;
		    case TypeMask.TextASCIIOptional:
				readTextASCIIOptional(token, null);
			    break;
			case TypeMask.TextUTF8:
				readTextUTF8(token,null);
				break;
			case TypeMask.TextUTF8Optional:
				readTextUTF8Optional(token, null);
				break;
			case TypeMask.Decimal:
				//readerDecimal();
				break;
			case TypeMask.DecimalOptional:
				break;
			case TypeMask.ByteArray:
				break;
			case TypeMask.ByteArrayOptional:
				break;
			default:
				throw new UnsupportedOperationException();
			}
	}
	
	
	@Override
	public long readLong(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			//not optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) { 
				return readLongUnsigned(token);
			} else {
				return readLongSigned(token);
			}
		} else {
			//optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				return readLongUnsignedOptional(token, valueOfOptional);
			} else {
				return readLongSignedOptional(token, valueOfOptional);
			}	
		}
		
	}
	
	private long readLongSignedOptional(int token, long valueOfOptional) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerLong.readLongSignedOptional(token,valueOfOptional);
				} else {
					//delta
					return readerLong.readLongSignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				//writerInteger.writeIntegerUnsignedConstant(value, token);
				return 0;
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerLong.readLongSignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return readerLong.readLongSignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return readerLong.readLongSignedDefaultOptional(token,valueOfOptional);
			}		
		}
		
	}

	private long readLongSigned(int token) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerLong.readLongSigned(token);
				} else {
					//delta
					return readerLong.readLongSignedDelta(token);
				}	
			} else {
				//constant
				return readerLong.readLongSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerLong.readLongSignedCopy(token);
				} else {
					//increment
					return readerLong.readLongSignedIncrement(token);	
				}	
			} else {
				// default
				return readerLong.readLongSignedDefault(token);
			}		
		}
		
	}

	private long readLongUnsignedOptional(int token, long valueOfOptional) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerLong.readLongUnsignedOptional(token,valueOfOptional);
				} else {
					//delta
					return readerLong.readLongUnsignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				return readerLong.readLongSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerLong.readLongUnsignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return readerLong.readLongUnsignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return readerLong.readLongUnsignedDefaultOptional(token,valueOfOptional);
			}		
		}

	}

	private long readLongUnsigned(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerLong.readLongUnsigned(token);
				} else {
					//delta
					return readerLong.readLongUnsignedDelta(token);
				}	
			} else {
				//constant
				return readerLong.readLongUnsignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerLong.readLongUnsignedCopy(token);
				} else {
					//increment
					return readerLong.readLongUnsignedIncrement(token);		
				}	
			} else {
				// default
				return readerLong.readLongUnsignedDefault(token);
			}		
		}
		
	}

	@Override
	public int readInt(int id, int valueOfOptional) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			//not optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) { 
				return readIntegerUnsigned(token);
			} else {
				return readIntegerSigned(token);
			}
		} else {
			//optional
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				return readIntegerUnsignedOptional(token, valueOfOptional);
			} else {
				return readIntegerSignedOptional(token, valueOfOptional);
			}	
		}		
	}

	private int readIntegerSignedOptional(int token, int valueOfOptional) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerSignedOptional(token,valueOfOptional);
				} else {
					//delta
					return readerInteger.readIntegerSignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				//writerInteger.writeIntegerUnsignedConstant(value, token);
				return 0;
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerSignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return readerInteger.readIntegerSignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return readerInteger.readIntegerSignedDefaultOptional(token,valueOfOptional);
			}		
		}
		
	}

	private int readIntegerSigned(int token) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerSigned(token);
				} else {
					//delta
					return readerInteger.readIntegerSignedDelta(token);
				}	
			} else {
				//constant
				return readerInteger.readIntegerSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerSignedCopy(token);
				} else {
					//increment
					return readerInteger.readIntegerSignedIncrement(token);	
				}	
			} else {
				// default
				return readerInteger.readIntegerSignedDefault(token);
			}		
		}
	}

	private int readIntegerUnsignedOptional(int token, int valueOfOptional) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerUnsignedOptional(token,valueOfOptional);
				} else {
					//delta
					return readerInteger.readIntegerUnsignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				//writerInteger.writeIntegerUnsignedConstant(value, token);
				return 0;
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerUnsignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return readerInteger.readIntegerUnsignedIncrementOptional(token,valueOfOptional);	
				}	
			} else {
				// default
				return readerInteger.readIntegerUnsignedDefaultOptional(token,valueOfOptional);
			}		
		}
	
	}

	private int readIntegerUnsigned(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerUnsigned(token);
				} else {
					//delta
					return readerInteger.readIntegerUnsignedDelta(token);
				}	
			} else {
				//constant
				return readerInteger.readIntegerUnsignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerUnsignedCopy(token);
				} else {
					//increment
					return readerInteger.readIntegerUnsignedIncrement(token);
				}	
			} else {
				// default
				return readerInteger.readIntegerUnsignedDefault(token);
			}		
		}
	}

	@Override
	public void readBytes(int id, ByteBuffer target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readBytes(int id, byte[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.ByteArray:
				throw new UnsupportedOperationException();
			case TypeMask.ByteArrayOptional:
				throw new UnsupportedOperationException();
			default://all other types should use their own method.
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public void openGroup(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		int pmapMaxSize = TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK);
		if (pmapMaxSize>0) {
			reader.readPMap(pmapMaxSize);
		}
		
	}

	@Override
	public void closeGroup(int id) {
		//must have same token that was used when opening the group.
		int token = id>=0 ? tokenLookup[id] : id;
		int pmapMaxSize = TokenBuilder.MASK_PMAP_MAX&(token>>TokenBuilder.SHIFT_PMAP_MASK);
		if (pmapMaxSize>0) {
			reader.popPMap();
		}
	}

	public boolean isGroupOpen() {
		return reader.isPMapOpen();
	}

	@Override
	public int readDecimalExponent(int id, int valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0!=(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		int oppExp = (token>>(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL))&TokenBuilder.MASK_OPER_DECIMAL;

		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalExponent(token, valueOfOptional);
		} else {
			return readerDecimal.readDecimalExponentOptional(token, oppExp, valueOfOptional);
		}
	}
	

	@Override
	public long readDecimalMantissa(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		int oppMant = (token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER_DECIMAL;
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalMantissa(token, oppMant, valueOfOptional);
		} else {
			return readerDecimal.readDecimalMantissaOptional(token, valueOfOptional);
		}
	}

	@Override
	public void readChars(int id, Appendable target) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII:
					readTextASCII(token, target);
				break;
			case TypeMask.TextASCIIOptional:
					readTextASCIIOptional(token, target);
				break;
			case TypeMask.TextUTF8:
					readTextUTF8(token,target);
				break;
			case TypeMask.TextUTF8Optional:
					readTextUTF8Optional(token, target);
			    break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8Optional(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(length, target);
				break;
			case OperatorMask.Copy:
				readerChar.readUTF8CopyOptional(token, target);
				break;
			case OperatorMask.Default:
				readerChar.readUTF8DefaultOptional(token, target);
				break;
			case OperatorMask.Delta:
				readerChar.readUTF8DeltaOptional(token, target);
				break;
			case OperatorMask.Tail:
				readerChar.readUTF8TailOptional(token, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextUTF8(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(length, target);
				break;
			case OperatorMask.Copy:
				readerChar.readUTF8Copy(token, target);
				break;
			case OperatorMask.Constant:
				readerChar.readUTF8Constant(token, target);
				break;
			case OperatorMask.Default:
				readerChar.readUTF8Default(token, target);
				break;
			case OperatorMask.Delta:
				readerChar.readUTF8Delta(token, target);
				break;
			case OperatorMask.Tail:
				readerChar.readUTF8Tail(token, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCIIOptional(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			case OperatorMask.Copy:
				readerChar.readASCIICopyOptional(token, target);
				break;
			case OperatorMask.Default:
				readerChar.readASCIIDefaultOptional(token, target);
				break;
			case OperatorMask.Delta:
				readerChar.readASCIIDeltaOptional(token, target);
				break;
			case OperatorMask.Tail:
				readerChar.readASCIITailOptional(token, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readTextASCII(int token, Appendable target) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				reader.readTextASCII(target);
				break;
			case OperatorMask.Copy:
				readerChar.readASCIICopy(token, target);
				break;
			case OperatorMask.Constant:
				readerChar.readASCIIConstant(token, target);
				break;
			case OperatorMask.Default:
				readerChar.readASCIIDefault(token, target);
				break;
			case OperatorMask.Delta:
				readerChar.readASCIIDelta(token, target);
				break;
			case OperatorMask.Tail:
				readerChar.readASCIITail(token, target);
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	@Override
	public int readChars(int id, char[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		switch ((token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE) {
			case TypeMask.TextASCII:
				return readTextASCII(token, target, offset);
			case TypeMask.TextASCIIOptional:
				return readTextASCIIOptional(token, target, offset);
			case TypeMask.TextUTF8:
				return readTextUTF8(token, target, offset);
			case TypeMask.TextUTF8Optional:
				return readTextUTF8Optional(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8Optional(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned()-1;
				reader.readTextUTF8(target,offset,length);
				return length;
			case OperatorMask.Copy:
				return readerChar.readUTF8CopyOptional(token, target, offset);
			case OperatorMask.Default:
				return readerChar.readUTF8DefaultOptional(token, target, offset);
			case OperatorMask.Delta:
				return readerChar.readUTF8DeltaOptional(token, target, offset);
			case OperatorMask.Tail:
				return readerChar.readUTF8TailOptional(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextUTF8(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				int length = reader.readIntegerUnsigned();
				reader.readTextUTF8(target,offset,length);
				return length;
			case OperatorMask.Copy:
				return readerChar.readUTF8Copy(token, target, offset);
			case OperatorMask.Constant:
				return readerChar.readUTF8Constant(token, target, offset);
			case OperatorMask.Default:
				return readerChar.readUTF8Default(token, target, offset);
			case OperatorMask.Delta:
				return readerChar.readUTF8Delta(token, target, offset);
			case OperatorMask.Tail:
				return readerChar.readUTF8Tail(token, target, offset);
			default:
				throw new UnsupportedOperationException();
	}
	}

	private int readTextASCIIOptional(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			case OperatorMask.Copy:
				return readerChar.readASCIICopyOptional(token, target, offset);
			case OperatorMask.Default:
				return readerChar.readASCIIDefaultOptional(token, target, offset);
			case OperatorMask.Delta:
				return readerChar.readASCIIDeltaOptional(token, target, offset);
			case OperatorMask.Tail:
				return readerChar.readASCIITailOptional(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}

	private int readTextASCII(int token, char[] target, int offset) {
		switch ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER) {
			case OperatorMask.None:
				return reader.readTextASCII(target,offset);
			case OperatorMask.Copy:
				return readerChar.readASCIICopy(token, target, offset);
			case OperatorMask.Constant:
				return readerChar.readASCIIConstant(token, target, offset);
			case OperatorMask.Default:
				return readerChar.readASCIIDefault(token, target, offset);
			case OperatorMask.Delta:
				return readerChar.readASCIIDelta(token, target, offset);
			case OperatorMask.Tail:
				return readerChar.readASCIITail(token, target, offset);
			default:
				throw new UnsupportedOperationException();
		}
	}



}
