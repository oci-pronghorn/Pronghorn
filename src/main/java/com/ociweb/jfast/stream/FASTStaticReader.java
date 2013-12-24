package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.DictionaryFactory;
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
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
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
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
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
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii
				readTextASCII(token, target);
			} else {
				//utf8
				readTextUTF8(token,target);
			}
		} else {
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii optional
				readTextASCIIOptional(token, target);
			} else {
				//utf8 optional
				readTextUTF8Optional(token, target);
			}
		}
	}

	private void readTextUTF8Optional(int token, Appendable target) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					int length = reader.readIntegerUnsigned()-1;
					reader.readTextUTF8(length, target);
				} else {
					//tail
					readerChar.readUTF8TailOptional(token, target);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					
				} else {
					//delta
					readerChar.readUTF8DeltaOptional(token, target);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				readerChar.readUTF8CopyOptional(token, target);
			} else {
				//default
				readerChar.readUTF8DefaultOptional(token, target);
			}
		}
		
	}

	private void readTextUTF8(int token, Appendable target) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					int length = reader.readIntegerUnsigned();
					reader.readTextUTF8(length, target);
				} else {
					//tail
					readerChar.readUTF8Tail(token, target);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					readerChar.readUTF8Constant(token, target);
				} else {
					//delta
					readerChar.readUTF8Delta(token, target);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				readerChar.readUTF8Copy(token, target);
			} else {
				//default
				readerChar.readUTF8Default(token, target);
			}
		}
	
	}

	private void readTextASCIIOptional(int token, Appendable target) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					reader.readTextASCII(target);
				} else {
					//tail
					readerChar.readASCIITailOptional(token, target);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					
				} else {
					//delta
					readerChar.readASCIIDeltaOptional(token, target);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				readerChar.readASCIICopyOptional(token, target);
			} else {
				//default
				readerChar.readASCIIDefaultOptional(token, target);
			}
		}
		
	}

	private void readTextASCII(int token, Appendable target) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					reader.readTextASCII(target);
				} else {
					//tail
					readerChar.readASCIITail(token, target);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					readerChar.readASCIIConstant(token, target);
				} else {
					//delta
					readerChar.readASCIIDelta(token, target);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				readerChar.readASCIICopy(token, target);
			} else {
				//default
				readerChar.readASCIIDefault(token, target);
			}
		}
		
	}

	@Override
	public int readChars(int id, char[] target, int offset) {
		int token = id>=0 ? tokenLookup[id] : id;
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii
				return readTextASCII(token, target, offset);
			} else {
				//utf8
				return readTextUTF8(token, target, offset);
			}
		} else {
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii optional
				return readTextASCIIOptional(token, target, offset);
			} else {
				//utf8 optional
				return readTextUTF8Optional(token, target, offset);
			}
		}
	}

	private int readTextUTF8Optional(int token, char[] target, int offset) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					int length = reader.readIntegerUnsigned()-1;
					reader.readTextUTF8(target,offset,length);
					return length;
				} else {
					//tail
					return readerChar.readUTF8TailOptional(token, target, offset);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					return -1;//TODO: undone
				} else {
					//delta
					return readerChar.readUTF8DeltaOptional(token, target, offset);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				return readerChar.readUTF8CopyOptional(token, target, offset);
			} else {
				//default
				return readerChar.readUTF8DefaultOptional(token, target, offset);
			}
		}

	}

	private int readTextUTF8(int token, char[] target, int offset) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					int length = reader.readIntegerUnsigned();
					reader.readTextUTF8(target,offset,length);
					return length;
				} else {
					//tail
					return readerChar.readUTF8Tail(token, target, offset);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					return readerChar.readUTF8Constant(token, target, offset);
				} else {
					//delta
					return readerChar.readUTF8Delta(token, target, offset);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				return readerChar.readUTF8Copy(token, target, offset);
			} else {
				//default
				return readerChar.readUTF8Default(token, target, offset);
			}
		}
		
	}

	private int readTextASCIIOptional(int token, char[] target, int offset) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					return reader.readTextASCII(target,offset);
				} else {
					//tail
					return readerChar.readASCIITailOptional(token, target, offset);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					return -1;//TODO: undone
					
				} else {
					//delta
					return readerChar.readASCIIDeltaOptional(token, target, offset);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopyOptional(token, target, offset);
			} else {
				//default
				return readerChar.readASCIIDefaultOptional(token, target, offset);
			}
		}

	}

	private int readTextASCII(int token, char[] target, int offset) {
		
		if (0==(token&(1<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.MASK_TYPE))) {
					//none
					return reader.readTextASCII(target,offset);
				} else {
					//tail
					return readerChar.readASCIITail(token, target, offset);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.MASK_TYPE))) {
					//constant
					return readerChar.readASCIIConstant(token, target, offset);
				} else {
					//delta
					return readerChar.readASCIIDelta(token, target, offset);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.MASK_TYPE))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopy(token, target, offset);
			} else {
				//default
				return readerChar.readASCIIDefault(token, target, offset);
			}
		}
		
	}

}
