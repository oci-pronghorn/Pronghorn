package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderChar;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
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
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	
		
	public FASTStaticReader(PrimitiveReader reader, DictionaryFactory dcr, int[] tokenLookup) {
		this.reader = reader;
		this.tokenLookup = tokenLookup;
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.readerChar = new FieldReaderChar(reader,dcr.charDictionary());
		this.readerBytes = new FieldReaderBytes(reader,dcr.byteDictionary());
		
		
		
	}
	
	public void copyDicionaryTo(FASTStaticWriter target) {
		//TODO: implement.
	}
	

	public void reset(DictionaryFactory df) {
		//clear all previous values to unset
		readerInteger.reset(df);
		readerLong.reset(df);
	}

	
	public TextHeap textHeap() {
		return readerChar.textHeap();
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
				readTextASCII(token); //TODO: these nulls are not corect but we do not need the result.
			    break;
		    case TypeMask.TextASCIIOptional:
				readTextASCIIOptional(token);
			    break;
			case TypeMask.TextUTF8:
				readTextUTF8(token);
				break;
			case TypeMask.TextUTF8Optional:
				readTextUTF8Optional(token);
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
				return readerLong.readLongSignedConstantOptional(token,valueOfOptional);
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
				return readerLong.readLongUnsignedConstantOptional(token,valueOfOptional);
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
				return readerInteger.readIntegerSignedConstantOptional(token,valueOfOptional);
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
				return readerInteger.readIntegerUnsignedConstantOptional(token, valueOfOptional);
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
	public int readChars(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii
				//System.err.println("read ascii");
				return readTextASCII(token);
			} else {
				//utf8
				//System.err.println("read utf8");
				return readTextUTF8(token);
			}
		} else {
			if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
				//ascii optional
				//System.err.println("read ascii opp");
				return readTextASCIIOptional(token);
			} else {
				//utf8 optional
				//System.err.println("read utf8 opp");
				return readTextUTF8Optional(token);
			}
		}
	}

	private int readTextUTF8Optional(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					//System.err.println("none");
					return readerChar.readUTF8Optional(token);
				} else {
					//tail
					//System.err.println("tail");
					return readerChar.readUTF8TailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//System.err.println("const");
					return readerChar.readUTF8ConstantOptional(token);
				} else {
					//delta
					//System.err.println("delta");
					return readerChar.readUTF8DeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerChar.readUTF8CopyOptional(token);
			} else {
				//default
				//System.err.println("default");
				return readerChar.readUTF8DefaultOptional(token);
			}
		}
		
	}

	private int readTextASCII(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerChar.readASCII(token);
				} else {
					//tail
					return readerChar.readASCIITail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerChar.readASCIIConstant(token);
				} else {
					//delta
					return readerChar.readASCIIDelta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopy(token);
			} else {
				//default
				return readerChar.readASCIIDefault(token);
			}
		}
		
	}

	private int readTextUTF8(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
				//	System.err.println("none");
					return readerChar.readUTF8(token);
				} else {
					//tail
				//	System.err.println("tail");
					return readerChar.readUTF8Tail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return readerChar.readUTF8Constant(token);
				} else {
					//delta
				//	System.err.println("delta read");
					return readerChar.readUTF8Delta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerChar.readUTF8Copy(token);
			} else {
				//default
				//System.err.println("default");
				return readerChar.readUTF8Default(token);
			}
		}
		
	}

	private int readTextASCIIOptional(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerChar.readASCII(token);
				} else {
					//tail
					return readerChar.readASCIITailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerChar.readASCIIConstantOptional(token);
				} else {
					//delta
					return readerChar.readASCIIDeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopyOptional(token);
			} else {
				//default
				return readerChar.readASCIIDefaultOptional(token);
			}
		}

	}


}
