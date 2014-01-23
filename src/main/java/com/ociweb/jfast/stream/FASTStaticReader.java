package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderChar;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterChar;
import com.ociweb.jfast.field.FieldWriterDecimal;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.FieldWriterLong;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTStaticReader implements FASTReader {


	
	private int templateStackHead = 0;
	private int[] templateStack = new int[100];// //TODO: need max depth?
	
	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	//This is the GLOBAL dictionary
	//When unspecified in the template GLOBAL is the default so these are used.
	private final FieldReaderInteger readerInteger;
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderChar readerChar;
	private final FieldReaderBytes readerBytes;
	
	//template specific dictionaries
	private final int maxTemplates = 10;
	private final FieldWriterInteger[] templateWriterInteger;
	private final FieldWriterLong[] templateWriterLong;
	private final FieldWriterDecimal[] templateWriterDecimal;
	private final FieldWriterChar[] templateWriterChar;
	private final FieldWriterBytes[] templateWriterBytes;
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	
		
	public FASTStaticReader(PrimitiveReader reader, DictionaryFactory dcr) {
		this.reader = reader;
		this.tokenLookup = dcr.getTokenLookup();
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.readerChar = new FieldReaderChar(reader,dcr.charDictionary());
		this.readerBytes = new FieldReaderBytes(reader,dcr.byteDictionary());
		
		this.templateWriterInteger = new FieldWriterInteger[maxTemplates];
		this.templateWriterLong    = new FieldWriterLong[maxTemplates];
		this.templateWriterDecimal = new FieldWriterDecimal[maxTemplates];
		this.templateWriterChar    = new FieldWriterChar[maxTemplates];
		this.templateWriterBytes   = new FieldWriterBytes[maxTemplates];
		
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

	private FieldReaderLong longDictionary(int token) {
		
		if (0==(token&(3<<18))) {
			return readerLong;
		} else {
			//these also take an extra lookup we are optimized for the global above			
			if (0==(token&(2<<18))) {
				int templateId = templateStack[templateStackHead];
				//AppType
				//FASTDynamic MUST know the template and therefore the type.
				//The template id is the first byte inside the group if pmap indicates.
				//that value must be read by unsignedInteger but can be done by open/close group!!
				throw new UnsupportedOperationException();
			} else {
				if (0==(token&(1<<18))) {
					//Template
					throw new UnsupportedOperationException();
				} else {
					//Custom
					throw new UnsupportedOperationException();
				}
			}
		}
	}
	
	private FieldReaderInteger integerDictionary(int token) {
		
		if (0==(token&(3<<18))) {
			return readerInteger;
		} else {
			//these also take an extra lookup we are optimized for the global above			
			if (0==(token&(2<<18))) {
				int templateId = templateStack[templateStackHead];
				//AppType
				//FASTDynamic MUST know the template and therefore the type.
				//The template id is the first byte inside the group if pmap indicates.
				//that value must be read by unsignedInteger but can be done by open/close group!!
				throw new UnsupportedOperationException();
			} else {
				if (0==(token&(1<<18))) {
					//Template
					throw new UnsupportedOperationException();
				} else {
					//Custom
					throw new UnsupportedOperationException();
				}
			}
		}
	}
	
	private FieldReaderDecimal decimalDictionary(int token) {
		
		if (0==(token&(3<<18))) {
			return readerDecimal;
		} else {
			//these also take an extra lookup we are optimized for the global above			
			if (0==(token&(2<<18))) {
				int templateId = templateStack[templateStackHead];
				//AppType
				//FASTDynamic MUST know the template and therefore the type.
				//The template id is the first byte inside the group if pmap indicates.
				//that value must be read by unsignedInteger but can be done by open/close group!!
				throw new UnsupportedOperationException();
			} else {
				if (0==(token&(1<<18))) {
					//Template
					throw new UnsupportedOperationException();
				} else {
					//Custom
					throw new UnsupportedOperationException();
				}
			}
		}
	}
	
	private FieldReaderChar charDictionary(int token) {
		
		if (0==(token&(3<<18))) {
			return readerChar;
		} else {
			//these also take an extra lookup we are optimized for the global above			
			if (0==(token&(2<<18))) {
				int templateId = templateStack[templateStackHead];
				//AppType
				//FASTDynamic MUST know the template and therefore the type.
				//The template id is the first byte inside the group if pmap indicates.
				//that value must be read by unsignedInteger but can be done by open/close group!!
				throw new UnsupportedOperationException();
			} else {
				if (0==(token&(1<<18))) {
					//Template
					throw new UnsupportedOperationException();
				} else {
					//Custom
					throw new UnsupportedOperationException();
				}
			}
		}
	}
	
	private FieldReaderBytes bytesDictionary(int token) {
		
		if (0==(token&(3<<18))) {
			return readerBytes;
		} else {
			//these also take an extra lookup we are optimized for the global above			
			if (0==(token&(2<<18))) {
				int templateId = templateStack[templateStackHead];
				//AppType
				//FASTDynamic MUST know the template and therefore the type.
				//The template id is the first byte inside the group if pmap indicates.
				//that value must be read by unsignedInteger but can be done by open/close group!!
				throw new UnsupportedOperationException();
			} else {
				if (0==(token&(1<<18))) {
					//Template
					throw new UnsupportedOperationException();
				} else {
					//Custom
					throw new UnsupportedOperationException();
				}
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
					return  longDictionary(token).readLongSignedOptional(token,valueOfOptional);
				} else {
					//delta
					return  longDictionary(token).readLongSignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				return  longDictionary(token).readLongSignedConstantOptional(token,valueOfOptional);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  longDictionary(token).readLongSignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return  longDictionary(token).readLongSignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return  longDictionary(token).readLongSignedDefaultOptional(token,valueOfOptional);
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
					return  longDictionary(token).readLongSigned(token);
				} else {
					//delta
					return  longDictionary(token).readLongSignedDelta(token);
				}	
			} else {
				//constant
				return  longDictionary(token).readLongSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  longDictionary(token).readLongSignedCopy(token);
				} else {
					//increment
					return  longDictionary(token).readLongSignedIncrement(token);	
				}	
			} else {
				// default
				return  longDictionary(token).readLongSignedDefault(token);
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
					return  longDictionary(token).readLongUnsignedOptional(token,valueOfOptional);
				} else {
					//delta
					return  longDictionary(token).readLongUnsignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				return  longDictionary(token).readLongUnsignedConstantOptional(token,valueOfOptional);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  longDictionary(token).readLongUnsignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return  longDictionary(token).readLongUnsignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return  longDictionary(token).readLongUnsignedDefaultOptional(token,valueOfOptional);
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
					return  longDictionary(token).readLongUnsigned(token);
				} else {
					//delta
					return  longDictionary(token).readLongUnsignedDelta(token);
				}	
			} else {
				//constant
				return  longDictionary(token).readLongUnsignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  longDictionary(token).readLongUnsignedCopy(token);
				} else {
					//increment
					return  longDictionary(token).readLongUnsignedIncrement(token);		
				}	
			} else {
				// default
				return  longDictionary(token).readLongUnsignedDefault(token);
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
					return integerDictionary(token).readIntegerSignedOptional(token,valueOfOptional);
				} else {
					//delta
					return integerDictionary(token).readIntegerSignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				return integerDictionary(token).readIntegerSignedConstantOptional(token,valueOfOptional);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return integerDictionary(token).readIntegerSignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return integerDictionary(token).readIntegerSignedIncrementOptional(token,valueOfOptional);
				}	
			} else {
				// default
				return integerDictionary(token).readIntegerSignedDefaultOptional(token,valueOfOptional);
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
					return integerDictionary(token).readIntegerSigned(token);
				} else {
					//delta
					return integerDictionary(token).readIntegerSignedDelta(token);
				}	
			} else {
				//constant
				return integerDictionary(token).readIntegerSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return integerDictionary(token).readIntegerSignedCopy(token);
				} else {
					//increment
					return integerDictionary(token).readIntegerSignedIncrement(token);	
				}	
			} else {
				// default
				return integerDictionary(token).readIntegerSignedDefault(token);
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
					return integerDictionary(token).readIntegerUnsignedOptional(token,valueOfOptional);
				} else {
					//delta
					return integerDictionary(token).readIntegerUnsignedDeltaOptional(token,valueOfOptional);
				}	
			} else {
				//constant
				return integerDictionary(token).readIntegerUnsignedConstantOptional(token, valueOfOptional);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return integerDictionary(token).readIntegerUnsignedCopyOptional(token,valueOfOptional);
				} else {
					//increment
					return integerDictionary(token).readIntegerUnsignedIncrementOptional(token,valueOfOptional);	
				}	
			} else {
				// default
				return integerDictionary(token).readIntegerUnsignedDefaultOptional(token,valueOfOptional);
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
					return integerDictionary(token).readIntegerUnsigned(token);
				} else {
					//delta
					return integerDictionary(token).readIntegerUnsignedDelta(token);
				}	
			} else {
				//constant
				return integerDictionary(token).readIntegerUnsignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return integerDictionary(token).readIntegerUnsignedCopy(token);
				} else {
					//increment
					return integerDictionary(token).readIntegerUnsignedIncrement(token);
				}	
			} else {
				// default
				return integerDictionary(token).readIntegerUnsignedDefault(token);
			}		
		}
	}

	@Override
	public int readBytes(int id) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		assert(0==(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {//compiler does all the work.
			return readByteArray(token);
		} else {
			return readByteArrayOptional(token);
		}
	}

	private int readByteArray(int token) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
				//	System.err.println("none");
					return bytesDictionary(token).readBytes(token);
				} else {
					//tail
				//	System.err.println("tail");
					return bytesDictionary(token).readBytesTail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return bytesDictionary(token).readBytesConstant(token);
				} else {
					//delta
				//	System.err.println("delta read");
					return bytesDictionary(token).readBytesDelta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return bytesDictionary(token).readBytesCopy(token);
			} else {
				//default
				//System.err.println("default");
				return bytesDictionary(token).readBytesDefault(token);
			}
		}
	}
	
	private int readByteArrayOptional(int token) {
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
				//	System.err.println("none");
					return bytesDictionary(token).readBytesOptional(token);
				} else {
					//tail
				//	System.err.println("tail");
					return bytesDictionary(token).readBytesTailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return bytesDictionary(token).readBytesConstantOptional(token);
				} else {
					//delta
				//	System.err.println("delta read");
					return bytesDictionary(token).readBytesDeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return bytesDictionary(token).readBytesCopyOptional(token);
			} else {
				//default
				//System.err.println("default");
				return bytesDictionary(token).readBytesDefaultOptional(token);
			}
		}
	}

	
	
	@Override
	public void openGroup(int id) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		int pmapMaxSize = TokenBuilder.extractMaxBytes(token);
		if (pmapMaxSize>0) {
			reader.readPMap(pmapMaxSize);
		}
		
		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
			//always push something on to the stack
			int newTop = (reader.popPMapBit()!=0) ? reader.readIntegerUnsigned() : templateStack[templateStackHead];
			templateStack[templateStackHead++] = newTop;

		}
	}

	@Override
	public void closeGroup(int id) {
		//must have same token that was used when opening the group.
		int token = id>=0 ? tokenLookup[id] : id;
		int pmapMaxSize = TokenBuilder.extractMaxBytes(token);
		if (pmapMaxSize>0) {
			reader.popPMap();
		}
		
		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
			//must always pop because open will always push
			templateStackHead--;
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
			return decimalDictionary(token).readDecimalExponent(token, valueOfOptional);
		} else {
			return decimalDictionary(token).readDecimalExponentOptional(token, oppExp, valueOfOptional);
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
			return decimalDictionary(token).readDecimalMantissa(token, oppMant, valueOfOptional);
		} else {
			return decimalDictionary(token).readDecimalMantissaOptional(token, valueOfOptional);
		}
	}

	@Override
	public int readText(int id) {
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
					return charDictionary(token).readUTF8Optional(token);
				} else {
					//tail
					//System.err.println("tail");
					return charDictionary(token).readUTF8TailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//System.err.println("const");
					return charDictionary(token).readUTF8ConstantOptional(token);
				} else {
					//delta
					//System.err.println("delta");
					return charDictionary(token).readUTF8DeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return charDictionary(token).readUTF8CopyOptional(token);
			} else {
				//default
				//System.err.println("default");
				return charDictionary(token).readUTF8DefaultOptional(token);
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
					return charDictionary(token).readASCII(token);
				} else {
					//tail
					return charDictionary(token).readASCIITail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return charDictionary(token).readASCIIConstant(token);
				} else {
					//delta
					return charDictionary(token).readASCIIDelta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return charDictionary(token).readASCIICopy(token);
			} else {
				//default
				return charDictionary(token).readASCIIDefault(token);
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
					return charDictionary(token).readUTF8(token);
				} else {
					//tail
				//	System.err.println("tail");
					return charDictionary(token).readUTF8Tail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return charDictionary(token).readUTF8Constant(token);
				} else {
					//delta
				//	System.err.println("delta read");
					return charDictionary(token).readUTF8Delta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return charDictionary(token).readUTF8Copy(token);
			} else {
				//default
				//System.err.println("default");
				return charDictionary(token).readUTF8Default(token);
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
					return charDictionary(token).readASCII(token);
				} else {
					//tail
					return charDictionary(token).readASCIITailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return charDictionary(token).readASCIIConstantOptional(token);
				} else {
					//delta
					return charDictionary(token).readASCIIDeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return charDictionary(token).readASCIICopyOptional(token);
			} else {
				//default
				return charDictionary(token).readASCIIDefaultOptional(token);
			}
		}

	}


}
