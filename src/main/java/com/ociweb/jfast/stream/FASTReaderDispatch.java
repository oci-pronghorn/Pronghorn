//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
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
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReaderDispatch{

	private int templateStackHead = 0;
	private final int[] templateStack;
	
	private final PrimitiveReader reader;
	private final int[] tokenLookup; //array of tokens as field id locations
	
	private final int[] intLookup; //key is field id
	private final long[] longLookup; //key is field id
	
	private int readFromIdx = -1;
		
	//This is the GLOBAL dictionary
	//When unspecified in the template GLOBAL is the default so these are used.
	private final FieldReaderInteger readerInteger;
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderChar readerChar;
	private final FieldReaderBytes readerBytes;
	
	//template specific dictionaries
	private final int maxTemplates;
	private final FieldWriterInteger[] templateWriterInteger;
	private final FieldWriterLong[] templateWriterLong;
	private final FieldWriterDecimal[] templateWriterDecimal;
	private final FieldWriterChar[] templateWriterChar;
	private final FieldWriterBytes[] templateWriterBytes;
	
	
	//TODO: need these per field? still working on this
	private int integerUnsignedOptionalValue=0;
	private int integerSignedOptionalValue=0;
	private int longUnsignedOptionalValue=0;
	private int longSignedOptionalValue=0;
	private int decimalExponentOptionalValue=0;
	private long decimalMantissaOptionalValue=0;
	
	private final DictionaryFactory dictionaryFactory;
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	
		
	public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, int maxTemplates, int[] tokenLookup) {
		this.reader = reader;
		this.tokenLookup = tokenLookup;
		this.dictionaryFactory = dcr;
		
		this.intLookup = new int[this.tokenLookup.length];
		this.longLookup = new long[this.tokenLookup.length];
		
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
		
		this.templateStack = new int[maxTemplates];
		this.maxTemplates = maxTemplates;
	}

	public void reset() {
		//clear all previous values to un-set
		readerInteger.reset(dictionaryFactory);
		readerLong.reset(dictionaryFactory);
		readerDecimal.reset(dictionaryFactory);
		readerChar.reset();
		readerBytes.reset();
		
	}

	
	public TextHeap textHeap() {
		return readerChar.textHeap();
	}
	
	public ByteHeap byteHeap() {
		return readerBytes.byteHeap();
	}
	
	int lastInt(int id) {
		return intLookup[id];
	}
	
	long lastLong(int id) {
		return longLookup[id];
	}
	
	public void dispatchReadPrefix(byte[] target) {
		reader.readByteData(target, 0, target.length);
	}
	
	//package protected, unless we find a need to expose it?
	boolean dispatchReadByToken(int id, int token) {
	
		if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
			dispatchReadByToken0(id, token);
			return true;
		} else {
			//pause node for more work processing will return false 
			return dispatchReadByToken1(id, token);
		}
	}

	//TODO: re-evaluate bit pmap write look for bulk write solution.
	
	
	private boolean dispatchReadByToken1(int id, int token) {
		//1????
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			//10???
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				//100??
				//Group Type, no others defined so no need to keep checking
				return readGroupCommand(token);
				
				
//				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
//					//1000?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//10000 GroupSimple
//					} else {
//						//10001 GroupTemplated
//					}
//				} else {
//					//1001?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//10010
//					} else {
//						//10011
//					}
//				}
				
			} else {
				//101??
				//Length Type, no others defined so no need to keep checking
				//TODO: this only happens on first pass of script
				//Every group should count passes, if seq must go back to the value?
				
				intLookup[id] =	readIntegerUnsigned(token);
				
//				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
//					//1010?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//10100
//					} else {
//						//10101
//					}
//				} else {
//					//1011?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//10110
//					} else {
//						//10111
//					}
//				}
				
			}
		} else {
			//11???
			//Dictionary Type, no others defined so no need to keep checking
			readDictionaryCommand(token);
			
//			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
//				//110??
//				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
//					//1100?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//11000
//					} else {
//						//11001
//					}
//				} else {
//					//1101?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//11010
//					} else {
//						//11011
//					}
//				}
//			} else {
//				//111??
//				if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
//					//1110?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//11100
//					} else {
//						//11101
//					}
//				} else {
//					//1111?
//					if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
//						//11110
//					} else {
//						//11111
//					}
//				}
//			}
		}
		return true;
	}

	private void readDictionaryCommand(int token) {

		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//OperatorMask.Dictionary_Reset      0000
			//clear all previous values to un-set		
			integerDictionary(token).reset(dictionaryFactory);
			longDictionary(token).reset(dictionaryFactory);
			decimalDictionary(token).reset(dictionaryFactory);
			charDictionary(token).reset();
			bytesDictionary(token).reset(); 
		} else {
			//OperatorMask.Dictionary_Read_From  0001
			//next read will need to use this index to pull the right initial value.
			//after it is used it must be cleared/reset to -1
			readFromIdx = TokenBuilder.extractCount(token);
		}
	}

	private boolean readGroupCommand(int token) {
		if (0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER))) {
			openGroup(token);
			//TODO: if this is a sequence the count needs to be pushed on a stack?
			
			return true;
		} else {
			//TODO: if we are closing a sequence then we must have picked up the
			//length and we will NOT read it again. so we need a flag for that.
			//
			
			return closeGroup(token);
		}
	}

	private void dispatchReadByToken0(int id, int token) {
		//0????
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			dispatchReadByToken00(id, token);
		} else {
			dispatchReadByToken01(id, token);
		}
	}

	private void dispatchReadByToken01(int id, int token) {
		//01???
		if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
			dispatchReadByToken010(id, token);
		} else {
			dispatchReadByToken011(id, token);
		}
	}

	private void dispatchReadByToken011(int id, int token) {
		//011??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0110? Decimal and DecimalOptional
			
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				intLookup[id] =	decimalDictionary(token).readDecimalExponent(token);
			} else {
				intLookup[id] =	decimalDictionary(token).readDecimalExponentOptional(token, decimalExponentOptionalValue);
			}
		
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				longLookup[id] =  decimalDictionary(token).readDecimalMantissa(token);
			} else {
				longLookup[id] =  decimalDictionary(token).readDecimalMantissaOptional(token, decimalMantissaOptionalValue);
			}
			
		} else {
			//0111?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//01110 ByteArray
				intLookup[id] =	readByteArray(token);
			} else {
				//01111 ByteArrayOptional
				intLookup[id] =	readByteArrayOptional(token);
			}
		}
	}

	private void dispatchReadByToken010(int id, int token) {
		//010??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0100?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//01000 TextASCII
				intLookup[id] =	readTextASCII(token);
			} else {
				//01001 TextASCIIOptional
				intLookup[id] =	readTextASCIIOptional(token);
			}
		} else {
			//0101?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//01010 TextUTF8
				intLookup[id] =	readTextUTF8(token);
			} else {
				//01011 TextUTF8Optional
				intLookup[id] =	readTextUTF8Optional(token);
			}
		}
	}

	private void dispatchReadByToken00(int id, int token) {
		//00???
		if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
			dispatchReadByToken000(id, token);
		} else {
			dispatchReadByToken001(id, token);
		}
	}

	private void dispatchReadByToken001(int id, int token) {
		//001??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0010?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00100 LongUnsigned
				longLookup[id] = readLongUnsigned(token);
			} else {
				//00101 LongUnsignedOptional
				longLookup[id] = readLongUnsignedOptional(token, longUnsignedOptionalValue);
			}
		} else {
			//0011?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00110 LongSigned
				longLookup[id] = readLongSigned(token);
			} else {
				//00111 LongSignedOptional
				longLookup[id] = readLongSignedOptional(token, longSignedOptionalValue);
			}
		}
	}

	private void dispatchReadByToken000(int id, int token) {
		//000??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0000?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00000 IntegerUnsigned
				intLookup[id] =	readIntegerUnsigned(token);
			} else {
				//00001 IntegerUnsignedOptional
				intLookup[id] =	readIntegerUnsignedOptional(token,integerUnsignedOptionalValue); 
			}
		} else {
			//0001?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00010 IntegerSigned
				intLookup[id] =	readIntegerSigned(token);
			} else {
				//00011 IntegerSignedOptional
				intLookup[id] =	readIntegerSignedOptional(token, integerSignedOptionalValue);
			}
		}
	}
	
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
		
		FieldReaderLong result = (0==(token&(3<<18)))?readerLong:longDictionarySpecial(token);
		//before returning dictionary prep previous value if needed.
		if (readFromIdx>=0) {
			result.copy(readFromIdx, TokenBuilder.extractCount(token));
			readFromIdx=-1;
		}
		return result;
		
	}

	private FieldReaderLong longDictionarySpecial(int token) {
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
	
	private FieldReaderInteger integerDictionary(int token) {
		
		FieldReaderInteger result = (0==(token&(3<<18)) ? readerInteger : 
									intDictionarySpecial(token));
		
		//before returning dictionary prep previous value if needed.
		if (readFromIdx>=0) {
			result.copy(readFromIdx, TokenBuilder.extractCount(token));
			readFromIdx=-1;
		}
		return result;
	}

	private FieldReaderInteger intDictionarySpecial(int token) {
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
	
	private FieldReaderDecimal decimalDictionary(int token) {
		
		FieldReaderDecimal result = (0==(token&(3<<18))) ? readerDecimal : decimalDictionarySpecial(token);

		//before returning dictionary prep previous value if needed.
		if (readFromIdx>=0) {
			int writeToIdx = TokenBuilder.extractCount(token);
			result.copyExponent(readFromIdx,writeToIdx);
			result.copyMantissa(readFromIdx,writeToIdx);
			readFromIdx=-1;
		}
		return result;
		
	}

	private FieldReaderDecimal decimalDictionarySpecial(int token) {
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
	
	private FieldReaderChar charDictionary(int token) {
		
		FieldReaderChar result = (0==(token&(3<<18)))?readerChar:charDictionarySpecial(token);
		
		//before returning dictionary prep previous value if needed.
		if (readFromIdx>=0) {
			//TODO: not an efficient approach but it will do for the first release
			result.textHeap().copy(readFromIdx, TokenBuilder.extractCount(token));
			readFromIdx=-1;
		}
		return result;
		
	}

	private FieldReaderChar charDictionarySpecial(int token) {
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
	
	private FieldReaderBytes bytesDictionary(int token) {
		
		FieldReaderBytes result = (0==(token&(3<<18)))?readerBytes:bytesDictionarySpecial(token);
		//before returning dictionary prep previous value if needed.
		if (readFromIdx>=0) {
			//TODO: not an efficient approach but it will do for the first release
			result.byteHeap().copy(readFromIdx, TokenBuilder.extractCount(token));
			readFromIdx=-1;
		}
		return result;
				
	}

	private FieldReaderBytes bytesDictionarySpecial(int token) {
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

	public int readBytes(int id) {
		
		int token = id>=0 ? tokenLookup[id] : id;
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE)));
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE)));
		
	//	System.out.println("reading "+TokenBuilder.tokenToString(token));
		
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
					return bytesDictionary(token).readBytes(token);
				} else {
					//tail
					return bytesDictionary(token).readBytesTail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return bytesDictionary(token).readBytesConstant(token);
				} else {
					//delta
					return bytesDictionary(token).readBytesDelta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return bytesDictionary(token).readBytesCopy(token);
			} else {
				//default
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
	//				System.err.println("none o");
					return bytesDictionary(token).readBytesOptional(token);
				} else {
					//tail
	//				System.err.println("tail o");
					return bytesDictionary(token).readBytesTailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
	//				System.err.println("const o");
					return bytesDictionary(token).readBytesConstantOptional(token);
				} else {
					//delta
	//				System.err.println("delta read o");
					return bytesDictionary(token).readBytesDeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
	//			System.err.println("copy o");
				return bytesDictionary(token).readBytesCopyOptional(token);
			} else {
				//default
	//			System.err.println("default 0");
				return bytesDictionary(token).readBytesDefaultOptional(token);
			}
		}
	}
	
	public int openMessage(int pmapMaxSize) {
		
		reader.openPMap(pmapMaxSize);
		//return template id or unknown
		return (0!=reader.popPMapBit()) ? reader.readIntegerUnsigned() : -1;//template Id

	}
	
	public void closeMessage() {
		reader.closePMap();
	}
	
	public void openGroup(int token) {

		assert(token<0);
		assert(0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		//assert(0==(token&(OperatorMask.Group_Bit_Templ<<TokenBuilder.SHIFT_OPER)));
		
		
		int pmapMaxSize = TokenBuilder.extractCount(token);
		if (pmapMaxSize>0) {
			reader.openPMap(pmapMaxSize);
		}
		
		
		
//		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) { //TODO:pull from operator!
//			//always push something on to the stack
//			int newTop = (reader.popPMapBit()!=0) ? reader.readIntegerUnsigned() : templateStack[templateStackHead];
//			templateStack[templateStackHead++] = newTop;
//
//		}
	}

	public boolean closeGroup(int token) {
		
		assert(token<0);
		assert(0!=(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		
		int pmapMaxSize = TokenBuilder.extractCount(token);
		if (pmapMaxSize>0) {
			reader.closePMap();
		}
		
		//TODO:pull from operator!
//		if (TokenBuilder.extractType(token)==TypeMask.GroupTemplated) {
//			//must always pop because open will always push
//			templateStackHead--;
//		}
		
		//is end of message or end of sequence must return notification.
		//return true to continue and false to stop for processing working.
		return (0==(token&( (OperatorMask.Group_Bit_Seq|
				              OperatorMask.Group_Bit_Msg) <<TokenBuilder.SHIFT_OPER)));
		
	}

	public int readDecimalExponent(int id, int valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return decimalDictionary(token).readDecimalExponent(token);
		} else {
			return decimalDictionary(token).readDecimalExponentOptional(token, valueOfOptional);
		}
	}
	

	public long readDecimalMantissa(int id, long valueOfOptional) {
		int token = id>=0 ? tokenLookup[id] : id;
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return decimalDictionary(token).readDecimalMantissa(token);
		} else {
			return decimalDictionary(token).readDecimalMantissaOptional(token, valueOfOptional);
		}
	}

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

	public boolean isEOF() {
		return reader.isEOF();
	}




}
