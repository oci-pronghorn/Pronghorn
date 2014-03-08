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
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReaderDispatch{
	
	private final PrimitiveReader reader;
	
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
		
	
    private final int nonTemplatePMapSize;
	
	private final DictionaryFactory dictionaryFactory;
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	int maxNestedSeqDepth = 64;
	int[] sequenceCountStack = new int[maxNestedSeqDepth];
	int sequenceCountStackHead = -1;
	int checkSequence;
	
	
		
	public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, 
			                   int maxTemplates, int nonTemplatePMapSize, int maxFieldId) {
		this.reader = reader;
		this.dictionaryFactory = dcr;
		this.nonTemplatePMapSize = nonTemplatePMapSize;
		
		assert(maxFieldId>1);
		//TODO: not sure these are a good idea, a simple list would be better.
//		System.err.println("max field id for array:"+maxFieldId);
		this.intLookup = new int[maxFieldId+1];
		this.longLookup = new long[maxFieldId+1];
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, dcr.decimalExponentDictionary(),dcr.decimalMantissaDictionary());
		this.readerChar = new FieldReaderChar(reader,dcr.charDictionary());
		this.readerBytes = new FieldReaderBytes(reader,dcr.byteDictionary());
	}

	public void reset() {
		//clear all previous values to un-set
		readerInteger.reset(dictionaryFactory);
		readerLong.reset(dictionaryFactory);
		readerDecimal.reset(dictionaryFactory);
		readerChar.reset();
		readerBytes.reset();
		sequenceCountStackHead = -1;
		
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
			//0????
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//00???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					dispatchReadByToken000(id, token);
				} else {
					dispatchReadByToken001(id, token);
				}
			} else {
				//01???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					dispatchReadByToken010(id, token);
				} else {
					dispatchReadByToken011(id, token);
				}
			}
			return false;
		} else {
			//pause node for more work processing will return false 
			return dispatchReadByToken1(id, token);	
		}
	}
	
	private boolean dispatchReadByToken1(int id, int token) {
		//1????
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			//10???
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				//100??
				//Group Type, no others defined so no need to keep checking
				readGroupCommand(token);
				return 	checkSequence!=0 && !completeSequence(token);
				
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
				//Only happens once before a node sequence so push it on the count stack
				int length = readIntegerUnsigned(token);
				sequenceCountStack[++sequenceCountStackHead] = length;
				//S//ystem.err.println("set new length:"+length);
				intLookup[id] = length; 
				
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
		return false;
	}

	private void readDictionaryCommand(int token) {

		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//OperatorMask.Dictionary_Reset      0000
			//clear all previous values to un-set for this dictionary only.
			
			//TODO: look up list of token ids for this dictionary.
			
			readerInteger.reset(dictionaryFactory);
			readerLong.reset(dictionaryFactory);
			readerDecimal.reset(dictionaryFactory);
			readerChar.reset();
			readerBytes.reset(); 
		} else {
			//OperatorMask.Dictionary_Read_From  0001
			//next read will need to use this index to pull the right initial value.
			//after it is used it must be cleared/reset to -1
			readFromIdx = TokenBuilder.MAX_INSTANCE&token;
		}
	}
	
	private void readGroupCommand(int token) {
		if (0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER))) {
			//this is NOT a message/template so the non-template pmapSize is used.			
			openGroup(token, nonTemplatePMapSize);
		} else {
			closeGroup(token);
		}
	}

	private void dispatchReadByToken0(int id, int token) {
		//0????
		if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
			//00???
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				dispatchReadByToken000(id, token);
			} else {
				dispatchReadByToken001(id, token);
			}
		} else {
			//01???
			if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
				dispatchReadByToken010(id, token);
			} else {
				dispatchReadByToken011(id, token);
			}
		}
	}

	private void dispatchReadByToken011(int id, int token) {
		//011??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0110? Decimal and DecimalOptional
			
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				intLookup[id] =	readerDecimal.readDecimalExponent(token);
			} else {
				intLookup[id] =	readerDecimal.readDecimalExponentOptional(token);
			}
		
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				longLookup[id] =  readerDecimal.readDecimalMantissa(token);
			} else {
				longLookup[id] =  readerDecimal.readDecimalMantissaOptional(token);
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

	private void dispatchReadByToken001(int id, int token) {
		//001??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0010?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00100 LongUnsigned
				longLookup[id] = readLongUnsigned(token);
			} else {
				//00101 LongUnsignedOptional
				longLookup[id] = readLongUnsignedOptional(token);
			}
		} else {
			//0011?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00110 LongSigned
				longLookup[id] = readLongSigned(token);
			} else {
				//00111 LongSignedOptional
				longLookup[id] = readLongSignedOptional(token);
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
				intLookup[id] =	readIntegerUnsignedOptional(token); 
			}
		} else {
			//0001?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00010 IntegerSigned
				intLookup[id] =	readIntegerSigned(token);
			} else {
				//00011 IntegerSignedOptional
				intLookup[id] =	readIntegerSignedOptional(token);
			}
		}
	}
	
	public long readLong(int token, long valueOfOptional) {
				
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
				return readLongUnsignedOptional(token);
			} else {
				return readLongSignedOptional(token);
			}	
		}
		
	}

	private long readLongSignedOptional(int token) {
		readerLong.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return  readerLong.readLongSignedOptional(token );
				} else {
					//delta
					return  readerLong.readLongSignedDeltaOptional(token );
				}	
			} else {
				//constant
				return  readerLong.readLongSignedConstantOptional(token );
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  readerLong.readLongSignedCopyOptional(token );
				} else {
					//increment
					return  readerLong.readLongSignedIncrementOptional(token );
				}	
			} else {
				// default
				return  readerLong.readLongSignedDefaultOptional(token );
			}		
		}
		
	}

	private long readLongSigned(int token) {
		readerLong.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return  readerLong.readLongSigned(token);
				} else {
					//delta
					return  readerLong.readLongSignedDelta(token);
				}	
			} else {
				//constant
				return  readerLong.readLongSignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  readerLong.readLongSignedCopy(token);
				} else {
					//increment
					return  readerLong.readLongSignedIncrement(token);	
				}	
			} else {
				// default
				return  readerLong.readLongSignedDefault(token);
			}		
		}
	}

	private long readLongUnsignedOptional(int token) {
		readerLong.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return  readerLong.readLongUnsignedOptional(token);
				} else {
					//delta
					return  readerLong.readLongUnsignedDeltaOptional(token );
				}	
			} else {
				//constant
				return  readerLong.readLongUnsignedConstantOptional(token );
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  readerLong.readLongUnsignedCopyOptional(token );
				} else {
					//increment
					return  readerLong.readLongUnsignedIncrementOptional(token );
				}	
			} else {
				// default
				return  readerLong.readLongUnsignedDefaultOptional(token );
			}		
		}

	}

	private long readLongUnsigned(int token) {
		readerLong.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return  readerLong.readLongUnsigned(token);
				} else {
					//delta
					return  readerLong.readLongUnsignedDelta(token);
				}	
			} else {
				//constant
				return  readerLong.readLongUnsignedConstant(token);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return  readerLong.readLongUnsignedCopy(token);
				} else {
					//increment
					return  readerLong.readLongUnsignedIncrement(token);		
				}	
			} else {
				// default
				return  readerLong.readLongUnsignedDefault(token);
			}		
		}
		
	}

	public int readInt(int token) {
		
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
				return readIntegerUnsignedOptional(token);
			} else {
				return readIntegerSignedOptional(token);
			}	
		}		
	}

	private int readIntegerSignedOptional(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerSignedOptional(token,readFromIdx);
				} else {
					//delta
					return readerInteger.readIntegerSignedDeltaOptional(token,readFromIdx);
				}	
			} else {
				//constant
				return readerInteger.readIntegerSignedConstantOptional(token,readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerSignedCopyOptional(token,readFromIdx);
				} else {
					//increment
					return readerInteger.readIntegerSignedIncrementOptional(token,readFromIdx);
				}	
			} else {
				// default
				return readerInteger.readIntegerSignedDefaultOptional(token,readFromIdx);
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
					return readerInteger.readIntegerSigned(token,readFromIdx);
				} else {
					//delta
					return readerInteger.readIntegerSignedDelta(token,readFromIdx);
				}	
			} else {
				//constant
				return readerInteger.readIntegerSignedConstant(token,readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerSignedCopy(token,readFromIdx);
				} else {
					//increment
					return readerInteger.readIntegerSignedIncrement(token,readFromIdx);	
				}	
			} else {
				// default
				return readerInteger.readIntegerSignedDefault(token,readFromIdx);
			}		
		}
	}

	private int readIntegerUnsignedOptional(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerInteger.readIntegerUnsignedOptional(token,readFromIdx);
				} else {
					//delta
					return readerInteger.readIntegerUnsignedDeltaOptional(token,readFromIdx);
				}	
			} else {
				//constant
				return readerInteger.readIntegerUnsignedConstantOptional(token,readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerUnsignedCopyOptional(token,readFromIdx);
				} else {
					//increment
					return readerInteger.readIntegerUnsignedIncrementOptional(token,readFromIdx);	
				}	
			} else {
				// default
				return readerInteger.readIntegerUnsignedDefaultOptional(token,readFromIdx);
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
					return readerInteger.readIntegerUnsigned(token,readFromIdx);
				} else {
					//delta
					return readerInteger.readIntegerUnsignedDelta(token,readFromIdx);
				}	
			} else {
				//constant
				return readerInteger.readIntegerUnsignedConstant(token,readFromIdx);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					return readerInteger.readIntegerUnsignedCopy(token,readFromIdx);
				} else {
					//increment
					return readerInteger.readIntegerUnsignedIncrement(token,readFromIdx);
				}	
			} else {
				// default
				return readerInteger.readIntegerUnsignedDefault(token,readFromIdx);
			}		
		}
	}

	public int readBytes(int token) {
				
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
		readerBytes.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerBytes.readBytes(token);
				} else {
					//tail
					return readerBytes.readBytesTail(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerBytes.readBytesConstant(token);
				} else {
					//delta
					return readerBytes.readBytesDelta(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerBytes.readBytesCopy(token);
			} else {
				//default
				return readerBytes.readBytesDefault(token);
			}
		}
	}
	
	private int readByteArrayOptional(int token) {
		readerBytes.setReadFrom(readFromIdx);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
	//				System.err.println("none o");
					return readerBytes.readBytesOptional(token);
				} else {
					//tail
	//				System.err.println("tail o");
					return readerBytes.readBytesTailOptional(token);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
	//				System.err.println("const o");
					return readerBytes.readBytesConstantOptional(token);
				} else {
					//delta
	//				System.err.println("delta read o");
					return readerBytes.readBytesDeltaOptional(token);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
	//			System.err.println("copy o");
				return readerBytes.readBytesCopyOptional(token);
			} else {
				//default
	//			System.err.println("default 0");
				return readerBytes.readBytesDefaultOptional(token);
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
	

	
	
	public void openGroup(int token, int pmapSize) {

		assert(token<0);
		assert(0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
			
		if (pmapSize>0) {
			reader.openPMap(pmapSize);
		}
	
	//	beginSequence(token);
			
		
//		if (0!=(token&(OperatorMask.Group_Bit_Templ<<TokenBuilder.SHIFT_OPER))) { //TODO:pull from operator!
//			//always push something on to the stack
//			int newTop = (reader.popPMapBit()!=0) ? reader.readIntegerUnsigned() : templateStack[templateStackHead];
//			templateStack[templateStackHead++] = newTop;
//
//		}
	}

//	//called after open group.?? or just near.
//	public boolean beginSequence(int token) {
//		//We repeat the open for each sequence but we do not want to push on
//		//to the seq stack. Upon jump back the token will mask out the seq bit.
//		if (
////			 0==(token&((8|4)<<TokenBuilder.SHIFT_TYPE)) &&
//	//		 0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)) &&
//				
//			 0!=(token&(OperatorMask.Group_Bit_Seq<<TokenBuilder.SHIFT_OPER))) {
//			//System.err.println("starting new sequence ");
//			return true;//started new sequence
//		}
//		return false;//no sequence to start
//	}


	/**
	 * Returns true if there is no sequence in play or if the active sequence can be closed.
	 * Once a sequence is closed the reader should move to the next point in the sequence. 
	 * 
	 * @param token
	 * @return
	 */
	public boolean completeSequence(int token) {

		checkSequence = 0;//reset for next time
		
		if (sequenceCountStackHead<=0) {
			//no sequence to worry about or not the right time
			return true;
		}
		
		
		//each sequence will need to repeat the pmap but we only need to push
		//and pop the stack when the sequence is first encountered.
		//if count is zero we can pop it off but not until then.
		
		if (--sequenceCountStack[sequenceCountStackHead]<1 
			//not needed if only called on sequence.
				//&&	0!=(token&(OperatorMask.Group_Bit_Seq<<TokenBuilder.SHIFT_OPER))
			) {
			//this group is a sequence so pop it off the stack.
			//System.err.println("finished seq");
			--sequenceCountStackHead;
			//this sequence (the active one) has now completed
			return true;
		}
		//System.err.println("Incomplete seq with count:"+sequenceCountStack[sequenceCountStackHead]);
		return false;
	}
	
	public void closeGroup(int token) {
		
		assert(token<0);
		assert(0!=(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
		
		if (0!=(token&(OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER))) {
			reader.closePMap();
		}
		
		checkSequence = (token&(OperatorMask.Group_Bit_Seq<<TokenBuilder.SHIFT_OPER));
		
	}

	public int readDecimalExponent(int token) {
		readerDecimal.setReadFrom(readFromIdx);
				
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalExponent(token);
		} else {
			return readerDecimal.readDecimalExponentOptional(token);
		}
	}
	

	public long readDecimalMantissa(int token) {
		readerDecimal.setReadFrom(readFromIdx);
				
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalMantissa(token);
		} else {
			return readerDecimal.readDecimalMantissaOptional(token);
		}
	}

	public int readText(int token) {
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
					return readerChar.readUTF8Optional(token,readFromIdx);
				} else {
					//tail
					//System.err.println("tail");
					return readerChar.readUTF8TailOptional(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//System.err.println("const");
					return readerChar.readUTF8ConstantOptional(token,readFromIdx);
				} else {
					//delta
					//System.err.println("delta");
					return readerChar.readUTF8DeltaOptional(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerChar.readUTF8CopyOptional(token,readFromIdx);
			} else {
				//default
				//System.err.println("default");
				return readerChar.readUTF8DefaultOptional(token,readFromIdx);
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
					return readerChar.readASCII(token,readFromIdx);
				} else {
					//tail
					return readerChar.readASCIITail(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerChar.readASCIIConstant(token,readFromIdx);
				} else {
					//delta
					return readerChar.readASCIIDelta(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopy(token,readFromIdx);
			} else {
				//default
				return readerChar.readASCIIDefault(token,readFromIdx);
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
					return readerChar.readUTF8(token,readFromIdx);
				} else {
					//tail
				//	System.err.println("tail");
					return readerChar.readUTF8Tail(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return readerChar.readUTF8Constant(token,readFromIdx);
				} else {
					//delta
				//	System.err.println("delta read");
					return readerChar.readUTF8Delta(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerChar.readUTF8Copy(token,readFromIdx);
			} else {
				//default
				//System.err.println("default");
				return readerChar.readUTF8Default(token,readFromIdx);
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
					return readerChar.readASCII(token,readFromIdx);
				} else {
					//tail
					return readerChar.readASCIITailOptional(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerChar.readASCIIConstantOptional(token,readFromIdx);
				} else {
					//delta
					return readerChar.readASCIIDeltaOptional(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerChar.readASCIICopyOptional(token,readFromIdx);
			} else {
				//default
				return readerChar.readASCIIDefaultOptional(token,readFromIdx);
			}
		}

	}

	public boolean isEOF() {
		return reader.isEOF();
	}




}
