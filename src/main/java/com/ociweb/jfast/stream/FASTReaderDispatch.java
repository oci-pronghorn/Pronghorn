//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.FieldReaderText;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReaderDispatch{
	
	final PrimitiveReader reader;
	
	private int readFromIdx = -1; 
	
	//This is the GLOBAL dictionary
	//When unspecified in the template GLOBAL is the default so these are used.
	private final FieldReaderInteger readerInteger;
	private final int[] rIntDictionary;
	
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderText readerText;
	private final TextHeap textHeap;
	
	private final FieldReaderBytes readerBytes;
			
    private final int nonTemplatePMapSize;
    private final int[][] dictionaryMembers;
	
	private final DictionaryFactory dictionaryFactory;
	

	private DispatchObserver observer;
	
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	final int maxNestedSeqDepth;
	final int[] sequenceCountStack;
	
	int sequenceCountStackHead = -1;
	int checkSequence;
    int jumpSequence; //Only needs to be set when returning true.
	
	TextHeap charDictionary;
	ByteHeap byteDictionary;

	int activeScriptCursor;
	int activeScriptLimit;
	
	int[] fullScript;

	private final FASTRingBuffer queue;
	private final int queueMASK;
	private final int[] queueBuffer;

	
	public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, 
			                   int nonTemplatePMapSize, int[][] dictionaryMembers, int maxTextLen, 
			                   int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
			                   int maxNestedGroupDepth) {
		this.reader = reader;
		this.dictionaryFactory = dcr;
		this.nonTemplatePMapSize = nonTemplatePMapSize;
		this.dictionaryMembers = dictionaryMembers;
				
		this.charDictionary = dcr.charDictionary(maxTextLen,charGap);
		this.byteDictionary = dcr.byteDictionary(maxVectorLen,bytesGap);
		
		this.maxNestedSeqDepth = maxNestedGroupDepth; 
		this.sequenceCountStack = new int[maxNestedSeqDepth];
		
		this.fullScript = fullScript;
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary(),dcr.integerDictionary());
		this.rIntDictionary = readerInteger.dictionary;
		
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary(),dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, 
													dcr.decimalExponentDictionary(),
													dcr.decimalExponentDictionary(),
				                                    dcr.decimalMantissaDictionary(),
				                                    dcr.decimalMantissaDictionary());
		this.readerText = new FieldReaderText(reader,charDictionary);
		this.textHeap = readerText.textHeap();
		
		this.readerBytes = new FieldReaderBytes(reader,byteDictionary);
		
		this.queue = new FASTRingBuffer((byte)8, //TODO: A, Generate values in template loader
						                (byte)7, 
						                readerText.textHeap());
		this.queueMASK = queue.mask;
		this.queueBuffer = queue.buffer;
		
	}
	
	public FASTRingBuffer ringBuffer() {
		return queue;
	}

	public void reset() {
		
		//System.err.println("total read fields:"+totalReadFields);
	//	totalReadFields = 0;
		
		//clear all previous values to un-set
		dictionaryFactory.reset(rIntDictionary);
		dictionaryFactory.reset(readerLong.dictionary);
		readerDecimal.reset(dictionaryFactory);
		readerText.reset();
		readerBytes.reset();
		sequenceCountStackHead = -1;
		
	}

	
	public TextHeap textHeap() {
		return readerText.textHeap();
	}
	
	public ByteHeap byteHeap() {
		return readerBytes.byteHeap();
	}
	
    //TODO: B, this code generation must take place when loading the catalog binary file.
	
	
	public boolean dispatchReadByTokenGen() {
		switch(activeScriptCursor) {
			 case 0:		
				 	return case0();
			 case 1:
				    assert(gatherReadData(reader,fullScript[1],1));
				    int length = case1();
					if (length==0) {
						    //jumping over sequence (forward) it was skipped (rare case)
							activeScriptCursor = 31;//9+22;
							return false;
					} 		
					sequenceCountStack[++sequenceCountStackHead] = length;
					
			 case 9:
				 	assert(gatherReadData(reader,fullScript[9],9));
				 	return case9();
			 case 30:
				 	assert(gatherReadData(reader,fullScript[30],30));
				 	return case30();
			 default:
   		    		assert(false) : "Unsupported Template";
			    	return false;				 
		}		
	}

	private boolean case0() {
		assert(gatherReadData(reader,fullScript[0],0));
		//write a NonNull constant, no need to check more because this can not be null or dynamic.
		  queue.appendInt2(0x80000000,0x02); //ref and length,  ASCIIConstant 0xa02c0000
		  activeScriptCursor = 1;
		  return false;
	}

	private int case1() {
		   case1reset();
		   
		   //textHeap ID and known fixed length
		   queue.appendInt6(0x80000001,0x03, //ASCIIConstant 0xa02c0001
		                    0x80000002,0x01, //ASCIIConstant 0xa02c0002
		                    0x80000003,0x0d); //ASCIIConstant 0xa02c0003
		   
		   //TODO: B, Code gen, we DO want to use this dictiionary and keep NONE opp operators IFF they are referenced by other fields.

		   queue.appendInt3(reader.readIntegerUnsigned(),
				            reader.readIntegerUnsigned(),
				            reader.readIntegerUnsigned());
				
		   return queue.appendInt1(reader.readIntegerUnsigned());  //readIntegerUnsigned(0xd00c0003)); 

	}

	private void case1reset() {
		
		   readerDecimal.exponent.dictionary[0] = readerDecimal.exponent.init[0];
		   readerDecimal.mantissa.dictionary[0] = readerDecimal.mantissa.init[0];
		   readerDecimal.exponent.dictionary[1] = readerDecimal.exponent.init[1];
		   readerDecimal.mantissa.dictionary[1] = readerDecimal.mantissa.init[1];
		   readerText.heap.reset(4);
		   rIntDictionary[4] = readerInteger.init[4];
		   rIntDictionary[8] = readerInteger.init[8];
		   rIntDictionary[9] = readerInteger.init[9];
		   rIntDictionary[10] = readerInteger.init[10];
		   rIntDictionary[11] = readerInteger.init[11];
		   rIntDictionary[12] = readerInteger.init[12];
	}

	private boolean case9() {
		
		reader.openPMap(nonTemplatePMapSize);
		
		case9b1();
		case9b2();				
		case9c1();				
		case9c2();

		closeGroup(0xc0dc0014);
		activeScriptCursor = 29;
		assert(gatherReadData(reader,fullScript[29],29));
		return checkSequence != 0 && completeSequence(0xc0dc0014); 
	}

	private void case9b2() {
		long h2;
		queue.appendInt6(reader.readIntegerSignedDefault(0 /* default value */),
				         (int) ((h2 = reader.readLongSignedDelta(0x00, 0x00, readerDecimal.mantissa.dictionary)) >>> 32),
				         (int) (h2 & 0xFFFFFFFF),
				         reader.readIntegerUnsignedCopy(0x0a, 0x0a, rIntDictionary),
				         reader.readIntegerSignedDeltaOptional(0x0b, 0x0b, rIntDictionary,constIntAbsent),
				         reader.readIntegerUnsignedDeltaOptional(0x0c, 0x0c, rIntDictionary,constIntAbsent));
	}

	private void case9b1() {
		int heapId;
		int heapIdxLen;
		int value2;
		queue.appendInt8(reader.readIntegerUnsignedCopy(0x04, 0x04, rIntDictionary),
				         reader.readIntegerUnsignedDefaultOptional(1 /*default or absent value */, constIntAbsent),
                         textIdRef(heapId = readerText.readASCIICopy(0xa01c0004, -1), heapIdxLen = textHeap.length2(heapId)),
                         heapIdxLen,
                         (value2 = reader.readIntegerUnsigned()) == 0 ? constIntAbsent : value2 - 1,
                         9, //dictionary1[0x07],//constant?
                         reader.readIntegerUnsignedCopy(0x08, 0x08, rIntDictionary),
                         reader.readIntegerUnsignedIncrement(0x09, 0x09, rIntDictionary)
                        );//not used if null 
	}

	private void case9c1() {
		int heapIdx;
		int heapIdxLen;
		long e21;
		queue.appendInt8(textIdRef(heapIdx = readerText.readASCIIDefault(0x05), heapIdxLen = textHeap.length2(heapIdx)),heapIdxLen,
				reader.readIntegerSignedDefaultOptional(2147483647 /* default or absent value */,constIntAbsent),
				(int) ((e21 = reader.readLongSignedDeltaOptional(0x01, 0x01, readerDecimal.mantissa.dictionary, constLongAbsent)) >>> 32),
				(int) (e21 & 0xFFFFFFFF),
				reader.readIntegerUnsignedDefaultOptional(2147483647, constIntAbsent),
				textIdRef(heapIdx = readerText.readASCIIDefault(0x06), heapIdxLen = textHeap.length2(heapIdx)),heapIdxLen
				);
	}

	private void case9c2() {
		int heapIdx;
		int heapIdxLen;
		queue.appendInt7(//not used if null
		                 textIdRef(heapIdx = readerText.readASCIIDefault(0x07), heapIdxLen = textHeap.length2(heapIdx)),heapIdxLen,
		                 textIdRef(heapIdx = readerText.readASCIIDefault(0x08), heapIdxLen = textHeap.length2(heapIdx)),heapIdxLen,
		                 reader.readIntegerUnsignedDefaultOptional(2147483647 ,constIntAbsent),
		                 textIdRef(heapIdx = readerText.readASCIIDefault(0x09), heapIdxLen = textHeap.length2(heapIdx)),heapIdxLen);//not used if null
	}


	private int textIdRef(int heapId, int length) {
		return heapId<0 ? heapId : queue.writeTextToRingBuffer(heapId, length);
	}

	private boolean case30() {
		//// example of what code generator must do.			
 
		//write a NonNull constant, no need to check more because this can not be null or dynamic.
		queue.appendInt2(0x8000000a,0x5);//ASCIIConstant 0xa02c000a
		queue.appendInt2(0x8000000b,0x0);//ASCIIConstant(0xa02c000b
		queue.appendInt2(0x8000000c,0x0); ///ASCIIConstant(0xa02c000c

		queue.appendInt2(reader.readIntegerUnsigned(),
				         reader.readIntegerUnsigned());
		
		//System.err.println("0x"+Integer.toHexString(c1)+",0x"+Integer.toHexString(textHeap.initLength(c1)));

		int f1 = readerText.readASCII(0xa40c000d,readFromIdx);

		int len5 = textHeap.length2(f1);
		queue.appendInt2(textIdRef(f1, len5),len5);//not used if null//normal read without constant, may need copy
				
		int length2;//be careful in code generation this can have all the same operators
		queue.appendInt1(length2 = reader.readIntegerUnsigned());// readIntegerUnsigned(0xd00c0011));
		if (length2==0) {
		    //jumping over sequence (forward) it was skipped (rare case)
			activeScriptCursor = 46;//36+10;
			return false;
		} else {			
			sequenceCountStack[++sequenceCountStackHead] = length2;
		}		

		reader.openPMap(nonTemplatePMapSize);
		
		queue.appendInt2(0x8000000e,0x0); //ASCIIConstant(0xa02c000e
				
		long b4 = reader.readLongUnsignedOptional(constLongAbsent);
		queue.appendInt3((int)(b4>>>32),
				         (int)(b4&0xFFFFFFFF),
				         reader.readIntegerUnsignedDefaultOptional(1/*default or optional */, constIntAbsent));

		long d4 = reader.readLongUnsigned(0x01, readerLong.dictionary);
		queue.appendInt5((int)(d4>>>32),
				         (int)(d4&0xFFFFFFFF),
				         reader.readIntegerUnsignedDefault(1/*default value*/),
				         reader.readIntegerUnsigned(),
				         rIntDictionary[0x15]);

		closeGroup(0xc0dc0008);
		activeScriptCursor = 45;
		return checkSequence!=0 && completeSequence(0xc0dc0008);

	}
	

	final int constIntAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
	final long constLongAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;

	

		

	
	
//	long totalReadFields = 0;
	
	   //The nested IFs for this short tree are slightly faster than switch 
	   //for more JVM configurations and when switch is faster (eg lots of JVM -XX: args)
	   //it is only slightly faster.
		
	   //For a dramatic speed up of this dispatch code look into code generation of the
	   //script as a series of function calls against the specific FieldReader*.class
	   //This is expected to save 4ns per field on the AMD hardware or a speedup > 12%.
		
		//Yet another idea is to process two tokens together and add a layer of
		//mangled functions that have "pre-coded" scripts. What if we just repeat the same type?
						
	//	totalReadFields++;
		
		//THOUGHTS
		//Build fixed length and put all in ring buffer, consumers can
		//look at leading int to determine what kind of message they have
		//and the script position can be looked up by field id once for their needs.
		//each "mini-message is expected to be very small" and all in cache
	//package protected, unless we find a need to expose it?
	final boolean dispatchReadByToken(FASTRingBuffer outputQueue) {
	
		
		//move everything needed in this tight loop to the stack
		int cursor = activeScriptCursor;
		int limit = activeScriptLimit;
		int[] script = fullScript;
		
//		
		boolean codeGen = false;// true;//cursor!=9 && cursor!=1 && cursor!=30 && cursor!=0;
		//TODO: B, once this code matches the methods used here take it out and move it to the TemplateLoader
		
	//	int zz = cursor;
		
		if (codeGen) {
			//code generation test
			System.err.println(" case "+cursor+":");			
			
		}
		
		try {
		do {
			int token = script[cursor];
			
			if (codeGen) {
				StringBuilder builder = new StringBuilder();
				builder.append("   ");
				TokenBuilder.methodNameRead(token, builder);
				
				
				System.err.println(builder);
				
				
			}
			
			assert(gatherReadData(reader,script[cursor],cursor));
			
			//The trick here is to keep all the conditionals in this method and do the work elsewhere.
			if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
				//0????
				if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
					//00???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = dispatchReadByTokenForInteger(token);//int
					} else {
						long value = dispatchReadByTokenForLong(token);
						outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(value>>>32);
						outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(value&0xFFFFFFFF);//long
					}
				} else {
					//01???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						//int for text					
						
						int heapIdx = dispatchReadByTokenForText(token);
						int heapIdxLen = textHeap.length2(heapIdx);
						queue.appendInt2(textIdRef(heapIdx, heapIdxLen),heapIdxLen);//not used if null
						
					} else {
						//011??
						if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
							//0110? Decimal and DecimalOptional
							if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = readerDecimal.readDecimalExponent(token, -1);
								long readDecimalMantissa = readerDecimal.readDecimalMantissa(token, -1);
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(readDecimalMantissa>>>32);
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(readDecimalMantissa&0xFFFFFFFF);
							} else {
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = readerDecimal.readDecimalExponentOptional(token, -1);
								long readDecimalMantissa = readerDecimal.readDecimalMantissaOptional(token, -1);
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(readDecimalMantissa>>>32);
								outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = (int)(readDecimalMantissa&0xFFFFFFFF);
							}
						} else {
							//0111?
							if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
								//01110 ByteArray
								outputQueue.appendBytes(readByteArray(token), byteDictionary);
							} else {
								//01111 ByteArrayOptional
								outputQueue.appendBytes(readByteArrayOptional(token), byteDictionary);
							}
						}
					}
				}
			} else { 
				//1????
				if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
					//10???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						//100??
						//Group Type, no others defined so no need to keep checking
						if (0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER))) {
							//this is NOT a message/template so the non-template pmapSize is used.			
							if (nonTemplatePMapSize>0) {
								reader.openPMap(nonTemplatePMapSize);
							}
						} else {
							return readGroupClose(token, cursor);	
						}
						
					} else {
						//101??
						
						//Length Type, no others defined so no need to keep checking
						//Only happens once before a node sequence so push it on the count stack
						int length;
						int value = length = readIntegerUnsigned(token);
						outputQueue.buffer[outputQueue.mask&outputQueue.addPos++] = value;
						
						//int oldCursor = cursor;
						cursor = sequenceJump(length, cursor);
					//	System.err.println("jumpDif:"+(cursor-oldCursor));
					}
				} else {
					//11???
					//Dictionary Type, no others defined so no need to keep checking
					if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
						readDictionaryReset2(dictionaryMembers[TokenBuilder.MAX_INSTANCE&token]);					
					} else {
						//OperatorMask.Dictionary_Read_From  0001
						//next read will need to use this index to pull the right initial value.
						//after it is used it must be cleared/reset to -1
						readDictionaryFromField(token);
					}				
				}	
			}
		} while (++cursor<limit);
		
		if (codeGen) {
			System.err.println("delta "+(cursor-activeScriptCursor));
		}
		activeScriptCursor = cursor;
		
//		if (0!=zz) {
//			System.err.println(zz+" xx "+activeScriptCursor);
//		}

		return false;
		} finally {
			if (codeGen) {
				//code generation test
				System.err.println("break;");			
				
			}
		}// */
	}

	private int sequenceJump(int length, int cursor) {
		if (length==0) {
		    //jumping over sequence (forward) it was skipped (rare case)
			cursor += (TokenBuilder.MAX_INSTANCE&fullScript[++cursor])+1;
		} else {			
			//jumpSequence = 0;
			sequenceCountStack[++sequenceCountStackHead] = length;
		}
		return cursor;
	}

	private void readDictionaryFromField(int token) {
		readFromIdx = TokenBuilder.MAX_INSTANCE&token;
	}

	private boolean readGroupClose(int token, int cursor) {
		closeGroup(token);
		//System.err.println("delta "+(cursor-activeScriptCursor));
		activeScriptCursor = cursor;
		return 	checkSequence!=0 && completeSequence(token);
	}

	public void setDispatchObserver(DispatchObserver observer) {
		this.observer=observer;
	}
	
	private boolean gatherReadData(PrimitiveReader reader, int token, int cursor) {

		if (null!=observer) {
			String value = "";
			//totalRead is bytes loaded from stream.
			
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos,token,cursor, value);
		}
		
		return true;
	}
	private boolean gatherReadData(PrimitiveReader reader, int token, int cursor, String value) {

		if (null!=observer) {
			//totalRead is bytes loaded from stream.
			
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos,token,cursor, value);
		}
		
		return true;
	}
	
	boolean gatherReadData(PrimitiveReader reader, String msg) {

		if (null!=observer) {
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos, -1, activeScriptCursor, msg);
		}
		
		return true;
	}

	private void readDictionaryReset2(int[] members) {
		
		boolean genCode = false;
		if (genCode) {
			System.err.println();;
		}
		
		int limit = members.length;
		int m = 0;
		int idx = members[m++]; //assumes that a dictionary always has at lest 1 member
		while (m<limit) {
			assert(idx<0);
			
			if (0==(idx&8)) {
				if (0==(idx&4)) {
					//integer
					while (m<limit && (idx = members[m++])>=0) {
						rIntDictionary[idx] = readerInteger.init[idx];
						if (genCode) {
							System.err.println("rIntDictionary["+idx+"] = readerInteger.init["+idx+"];");
						}
					}
				} else {
					//long
					//System.err.println("long");
					while (m<limit && (idx = members[m++])>=0) {
						readerLong.dictionary[idx] = readerLong.init[idx];
						if (genCode) {
							System.err.println("readerLong.dictionary["+idx+"] = readerLong.init["+idx+"];");
						}
					}
				}
			} else {
				if (0==(idx&4)) {							
					//text
					while (m<limit && (idx = members[m++])>=0) {						
						readerText.heap.reset(idx);
						if (genCode) {
							System.err.println("readerText.heap.reset("+idx+");");
						}	
					}
				} else {
					if (0==(idx&2)) {								
						//decimal
						//System.err.println("decimal");
						while (m<limit && (idx = members[m++])>=0) {
							readerDecimal.exponent.dictionary[idx] = readerDecimal.exponent.init[idx];
							readerDecimal.mantissa.dictionary[idx] = readerDecimal.mantissa.init[idx];
							if (genCode) {
								System.err.println("readerDecimal.exponent.dictionary["+idx+"] = readerDecimal.exponent.init["+idx+"];");
								System.err.println("readerDecimal.mantissa.dictionary["+idx+"] = readerDecimal.mantissa.init["+idx+"];");
							}	
						}
					} else {
						//bytes
						while (m<limit && (idx = members[m++])>=0) {
							readerBytes.reset(idx);
							if (genCode) {
								System.err.println("readerBytes.reset("+idx+");");
							}	
						}
					}
				}
			}	
		}
	}


	private int dispatchReadByTokenForText(int token) {
	//	System.err.println(" CharToken:"+TokenBuilder.tokenToString(token));
		
		//010??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0100?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//01000 TextASCII
				return readTextASCII(token);
			} else {
				//01001 TextASCIIOptional
				return 	readTextASCIIOptional(token);
			}
		} else {
			//0101?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//01010 TextUTF8
				return 	readTextUTF8(token);
			} else {
				//01011 TextUTF8Optional
				return 	readTextUTF8Optional(token);
			}
		}
	}

	private long dispatchReadByTokenForLong(int token) {
		//001??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0010?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00100 LongUnsigned
				return readLongUnsigned(token);
			} else {
				//00101 LongUnsignedOptional
				return readLongUnsignedOptional(token);
			}
		} else {
			//0011?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00110 LongSigned
				return readLongSigned(token);
			} else {
				//00111 LongSignedOptional
				return readLongSignedOptional(token);
			}
		}
	}

	private int dispatchReadByTokenForInteger(int token) {
		//000??
		if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
			//0000?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00000 IntegerUnsigned
				return readIntegerUnsigned(token);
			} else {
				//00001 IntegerUnsignedOptional
				return readIntegerUnsignedOptional(token); 
			}
		} else {
			//0001?
			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
				//00010 IntegerSigned
				return	readIntegerSigned(token);
			} else {
				//00011 IntegerSignedOptional
				return	readIntegerSignedOptional(token);
			}
		}
	}
	
	public long readLong(int token) {
				
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
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongSignedOptional(constAbsent);
				} else {
					//delta
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongSignedDeltaOptional(target, source, readerLong.dictionary, constAbsent);
				}	
			} else {
				//constant
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constConst = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
				
				return readerLong.reader.readLongSignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
					long value = readerLong.reader.readLongSignedCopy(target, source, readerLong.dictionary);
					return (0 == value ? constAbsent: value-1);
				} else {
					//increment
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongSignedIncrementOptional(target, source, readerLong.dictionary, constAbsent);
				}	
			} else {
				// default
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constDefault = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK]==0?constAbsent:readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
				
				return readerLong.reader.readLongSignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					
					return readerLong.reader.readLongSigned(target, readerLong.dictionary);
				} else {
					//delta
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					
					return readerLong.reader.readLongSignedDelta(target, source, readerLong.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					return readerLong.reader.readLongSignedCopy(target, source, readerLong.dictionary);
				} else {
					//increment
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					
					return readerLong.reader.readLongSignedIncrement(target, source, readerLong.dictionary);	
				}	
			} else {
				// default
				long constDefault = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
				
				return readerLong.reader.readLongSignedDefault(constDefault);
			}		
		}
	}

	private long readLongUnsignedOptional(int token) {
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
			//none, constant, delta
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//none, delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//none
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongUnsignedOptional(constAbsent);
				} else {
					//delta
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongUnsignedDeltaOptional(target, source, readerLong.dictionary, constAbsent);
				}	
			} else {
				//constant
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constConst = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
						
				return readerLong.reader.readLongUnsignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
					long value = readerLong.reader.readLongUnsignedCopy(target, source, readerLong.dictionary);
					return (0 == value ? constAbsent: value-1);
				} else {
					//increment
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return readerLong.reader.readLongUnsignedIncrementOptional(target, source, readerLong.dictionary, constAbsent);
				}	
			} else {
				// default
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constDefault = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK]==0?constAbsent:readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
				
				return readerLong.reader.readLongUnsignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					
					return readerLong.reader.readLongUnsigned(target, readerLong.dictionary);
				} else {
					//delta
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					return readerLong.reader.readLongUnsignedDelta(target, source, readerLong.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					return readerLong.reader.readLongUnsignedCopy(target, source, readerLong.dictionary);
				} else {
					//increment
					int target = token&readerLong.MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerLong.MAX_LONG_INSTANCE_MASK : target;
					
					return readerLong.reader.readLongUnsignedIncrement(target, source, readerLong.dictionary);		
				}	
			} else {
				// default
				long constDefault = readerLong.dictionary[token & readerLong.MAX_LONG_INSTANCE_MASK];
				
				return readerLong.reader.readLongUnsignedDefault(constDefault);
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
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerSignedOptional(constAbsent);
				} else {
					//delta
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerSignedDeltaOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
				
				return readerInteger.reader.readIntegerSignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					int value = readerInteger.reader.readIntegerSignedCopy(target, source, rIntDictionary);
					return (0 == value ? constAbsent: (value>0 ? value-1 : value));
				} else {
					//increment
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerSignedIncrementOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				// default
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constDefault = rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK]==0?constAbsent:rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
						
				return readerInteger.reader.readIntegerSignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					
					//no need to set initValueFlags for field that can never be null
					return reader.readIntegerSigned();
				} else {
					//delta
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerSignedDelta(target, source, rIntDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerSignedCopy(target, source, rIntDictionary);
				} else {
					//increment
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerSignedIncrement(target, source, rIntDictionary);	
				}	
			} else {
				// default
				int constDefault = rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];	
				
				return readerInteger.reader.readIntegerSignedDefault(constDefault);
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
					assert(readFromIdx<0);
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					int value = readerInteger.reader.readIntegerUnsigned();
					return value==0 ? constAbsent : value-1;
				} else {
					//delta
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));		
					
					return readerInteger.reader.readIntegerUnsignedDeltaOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
				
				return readerInteger.reader.readIntegerUnsignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					int value = readerInteger.reader.readIntegerUnsignedCopy(target, source, rIntDictionary);
					
					return (0 == value ? TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token)): value-1);
				} else {
					//increment
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerUnsignedIncrementOptional(target, source, rIntDictionary, constAbsent);	
				}	
			} else {
				// default
				int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int t = rIntDictionary[source];
				int constDefault = t == 0 ? constAbsent : t-1; 
				
				return readerInteger.reader.readIntegerUnsignedDefaultOptional(constDefault, constAbsent);
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
					assert(readFromIdx<0);
					return reader.readIntegerUnsigned();
				} else {
					//delta
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerUnsignedDelta(target, source, rIntDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rIntDictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
							
					return readerInteger.reader.readIntegerUnsignedCopy(target, source, rIntDictionary);
				} else {
					//increment
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerUnsignedIncrement(target, source, rIntDictionary);
				}	
			} else {
				// default
				int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
				int constDefault = rIntDictionary[source];
				
				return readerInteger.reader.readIntegerUnsignedDefault(constDefault);
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
		
		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
			//none constant delta tail 
			if (0==(token&(6<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//none tail
				if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
					//none
					return readerBytes.readBytes(token, readFromIdx);
				} else {
					//tail
					return readerBytes.readBytesTail(token, readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerBytes.readBytesConstant(token, readFromIdx);
				} else {
					//delta
					return readerBytes.readBytesDelta(token, readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerBytes.readBytesCopy(token, readFromIdx);
			} else {
				//default
				return readerBytes.readBytesDefault(token, readFromIdx);
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
					return readerBytes.readBytesOptional(token, readFromIdx);
				} else {
					//tail
					return readerBytes.readBytesTailOptional(token, readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerBytes.readBytesConstantOptional(token, readFromIdx);
				} else {
					//delta
					return readerBytes.readBytesDeltaOptional(token, readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerBytes.readBytesCopyOptional(token, readFromIdx);
			} else {
				//default
				return readerBytes.readBytesDefaultOptional(token, readFromIdx);
			}
		}
	}
	
	public void openGroup(int token, int pmapSize) {

		assert(token<0);
		assert(0==(token&(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER)));
			
		if (pmapSize>0) {
			reader.openPMap(pmapSize);
		}
	}


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
			return false;
		}
		
		
		//each sequence will need to repeat the pmap but we only need to push
		//and pop the stack when the sequence is first encountered.
		//if count is zero we can pop it off but not until then.
		
		if (--sequenceCountStack[sequenceCountStackHead]<1) {
			//this group is a sequence so pop it off the stack.
			//System.err.println("finished seq");
			--sequenceCountStackHead;
			//finished this sequence so leave pointer where it is
			jumpSequence= 0;
		} else {
			//do this sequence again so move pointer back
			jumpSequence = (TokenBuilder.MAX_INSTANCE&token);
		}
		return true;
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
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalExponent(token, readFromIdx);
		} else {
			return readerDecimal.readDecimalExponentOptional(token, readFromIdx);
		}
	}
	

	public long readDecimalMantissa(int token) {
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			return readerDecimal.readDecimalMantissa(token, readFromIdx);
		} else {
			return readerDecimal.readDecimalMantissaOptional(token, readFromIdx);
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
					return readerText.readUTF8Optional(token,readFromIdx);
				} else {
					//tail
					//System.err.println("tail");
					return readerText.readUTF8TailOptional(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//System.err.println("const");
					return readerText.readUTF8ConstantOptional(token,readFromIdx);
				} else {
					//delta
					//System.err.println("delta");
					return readerText.readUTF8DeltaOptional(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerText.readUTF8CopyOptional(token,readFromIdx);
			} else {
				//default
				//System.err.println("default");
				return readerText.readUTF8DefaultOptional(token,readFromIdx);
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
					return readerText.readASCII(token,readFromIdx);
				} else {
					//tail
					return readerText.readASCIITail(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					return readerText.readASCIIConstant(token,readFromIdx);
				} else {
					//delta
					return readerText.readASCIIDelta(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				return readerText.readASCIICopy(token,readFromIdx);
			} else {
				//default
				int target = readerText.MAX_TEXT_INSTANCE_MASK&token;
				
				return readerText.readASCIIDefault(target);
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
					return readerText.readUTF8(token,readFromIdx);
				} else {
					//tail
				//	System.err.println("tail");
					return readerText.readUTF8Tail(token,readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					return readerText.readUTF8Constant(token,readFromIdx);
				} else {
					//delta
				//	System.err.println("delta read");
					return readerText.readUTF8Delta(token,readFromIdx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				return readerText.readUTF8Copy(token,readFromIdx);
			} else {
				//default
				//System.err.println("default");
				return readerText.readUTF8Default(token,readFromIdx);
			}
		}
		
	}

	private int readTextASCIIOptional(int token) {
		
		if (0==(token&((4|2|1)<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
				//none
				return readerText.readASCII(token,readFromIdx);
			} else {
				//tail
				return readerText.readASCIITailOptional(token,readFromIdx);
			}
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					return readerText.readASCIIDeltaOptional(token,readFromIdx);
				} else {
					return readerText.readASCIIConstantOptional(token,readFromIdx);
				}		
			} else {
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					return readerText.readASCIICopyOptional(token,readFromIdx);
				} else {
					//for ASCII we don't need special behavior for optional
					int target = readerText.MAX_TEXT_INSTANCE_MASK&token;
					//CODE:
				//	System.err.println("readerText.readASCIIDefault("+target+")");
					return readerText.readASCIIDefault(target);
				}
				
			}
		}
		
	}



}
