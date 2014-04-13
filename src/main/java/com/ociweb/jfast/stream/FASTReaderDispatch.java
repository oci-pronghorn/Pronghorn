//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
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
	protected final int MAX_INT_INSTANCE_MASK;
	protected final int[] rIntDictionary;
	protected final int[] rIntInit;
	
	protected final int MAX_LONG_INSTANCE_MASK;
	protected final long[] rLongDictionary;
	protected final long[] rLongInit;
		
	protected final int DECIMAL_MAX_INT_INSTANCE_MASK;
	protected final int[] expDictionary;
	protected final int[] expInit;
	
	protected final int DECIMAL_MAX_LONG_INSTANCE_MASK;
	protected final long[] mantDictionary;
	protected final long[] mantInit;
		
	protected  final FieldReaderText readerText;	
	protected  final FieldReaderBytes readerBytes;
			
	protected  final int nonTemplatePMapSize;
	protected  final int[][] dictionaryMembers;
	
	protected  final DictionaryFactory dictionaryFactory;	

	protected  DispatchObserver observer;
	
	
	//constant fields are always the same or missing but never anything else.
	//         manditory constant does not use pmap and has constant injected at destnation never xmit
	//         optional constant does use the pmap 1 (use initial const) 0 (not present)
	//
	//default fields can be the default or overridden this one time with a new value.

	protected final int maxNestedSeqDepth;
	protected final int[] sequenceCountStack;
	
	int sequenceCountStackHead = -1;
	int checkSequence;
    int jumpSequence; //Only needs to be set when returning true.
	
	TextHeap charDictionary;
	ByteHeap byteDictionary;

	int activeScriptCursor;
	int activeScriptLimit;
	
	int[] fullScript;

	protected  final FASTRingBuffer queue;

	
	public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, 
			                  int nonTemplatePMapSize, int[][] dictionaryMembers, int maxTextLen, 
			                  int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
			                  int maxNestedGroupDepth, int primaryRingBits, int textRingBits) {
		this.reader = reader;
		this.dictionaryFactory = dcr;
		this.nonTemplatePMapSize = nonTemplatePMapSize;
		this.dictionaryMembers = dictionaryMembers;
				
		this.charDictionary = dcr.charDictionary(maxTextLen,charGap);
		this.byteDictionary = dcr.byteDictionary(maxVectorLen,bytesGap);
		
		this.maxNestedSeqDepth = maxNestedGroupDepth; 
		this.sequenceCountStack = new int[maxNestedSeqDepth];
		
		this.fullScript = fullScript;
		
		this.rIntDictionary = dcr.integerDictionary();
		this.rIntInit = dcr.integerDictionary();
		assert(rIntDictionary.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(rIntDictionary.length));
		assert(rIntDictionary.length==rIntInit.length);
		this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length-1));
		
		this.rLongDictionary = dcr.longDictionary();
		this.rLongInit = dcr.longDictionary();
		assert(rLongDictionary.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(rLongDictionary.length));
		assert(rLongDictionary.length==rLongInit.length);
		
		this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length-1));
		
		this.expDictionary = dcr.decimalExponentDictionary();
		this.expInit = dcr.decimalExponentDictionary();
		this.mantDictionary = dcr.decimalMantissaDictionary();
		this.mantInit = dcr.decimalMantissaDictionary();
				
		assert(expDictionary.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(expDictionary.length));
		assert(expDictionary.length==expInit.length);
		
		assert(mantDictionary.length<TokenBuilder.MAX_INSTANCE);
		assert(TokenBuilder.isPowerOfTwo(mantDictionary.length));
		assert(mantDictionary.length==mantInit.length);

		this.DECIMAL_MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (expDictionary.length-1));		
		this.DECIMAL_MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (mantDictionary.length-1));
		
		this.readerText = new FieldReaderText(reader, charDictionary);
		
		this.readerBytes = new FieldReaderBytes(reader,byteDictionary);
		
		this.queue = new FASTRingBuffer((byte)primaryRingBits, (byte)textRingBits, readerText.textHeap());
		
	}
	
	public FASTRingBuffer ringBuffer() {
		return queue;
	}

	public void reset() {
		
		//System.err.println("total read fields:"+totalReadFields);
	//	totalReadFields = 0;
		
		//clear all previous values to un-set
		dictionaryFactory.reset(rIntDictionary);
		dictionaryFactory.reset(rLongDictionary);
		dictionaryFactory.reset(expDictionary,mantDictionary);
		if (null!=readerText.heap) {
			readerText.heap.reset();		
		}
		readerBytes.reset();
		sequenceCountStackHead = -1;
		
	}

	
	public TextHeap textHeap() {
		return readerText.textHeap();
	}
	
	public ByteHeap byteHeap() {
		return readerBytes.byteHeap();
	}
	


	
	
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
	boolean dispatchReadByToken() {
	
		
		//move everything needed in this tight loop to the stack
		int cursor = activeScriptCursor;
		int limit = activeScriptLimit;
		int[] script = fullScript;
		
//		
		boolean codeGen = false;// true;//cursor!=9 && cursor!=1 && cursor!=30 && cursor!=0;
		//TODO: T, once this code matches the methods used here take it out and move it to the TemplateLoader
		
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
			
			assert(gatherReadData(reader, cursor));
			
			//The trick here is to keep all the conditionals in this method and do the work elsewhere.
			if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
				//0????
				if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
					//00???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						queue.appendInt1( dispatchReadByTokenForInteger(token));//int
					} else {
						long value = dispatchReadByTokenForLong(token);
						queue.appendInt1( (int)(value>>>32));
						queue.appendInt1( (int)(value&0xFFFFFFFF));//long
					}
				} else {
					//01???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						//int for text					
						
						int heapIdx = dispatchReadByTokenForText(token);
						int heapIdxLen = textHeap().length2(heapIdx);
						queue.appendInt2(heapIdx<0 ? heapIdx : queue.writeTextToRingBuffer(heapIdx, heapIdxLen),heapIdxLen);//not used if null
						
					} else {
						//011??
						if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
							//0110? Decimal and DecimalOptional
							if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
								int result1;
								
								//oppExp
								if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
									//none, constant, delta
									if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
										//none, delta
										if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
											//none
											//no need to set initValueFlags for field that can never be null
											result1 = reader.readIntegerSigned();
										} else {
											//delta
											int target1 = token&MAX_INT_INSTANCE_MASK;
											int source3 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target1;
											
											result1 = reader.readIntegerSignedDelta(target1, source3, expDictionary);
										}	
									} else {
										//constant
										//always return this required value.
										result1 = expDictionary[token & MAX_INT_INSTANCE_MASK];
									}
									
								} else {
									//copy, default, increment
									if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
										//copy, increment
										if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
											//copy
											int target2 = token&MAX_INT_INSTANCE_MASK;
											int source2 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target2;
											
											result1 = reader.readIntegerSignedCopy(target2, source2, expDictionary);
										} else {
											//increment
											int target3 = token&MAX_INT_INSTANCE_MASK;
											int source1 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target3;
											
											result1 = reader.readIntegerSignedIncrement(target3, source1, expDictionary);
										}	
									} else {
										// default
										int constDefault1 = expDictionary[token & MAX_INT_INSTANCE_MASK];	
										
										result1 = reader.readIntegerSignedDefault(constDefault1);
									}		
								}
								queue.appendInt1( result1);
								long result;
								if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
									//none, constant, delta
									if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
										//none, delta
										if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
											//none
											int target = token&MAX_LONG_INSTANCE_MASK;
											
											result = reader.readLongSigned(target, mantDictionary);
										} else {
											//delta
											int target = token&MAX_LONG_INSTANCE_MASK;
											int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
											
											
											result = reader.readLongSignedDelta(target, source, mantDictionary);
										}	
									} else {
										//constant
										//always return this required value.
										result = mantDictionary[token & MAX_LONG_INSTANCE_MASK];
									}
									
								} else {
									//copy, default, increment
									if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
										//copy, increment
										if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
											//copy
											int target = token&MAX_LONG_INSTANCE_MASK;
											int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
											
											result = reader.readLongSignedCopy(target, source, mantDictionary);
										} else {
											//increment
											int target = token&MAX_LONG_INSTANCE_MASK;
											int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
											
											
											result = reader.readLongSignedIncrement(target, source, mantDictionary);
										}	
									} else {
										// default
										long constDefault = mantDictionary[token & MAX_LONG_INSTANCE_MASK];
										
										result = reader.readLongSignedDefault(constDefault);
									}		
								}
								long readDecimalMantissa = result;
								queue.appendInt1( (int)(readDecimalMantissa>>>32));
								queue.appendInt1( (int)(readDecimalMantissa&0xFFFFFFFF));
							} else {
								int result1;
								//oppExp
										if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
											//none, constant, delta
											if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
												//none, delta
												if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
													//none
													int constAbsent1 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
													
													result1 = reader.readIntegerSignedOptional(constAbsent1);
												} else {
													//delta
													int target3 = token&MAX_INT_INSTANCE_MASK;
													int source2 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target3;
													int constAbsent2 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
													
													result1 = reader.readIntegerSignedDeltaOptional(target3, source2, expDictionary, constAbsent2);
												}	
											} else {
												//constant
												int constAbsent6 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
												int constConst1 = expDictionary[token & MAX_INT_INSTANCE_MASK];
												
												result1 = reader.readIntegerSignedConstantOptional(constAbsent6, constConst1);
											}
											
										} else {
											//copy, default, increment
											if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
												//copy, increment
												if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
													//copy
													int target2 = token&MAX_INT_INSTANCE_MASK;
													int source1 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target2;
													int constAbsent3 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
													
													int value1 = reader.readIntegerSignedCopy(target2, source1, expDictionary);
													result1 = (0 == value1 ? constAbsent3: (value1>0 ? value1-1 : value1));
												} else {
													//increment
													int target1 = token&MAX_INT_INSTANCE_MASK;
													int source3 = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target1;
													int constAbsent4 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
													
													result1 = reader.readIntegerSignedIncrementOptional(target1, source3, expDictionary, constAbsent4);
												}	
											} else {
												// default
												int constAbsent5 = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
												int constDefault1 = expDictionary[token & MAX_INT_INSTANCE_MASK]==0?constAbsent5:expDictionary[token & MAX_INT_INSTANCE_MASK];
														
												result1 = reader.readIntegerSignedDefaultOptional(constDefault1, constAbsent5);
											}		
										}
										queue.appendInt1( result1);
								long result;
								//oppMaint
										if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
											//none, constant, delta
											if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
												//none, delta
												if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
													//none
													long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
													
													result = reader.readLongSignedOptional(constAbsent);
												} else {
													//delta
													int target = token&MAX_LONG_INSTANCE_MASK;
													int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
													long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
													
													result = reader.readLongSignedDeltaOptional(target, source, mantDictionary, constAbsent);
												}	
											} else {
												//constant
												long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
												long constConst = mantDictionary[token & MAX_LONG_INSTANCE_MASK];
												
												result = reader.readLongSignedConstantOptional(constAbsent, constConst);
											}
											
										} else {
											//copy, default, increment
											if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
												//copy, increment
												if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
													//copy
													int target = token&MAX_LONG_INSTANCE_MASK;
													int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
													long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
															
													long value = reader.readLongSignedCopy(target, source, mantDictionary);
													result = (0 == value ? constAbsent: value-1);
												} else {
													//increment
													int target = token&MAX_LONG_INSTANCE_MASK;
													int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
													long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
													
													result = reader.readLongSignedIncrementOptional(target, source, mantDictionary, constAbsent);
												}	
											} else {
												// default
												long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
												long constDefault = mantDictionary[token & MAX_LONG_INSTANCE_MASK]==0?constAbsent:mantDictionary[token & MAX_LONG_INSTANCE_MASK];
												
												result = reader.readLongSignedDefaultOptional(constDefault, constAbsent);
											}		
										}
								long readDecimalMantissa = result;
								queue.appendInt1( (int)(readDecimalMantissa>>>32));
								queue.appendInt1( (int)(readDecimalMantissa&0xFFFFFFFF));
							}
						} else {
							//0111?
							if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
								//01110 ByteArray
								queue.appendBytes(readByteArray(token), byteDictionary);
							} else {
								//01111 ByteArrayOptional
								queue.appendBytes(readByteArrayOptional(token), byteDictionary);
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
						queue.appendInt1( value);
						
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
		return 	checkSequence!=0 && completeSequence((TokenBuilder.MAX_INSTANCE&token));
	}

	public void setDispatchObserver(DispatchObserver observer) {
		this.observer=observer;
	}
	
	protected boolean gatherReadData(PrimitiveReader reader, int cursor) {

		int token = fullScript[cursor];
		
		if (null!=observer) {
			String value = "";
			//totalRead is bytes loaded from stream.
			
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos,token,cursor, value);
		}
		
		return true;
	}
	protected boolean gatherReadData(PrimitiveReader reader, int cursor, String value) {

		int token = fullScript[cursor];
		
		if (null!=observer) {
			//totalRead is bytes loaded from stream.
			
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos,token,cursor, value);
		}
		
		return true;
	}
	
	protected boolean gatherReadData(PrimitiveReader reader, String msg) {

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
						rIntDictionary[idx] = rIntInit[idx];
						if (genCode) {
							System.err.println("rIntDictionary["+idx+"] = readerInteger.init["+idx+"];");
						}
					}
				} else {
					//long
					//System.err.println("long");
					while (m<limit && (idx = members[m++])>=0) {
						rLongDictionary[idx] = rLongInit[idx];
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
							expDictionary[idx] = expInit[idx];
							mantDictionary[idx] = mantInit[idx];
							if (genCode) {
								System.err.println("exponent.dictionary["+idx+"] = exponent.init["+idx+"];");
								System.err.println("mantissa.dictionary["+idx+"] = mantissa.init["+idx+"];");
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
					
					return reader.readLongSignedOptional(constAbsent);
				} else {
					//delta
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return reader.readLongSignedDeltaOptional(target, source, rLongDictionary, constAbsent);
				}	
			} else {
				//constant
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
				
				return reader.readLongSignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
					long value = reader.readLongSignedCopy(target, source, rLongDictionary);
					return (0 == value ? constAbsent: value-1);
				} else {
					//increment
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return reader.readLongSignedIncrementOptional(target, source, rLongDictionary, constAbsent);
				}	
			} else {
				// default
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK]==0?constAbsent:rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
				
				return reader.readLongSignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&MAX_LONG_INSTANCE_MASK;
					
					return reader.readLongSigned(target, rLongDictionary);
				} else {
					//delta
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					
					return reader.readLongSignedDelta(target, source, rLongDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					return reader.readLongSignedCopy(target, source, rLongDictionary);
				} else {
					//increment
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					
					return reader.readLongSignedIncrement(target, source, rLongDictionary);	
				}	
			} else {
				// default
				long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
				
				return reader.readLongSignedDefault(constDefault);
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
					
					return reader.readLongUnsignedOptional(constAbsent);
				} else {
					//delta
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return reader.readLongUnsignedDeltaOptional(target, source, rLongDictionary, constAbsent);
				}	
			} else {
				//constant
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
						
				return reader.readLongUnsignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							
					long value = reader.readLongUnsignedCopy(target, source, rLongDictionary);
					return (0 == value ? constAbsent: value-1);
				} else {
					//increment
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
					
					return reader.readLongUnsignedIncrementOptional(target, source, rLongDictionary, constAbsent);
				}	
			} else {
				// default
				long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
				long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK]==0?constAbsent:rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
				
				return reader.readLongUnsignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&MAX_LONG_INSTANCE_MASK;
					
					return reader.readLongUnsigned(target, rLongDictionary);
				} else {
					//delta
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					return reader.readLongUnsignedDelta(target, source, rLongDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					return reader.readLongUnsignedCopy(target, source, rLongDictionary);
				} else {
					//increment
					int target = token&MAX_LONG_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_LONG_INSTANCE_MASK : target;
					
					return reader.readLongUnsignedIncrement(target, source, rLongDictionary);		
				}	
			} else {
				// default
				long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
				
				return reader.readLongUnsignedDefault(constDefault);
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
					
					return reader.readIntegerSignedOptional(constAbsent);
				} else {
					//delta
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return reader.readIntegerSignedDeltaOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
				
				return reader.readIntegerSignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					int value = reader.readIntegerSignedCopy(target, source, rIntDictionary);
					return (0 == value ? constAbsent: (value>0 ? value-1 : value));
				} else {
					//increment
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return reader.readIntegerSignedIncrementOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				// default
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK]==0?constAbsent:rIntDictionary[token & MAX_INT_INSTANCE_MASK];
						
				return reader.readIntegerSignedDefaultOptional(constDefault, constAbsent);
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
					int target = token&MAX_INT_INSTANCE_MASK;
					
					//no need to set initValueFlags for field that can never be null
					return reader.readIntegerSigned();
				} else {
					//delta
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					return reader.readIntegerSignedDelta(target, source, rIntDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rIntDictionary[token & MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					return reader.readIntegerSignedCopy(target, source, rIntDictionary);
				} else {
					//increment
					int target = token&MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					return reader.readIntegerSignedIncrement(target, source, rIntDictionary);	
				}	
			} else {
				// default
				int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];	
				
				return reader.readIntegerSignedDefault(constDefault);
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
					
					int value = reader.readIntegerUnsigned();
					return value==0 ? constAbsent : value-1;
				} else {
					//delta
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));		
					
					return reader.readIntegerUnsignedDeltaOptional(target, source, rIntDictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
				
				return reader.readIntegerUnsignedConstantOptional(constAbsent, constConst);
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					int value = reader.readIntegerUnsignedCopy(target, source, rIntDictionary);
					
					return (0 == value ? TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token)): value-1);
				} else {
					//increment
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return reader.readIntegerUnsignedIncrementOptional(target, source, rIntDictionary, constAbsent);	
				}	
			} else {
				// default
				int target = token & MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int t = rIntDictionary[source];
				int constDefault = t == 0 ? constAbsent : t-1; 
				
				return reader.readIntegerUnsignedDefaultOptional(constDefault, constAbsent);
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
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					return reader.readIntegerUnsignedDelta(target, source, rIntDictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return rIntDictionary[token & MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
							
					return reader.readIntegerUnsignedCopy(target, source, rIntDictionary);
				} else {
					//increment
					int target = token & MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
					
					return reader.readIntegerUnsignedIncrement(target, source, rIntDictionary);
				}	
			} else {
				// default
				int target = token & MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&MAX_INT_INSTANCE_MASK : target;
				int constDefault = rIntDictionary[source];
				
				return reader.readIntegerUnsignedDefault(constDefault);
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
	 * @return
	 */
	public boolean completeSequence(int backvalue) {
		
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
			jumpSequence = backvalue;
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

	//TODO: A, Optional absent null is not implemented yet for Decimal type.
	public int readDecimalExponent(int token) {
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			int result;
			
			//oppExp
			if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
				//none, constant, delta
				if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//none, delta
					if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//none
						//no need to set initValueFlags for field that can never be null
						result = reader.readIntegerSigned();
					} else {
						//delta
						int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
						
						result = reader.readIntegerSignedDelta(target, source, expDictionary);
					}	
				} else {
					//constant
					//always return this required value.
					result = expDictionary[token & DECIMAL_MAX_INT_INSTANCE_MASK];
				}
				
			} else {
				//copy, default, increment
				if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
					//copy, increment
					if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//copy
						int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
						
						result = reader.readIntegerSignedCopy(target, source, expDictionary);
					} else {
						//increment
						int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
						
						result = reader.readIntegerSignedIncrement(target, source, expDictionary);
					}	
				} else {
					// default
					int constDefault = expDictionary[token & DECIMAL_MAX_INT_INSTANCE_MASK];	
					
					result = reader.readIntegerSignedDefault(constDefault);
				}		
			}
			return result;
		} else {
			int result;
			//oppExp
					if (0==(token&(1<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
						//none, constant, delta
						if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//none, delta
							if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
								//none
								int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
								
								result = reader.readIntegerSignedOptional(constAbsent);
							} else {
								//delta
								int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
								int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
								
								result = reader.readIntegerSignedDeltaOptional(target, source, expDictionary, constAbsent);
							}	
						} else {
							//constant
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							int constConst = expDictionary[token & DECIMAL_MAX_INT_INSTANCE_MASK];
							
							result = reader.readIntegerSignedConstantOptional(constAbsent, constConst);
						}
						
					} else {
						//copy, default, increment
						if (0==(token&(2<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
							//copy, increment
							if (0==(token&(4<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
								//copy
								int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
								int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
								
								int value = reader.readIntegerSignedCopy(target, source, expDictionary);
								result = (0 == value ? constAbsent: (value>0 ? value-1 : value));
							} else {
								//increment
								int target = token&DECIMAL_MAX_INT_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_INT_INSTANCE_MASK : target;
								int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
								
								result = reader.readIntegerSignedIncrementOptional(target, source, expDictionary, constAbsent);
							}	
						} else {
							// default
							int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
							int constDefault = expDictionary[token & DECIMAL_MAX_INT_INSTANCE_MASK]==0?constAbsent:expDictionary[token & DECIMAL_MAX_INT_INSTANCE_MASK];
									
							result = reader.readIntegerSignedDefaultOptional(constDefault, constAbsent);
						}		
					}
					return result;
		}
	}
	

	public long readDecimalMantissa(int token) {
		
		assert(0==(token&(2<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(4<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		assert(0!=(token&(8<<TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
		
		if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
			long result;
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//none, constant, delta
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					//none, delta
					if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
						//none
						int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
						
						result = reader.readLongSigned(target, mantDictionary);
					} else {
						//delta
						int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
						
						
						result = reader.readLongSignedDelta(target, source, mantDictionary);
					}	
				} else {
					//constant
					//always return this required value.
					result = mantDictionary[token & DECIMAL_MAX_LONG_INSTANCE_MASK];
				}
				
			} else {
				//copy, default, increment
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					//copy, increment
					if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
						//copy
						int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
						
						result = reader.readLongSignedCopy(target, source, mantDictionary);
					} else {
						//increment
						int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
						int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
						
						
						result = reader.readLongSignedIncrement(target, source, mantDictionary);
					}	
				} else {
					// default
					long constDefault = mantDictionary[token & DECIMAL_MAX_LONG_INSTANCE_MASK];
					
					result = reader.readLongSignedDefault(constDefault);
				}		
			}
			return result;
		} else {
			long result;
			//oppMaint
					if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
						//none, constant, delta
						if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
							//none, delta
							if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
								//none
								long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
								
								result = reader.readLongSignedOptional(constAbsent);
							} else {
								//delta
								int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
								long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
								
								result = reader.readLongSignedDeltaOptional(target, source, mantDictionary, constAbsent);
							}	
						} else {
							//constant
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							long constConst = mantDictionary[token & DECIMAL_MAX_LONG_INSTANCE_MASK];
							
							result = reader.readLongSignedConstantOptional(constAbsent, constConst);
						}
						
					} else {
						//copy, default, increment
						if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
							//copy, increment
							if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
								//copy
								int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
								long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
										
								long value = reader.readLongSignedCopy(target, source, mantDictionary);
								result = (0 == value ? constAbsent: value-1);
							} else {
								//increment
								int target = token&DECIMAL_MAX_LONG_INSTANCE_MASK;
								int source = readFromIdx>0? readFromIdx&DECIMAL_MAX_LONG_INSTANCE_MASK : target;
								long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
								
								result = reader.readLongSignedIncrementOptional(target, source, mantDictionary, constAbsent);
							}	
						} else {
							// default
							long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
							long constDefault = mantDictionary[token & DECIMAL_MAX_LONG_INSTANCE_MASK]==0?constAbsent:mantDictionary[token & DECIMAL_MAX_LONG_INSTANCE_MASK];
							
							result = reader.readLongSignedDefaultOptional(constDefault, constAbsent);
						}		
					}
					return result;
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
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					
					return readerText.readUTF8Optional(idx);
				} else {
					//tail
					//System.err.println("tail");
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readUTF8TailOptional(idx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//System.err.println("const");
					int constInit = (token & readerText.MAX_TEXT_INSTANCE_MASK)|FieldReaderText.INIT_VALUE_MASK;
					int constValue = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readConstantOptional(constInit, constValue);
				} else {
					//delta
					//System.err.println("delta");
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readUTF8DeltaOptional(idx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				return readerText.readUTF8CopyOptional(idx);
			} else {
				//default
				//System.err.println("default");
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				return readerText.readUTF8DefaultOptional(idx);
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
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					
					return readerText.readASCII(idx);
				} else {
					//tail
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readASCIITail(idx, reader.readIntegerUnsigned(), readFromIdx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
					//always return this required value.
					return (token & readerText.MAX_TEXT_INSTANCE_MASK) | FieldReaderText.INIT_VALUE_MASK;
				} else {
					//delta
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readASCIIDelta(readFromIdx, idx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				return readerText.readASCIICopy(idx);
			} else {
				//default
				int target = readerText.MAX_TEXT_INSTANCE_MASK&token;
				
				return reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|target) : readerText.readASCIIToHeap(target);
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
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					
					return readerText.readUTF8(idx);
				} else {
					//tail
				//	System.err.println("tail");
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readUTF8Tail(idx);
				}
			} else {
				// constant delta
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//constant
				//	System.err.println("const");
					//always return this required value.
					return (token & readerText.MAX_TEXT_INSTANCE_MASK) | FieldReaderText.INIT_VALUE_MASK;
				} else {
					//delta
				//	System.err.println("delta read");
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readUTF8Delta(idx);
				}
			}
		} else {
			//copy default
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {//compiler does all the work.
				//copy
				//System.err.println("copy");
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				return readerText.readUTF8Copy(idx);
			} else {
				//default
				//System.err.println("default");
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				return readerText.readUTF8Default(idx);
			}
		}
		
	}

	private int readTextASCIIOptional(int token) {
		
		if (0==(token&((4|2|1)<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(8<<TokenBuilder.SHIFT_OPER))) {
				//none
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
				
				
				return readerText.readASCII(idx);
			} else {
				//tail
				int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
						
				return readerText.readASCIITailOptional(idx);
			}
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readASCIIDeltaOptional(readFromIdx, idx);
				} else {
					int constInit = (token & readerText.MAX_TEXT_INSTANCE_MASK)|FieldReaderText.INIT_VALUE_MASK;
					int constValue = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readConstantOptional(constInit, constValue);
				}		
			} else {
				if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
					int idx = token & readerText.MAX_TEXT_INSTANCE_MASK;
					
					return readerText.readASCIICopyOptional(idx);
				} else {
					//for ASCII we don't need special behavior for optional
					int target = readerText.MAX_TEXT_INSTANCE_MASK&token;
					//CODE:
				//	System.err.println("readerText.readASCIIDefault("+target+")");
					return reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|target) : readerText.readASCIIToHeap(target);
				}
				
			}
		}
		
	}



}
