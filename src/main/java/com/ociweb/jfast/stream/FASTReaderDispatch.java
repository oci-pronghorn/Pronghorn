//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.Arrays;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldReaderText;
import com.ociweb.jfast.field.FieldReaderDecimal;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldReaderLong;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
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
	private final FieldReaderLong    readerLong;
	private final FieldReaderDecimal readerDecimal;
	private final FieldReaderText readerText;
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

	int maxNestedSeqDepth = 64; //TODO: need value from template
	int[] sequenceCountStack = new int[maxNestedSeqDepth];
	int sequenceCountStackHead = -1;
	int checkSequence;
    int jumpSequence;
	
	TextHeap charDictionary;
	ByteHeap byteDictionary;

	int activeScriptCursor;
	int activeScriptLimit;
	
	int[] fullScript;

	
	public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, 
			                   int nonTemplatePMapSize, int[][] dictionaryMembers, int maxTextLen, 
			                   int maxVectorLen, int charGap, int bytesGap, int[] fullScript) {
		this.reader = reader;
		this.dictionaryFactory = dcr;
		this.nonTemplatePMapSize = nonTemplatePMapSize;
		this.dictionaryMembers = dictionaryMembers;
				
		this.charDictionary = dcr.charDictionary(maxTextLen,charGap);
		this.byteDictionary = dcr.byteDictionary(maxVectorLen,bytesGap);
		
		this.fullScript = fullScript;
		
		this.readerInteger = new FieldReaderInteger(reader,dcr.integerDictionary(),dcr.integerDictionary());
		this.readerLong = new FieldReaderLong(reader,dcr.longDictionary(),dcr.longDictionary());
		this.readerDecimal = new FieldReaderDecimal(reader, 
													dcr.decimalExponentDictionary(),
													dcr.decimalExponentDictionary(),
				                                    dcr.decimalMantissaDictionary(),
				                                    dcr.decimalMantissaDictionary());
		this.readerText = new FieldReaderText(reader,charDictionary);
		this.readerBytes = new FieldReaderBytes(reader,byteDictionary);
	}

	public void reset() {
		
		//System.err.println("total read fields:"+totalReadFields);
	//	totalReadFields = 0;
		
		//clear all previous values to un-set
		dictionaryFactory.reset(readerInteger.dictionary);
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
	
	public boolean dispatchReadByTokenGen(FASTRingBuffer outputQueue) {

		//TODO Ideas:
		//dispatch code extends RingBuffer
		//dispatch code extends joined readers?
		//* readers become static calls with arguments (would help with Julia all the way down)
		//* can ring buffer pass in needed data?
		//MUCH OF THIS IS ALREADY OPTIMIZED AT RUN TIME BY JIT THEN WHY IS IT STILL SLOW?
		//TODO: do end-run around INVOKEVIRTUAL by calling fewer larger methods.
		
		int cursor = activeScriptCursor;
		//TODO: all these methods need to be InvokeStatic or InvokeSpecial!!!
		// Interface calls are the slowest followed by virtuals then static/special.
		
		
		switch(cursor) {
			 //TODO: hardcode the token INDEX values into here instead of script lookups with token mask!!!
		
			 case 0:
				   //System.err.println("0x"+Integer.toHexString(script[cursor]));
				   outputQueue.appendText(readerText.readASCIIConstant(0xa02c0000,readFromIdx));
				   activeScriptCursor = 1;
				   return false;
				
			 case 1:
				   
				 //TODO: we DO want to use this dictiionary and keep NONE opp operators IFF they are referenced by other fields.
			    	int[] dictionary = readerInteger.dictionary;
				   		   
				   readDictionaryReset(0xe00c0002);	 
				   int a = readerText.readASCIIConstant(0xa02c0001,-1);
				   outputQueue.appendText(a); 
				   int b = readerText.readASCIIConstant(0xa02c0002,-1);
				   outputQueue.appendText(b); 
				   int c = readerText.readASCIIConstant(0xa02c0003,-1);
				   outputQueue.appendText(c);
				   int d = reader.readIntegerUnsigned();
				   outputQueue.appendInteger(d);
				   int e = reader.readIntegerUnsigned();
				   outputQueue.appendInteger(e);
				   int f = reader.readIntegerUnsigned();
				   outputQueue.appendInteger(f);
				   cursor+=8;
				   							   
				   //outputQueue.append(readLength(token,readFromIdx));
					int length;
					outputQueue.appendInteger(length = readIntegerUnsigned(0xd00c0003));

					if (length==0) {
					    //jumping over sequence (forward) it was skipped (rare case)
						cursor += 22;
						activeScriptCursor = cursor;
						break;
					} else {			
						sequenceCountStack[++sequenceCountStackHead] = length;
					}		
					
				   //TODO: this is rather questionable because the length can be zero but falls through for now.
				   
			 case 9:
				 case9a(outputQueue);
			     case9b(outputQueue);
			   return readGroupClose(0xc0dc0014, 29); //TODO: wrong tokens for jump!!!

					
			 case 30:
							 
				   case30a(outputQueue);
				
				   cursor+=6;
				   //outputQueue.append(readLength(token,readFromIdx));
					int length2;
					outputQueue.appendInteger(length2 = readIntegerUnsigned(0xd00c0011));
	
					if (length2==0) {
					    //jumping over sequence (forward) it was skipped (rare case)
						cursor += 10;
						activeScriptCursor = cursor;
						break;
					} else {			
						sequenceCountStack[++sequenceCountStackHead] = length2;
					}				
				
				   return case30b(outputQueue, cursor);
		
		}
		
		assert(false) : "Unsupported Template";
		return false;
	}
	

	final int constIntAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
	final long constLongAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;

	private void case30a(FASTRingBuffer outputQueue) {
		int a = readerText.readASCIIConstant(0xa02c000a,readFromIdx);
		outputQueue.appendText(a);
	    
		int b = readerText.readASCIIConstant(0xa02c000b,readFromIdx);
	    outputQueue.appendText(b);
	   
	    int c = readerText.readASCIIConstant(0xa02c000c,readFromIdx);
	    outputQueue.appendText(c);
	   
	    int d = reader.readIntegerUnsigned();
	    outputQueue.appendInteger(d);
	
	    int e = reader.readIntegerUnsigned();
	    outputQueue.appendInteger(e);
	   
	    int f = readerText.readASCII(0xa40c000d,readFromIdx);
	    outputQueue.appendText(f);
	}

	private boolean case30b(FASTRingBuffer outputQueue, int cursor) {
		boolean result;
		reader.openPMap(nonTemplatePMapSize);
		
	    int a = readerText.readASCIIConstant(0xa02c000e,readFromIdx);
		outputQueue.appendText(a);
		long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(0x940c0000));
	
		long b = readerLong.reader.readLongUnsignedOptional(constAbsent);
		outputQueue.appendLong(b);
		
		int t = readerInteger.dictionary[0x12];//TODO: compute runtime constant in code gen
		int constDefault1 = t == 0 ? constIntAbsent : t-1; //TODO: runtime constant in code gen;
        int c = reader.readIntegerUnsignedDefaultOptional(constDefault1, constIntAbsent);
		outputQueue.appendInteger(c);
		int target = 0x900c0001&readerLong.MAX_LONG_INSTANCE_MASK;
	
		long d = readerLong.reader.readLongUnsigned(target, readerLong.dictionary);
		outputQueue.appendLong(d);

		int constDefault = readerInteger.dictionary[0x13];//TODO: runtime constant to be injected by code generator.
		int e = reader.readIntegerUnsignedDefault(constDefault);
		outputQueue.appendInteger(e);
		
		int f = reader.readIntegerUnsigned();
		outputQueue.appendInteger(f);
		
		int g = readerInteger.dictionary[0x15];
		outputQueue.appendInteger(g);

		cursor+=10;
		   //outputQueue.append(readGroup(token,readFromIdx));
		result = readGroupClose(0xc0dc0008, cursor-1);
		activeScriptCursor = cursor-1;
		return result;
	}

	private void case9b(FASTRingBuffer outputQueue) {

		   int[] dictionary = readerInteger.dictionary;
		   
		   FieldReaderDecimal rDecimal = readerDecimal;
		
		   int a1 = reader.readIntegerUnsignedCopy(0x0a, 0x0a, dictionary);
		   outputQueue.appendInteger(a1);
		
		   int b1 = reader.readIntegerSignedDeltaOptional(0x0b, 0x0b, dictionary, constIntAbsent);
		   outputQueue.appendInteger(b1);
		
		   int c1 = reader.readIntegerUnsignedDeltaOptional(0x0c, 0x0c, dictionary, constIntAbsent);
		   outputQueue.appendInteger(c1);

		   int d1 = readerText.readASCIIDefault(0x05);
		   outputQueue.appendText(d1);
		   		   
		   int constDefault2 = rDecimal.exponent.dictionary[0x01]==0?constIntAbsent:rDecimal.exponent.dictionary[0x01];
			   
		   int e1 = reader.readIntegerSignedDefaultOptional(constDefault2, constIntAbsent);
		   long e2 = reader.readLongSignedDeltaOptional(0x01, 0x01, rDecimal.mantissa.dictionary, constLongAbsent);
		   outputQueue.appendDecimal(e1, e2);
		   
		   int constDefault1 = dictionary[0x0d] == 0 ? constIntAbsent : dictionary[0x0d]-1; //TODO: runtime constant;
		   int a = reader.readIntegerUnsignedDefaultOptional(constDefault1, constIntAbsent);
		   outputQueue.appendInteger(a);

		   int b = readerText.readASCIIDefault(0x06);
		   outputQueue.appendText(b);

		   int c = readerText.readASCIIDefault(0x07);
		   outputQueue.appendText(c);

		   int d = readerText.readASCIIDefault(0x08);
		   outputQueue.appendText(d);
		
		   int t = dictionary[0x0e];//TODO: runtime constant
		   int constDefault = t == 0 ? constIntAbsent : t-1; //TODO: runtime constant;
		   
		   int e = reader.readIntegerUnsignedDefaultOptional(constDefault, constIntAbsent);
		   outputQueue.appendInteger(e);
	
		   int f = readerText.readASCIIDefault(0x09);
		   outputQueue.appendText(f);
	}

	private void case9a(FASTRingBuffer outputQueue) {
		//TODO: expose ringbuffer to make the append methods static here.
		
		   int[] dictionary = readerInteger.dictionary;
		   FieldReaderDecimal rDecimal = readerDecimal;
		
		   reader.openPMap(nonTemplatePMapSize);

		   int a = reader.readIntegerUnsignedCopy(0x04, 0x04, dictionary);
		   outputQueue.appendInteger(a);
		   

		   int t = dictionary[0x05];//TODO: runtime constant
		   int constDefault = t == 0 ? constIntAbsent : t-1; //TODO: runtime constant;
		   int b = reader.readIntegerUnsignedDefaultOptional(constDefault, constIntAbsent);
		   outputQueue.appendInteger(b);
		   
		   int c = readerText.readASCIICopy(0xa01c0004,-1);
		   outputQueue.appendText(c);
		
		   int value = reader.readIntegerUnsigned();
		   int d = value==0 ? constIntAbsent : value-1;
		   outputQueue.appendInteger(d);
		   
		   int e = dictionary[0x07];
		   outputQueue.appendInteger(e);
		   
		   int f = reader.readIntegerUnsignedCopy(0x08, 0x08, dictionary);
  		   outputQueue.appendInteger(f);
		
		   int g = reader.readIntegerUnsignedIncrement(0x09, 0x09, dictionary);
		   outputQueue.appendInteger(g);
		
		   int constDefault1 = rDecimal.exponent.dictionary[0x00]; //TODO: runtime constant

		    //System.err.println(  TokenBuilder.tokenToString(0xb1cc0000));
		    int h1 = reader.readIntegerSignedDefault(constDefault1);
			long h2 = reader.readLongSignedDelta(0x00, 0x00, rDecimal.mantissa.dictionary);
			outputQueue.appendDecimal(h1, h2);
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
	final boolean dispatchReadByToken(FASTRingBuffer outputQueue) {
	
		
		//move everything needed in this tight loop to the stack
		int cursor = activeScriptCursor;
		int limit = activeScriptLimit;
		int[] script = fullScript;
		
		//this is a unique definition for the series of fields to follow.
		//if this can be cached it would be a big reduction in work!!!
		//System.err.println("cursor:"+cursor);
		//TODO: could build linked list for each location?
//		
		boolean codeGen = cursor!=9 && cursor!=1 && cursor!=30 && cursor!=0;
		//TODO: once this code matches the methods used here take it out and move it to the TemplateLoader
		
		int zz = cursor;
		
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
			
			assert(gatherReadData(reader,token,cursor));

			//TODO: Need group method with optional support
			//TODO: Need a way to unify Decimal? Do as two Tokens?
//			StringBuilder target = new StringBuilder();
//			TokenBuilder.methodNameRead(token, target);
//			System.err.println(target);
			
			//The trick here is to keep all the conditionals in this method and do the work elsewhere.
			if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
				//0????
				if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
					//00???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						outputQueue.appendInteger(dispatchReadByTokenForInteger(token));//int
					} else {
						outputQueue.appendLong(dispatchReadByTokenForLong(token));//long
					}
				} else {
					//01???
					if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
						//int for text					
						outputQueue.appendText(dispatchReadByTokenForText(token));				
					} else {
						//011??
						if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
							//0110? Decimal and DecimalOptional
							if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
								outputQueue.appendDecimal(readerDecimal.readDecimalExponent(token, -1), //TODO: must add support for decimal pulling last value from another dicitonary.
								                   		  readerDecimal.readDecimalMantissa(token, -1));
							} else {
								outputQueue.appendDecimal(readerDecimal.readDecimalExponentOptional(token, -1),
								    		       		  readerDecimal.readDecimalMantissaOptional(token, -1));
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
							boolean result = readGroupClose(token, cursor);	
							//if (1!=zz && 9!=zz && 30!=zz) {
							//	System.err.println(zz+" yy "+activeScriptCursor);
							//}

							return result;
						}
						
					} else {
						//101??
						
						//Length Type, no others defined so no need to keep checking
						//Only happens once before a node sequence so push it on the count stack
						int length;
						outputQueue.appendInteger(length = readIntegerUnsigned(token));
						
						//int oldCursor = cursor;
						cursor = sequenceJump(length, cursor);
					//	System.err.println("jumpDif:"+(cursor-oldCursor));
					}
				} else {
					//11???
					//Dictionary Type, no others defined so no need to keep checking
					if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
						readDictionaryReset(token);					
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
		
		if (0!=zz) {
			System.err.println(zz+" xx "+activeScriptCursor);
		}

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
			jumpSequence = 0;//TODO: not sure this is needed.
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
	
	boolean gatherReadData(PrimitiveReader reader, String msg) {

		if (null!=observer) {
			long absPos = reader.totalRead()-reader.bytesReadyToParse();
			observer.tokenItem(absPos, -1, activeScriptCursor, msg);
		}
		
		return true;
	}

	private void readDictionaryReset(int token) {
		readDictionaryReset2(dictionaryMembers[TokenBuilder.MAX_INSTANCE&token]);
	}

	//TODO: code generation, may be the best solution for this.
	private void readDictionaryReset2(int[] members) {
		
		//System.err.println("reset:"+Arrays.toString(members));
		
		//TODO: URGENT: no need to reset the values of constants and default where the value is not avoided
		
		int limit = members.length;
		int m = 0;
		int idx = members[m++]; //assumes that a dictionary always has at lest 1 member
		while (m<limit) {
			assert(idx<0);
			
			if (0==(idx&8)) {
				if (0==(idx&4)) {
					//integer
					//System.err.println("int");
					int[] d1 = readerInteger.dictionary;
					int[] d2 = readerInteger.init;
					while (m<limit && (idx = members[m++])>=0) {
						d1[idx] = d2[idx];
					}
				} else {
					//long
					//System.err.println("long");
					while (m<limit && (idx = members[m++])>=0) {
						readerLong.dictionary[idx] = readerLong.init[idx];
					}
				}
			} else {
				if (0==(idx&4)) {							
					//text
					//System.err.println("text");
					TextHeap h = readerText.heap;
					while (m<limit && (idx = members[m++])>=0) {						
						h.setNull(idx); //TODO: this is not right, must keep value string from template parse.
					}
				} else {
					if (0==(idx&2)) {								
						//decimal
						//System.err.println("decimal");
						while (m<limit && (idx = members[m++])>=0) {
							readerDecimal.exponent.dictionary[idx] = readerDecimal.exponent.init[idx];
							readerDecimal.mantissa.dictionary[idx] = readerDecimal.mantissa.init[idx];
						}
					} else {
						//bytes
						while (m<limit && (idx = members[m++])>=0) {
							readerBytes.reset(idx);
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
				return 	readTextASCII(token);
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
					
					return readerInteger.reader.readIntegerSignedDeltaOptional(target, source, readerInteger.dictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
				
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
					
					int value = readerInteger.reader.readIntegerSignedCopy(target, source, readerInteger.dictionary);
					return (0 == value ? constAbsent: (value>0 ? value-1 : value));
				} else {
					//increment
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerSignedIncrementOptional(target, source, readerInteger.dictionary, constAbsent);
				}	
			} else {
				// default
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constDefault = readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK]==0?constAbsent:readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
						
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
					
					return readerInteger.reader.readIntegerSignedDelta(target, source, readerInteger.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerSignedCopy(target, source, readerInteger.dictionary);
				} else {
					//increment
					int target = token&readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>0? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerSignedIncrement(target, source, readerInteger.dictionary);	
				}	
			} else {
				// default
				int constDefault = readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];	
				
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
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));//TODO: runtime constant		
					
					return readerInteger.reader.readIntegerUnsignedDeltaOptional(target, source, readerInteger.dictionary, constAbsent);
				}	
			} else {
				//constant
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
				int constConst = readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
				
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
					
					int value = readerInteger.reader.readIntegerUnsignedCopy(target, source, readerInteger.dictionary);
					
					return (0 == value ? TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token)): value-1);
				} else {
					//increment
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
					
					return readerInteger.reader.readIntegerUnsignedIncrementOptional(target, source, readerInteger.dictionary, constAbsent);	
				}	
			} else {
				// default
				int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
				int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));//TODO: runtime constant.
				int t = readerInteger.dictionary[source];//TODO: runtime constant
				int constDefault = t == 0 ? constAbsent : t-1; //TODO: runtime constant;
				
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
					
					return readerInteger.reader.readIntegerUnsignedDelta(target, source, readerInteger.dictionary);
				}	
			} else {
				//constant
				//always return this required value.
				return readerInteger.dictionary[token & readerInteger.MAX_INT_INSTANCE_MASK];
			}
			
		} else {
			//copy, default, increment
			if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
				//copy, increment
				if (0==(token&(4<<TokenBuilder.SHIFT_OPER))) {
					//copy
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
							
					//TODO: do each refactor like this . then inline each of these wrapping methods. ReadFrom/To will be implemented this way.
					return readerInteger.reader.readIntegerUnsignedCopy(target, source, readerInteger.dictionary);
				} else {
					//increment
					int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
					int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
					
					return readerInteger.reader.readIntegerUnsignedIncrement(target, source, readerInteger.dictionary);
				}	
			} else {
				// default
				int target = token & readerInteger.MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>=0 ? readFromIdx&readerInteger.MAX_INT_INSTANCE_MASK : target;
				int constDefault = readerInteger.dictionary[source];//TODO: runtime constant to be injected by code generator.
				
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
					
					return readerText.readASCIIDefault(target);
				}
				
			}
		}
		
	}



}
