//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderText {

    //TODO A, refactor this into a static helper class with the code needed to tie heap and reader together.
    
	public static final int INIT_VALUE_MASK = 0x80000000;
	
	public final int MAX_TEXT_INSTANCE_MASK;
	
	final static byte NULL_STOP = (byte)0x80;
	
	public FieldReaderText(TextHeap heap) {
		
		assert(null==heap || heap.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==heap || TokenBuilder.isPowerOfTwo(heap.itemCount()));
		
		this.MAX_TEXT_INSTANCE_MASK = null==heap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (heap.itemCount()-1));

	}


	private static void readASCIIToHeapNone(int idx, byte val, TextHeap textHeap, PrimitiveReader primitiveReader) {
		// 0x80 is a null string.
		// 0x00, 0x80 is zero length string
		if (0==val) {
			//almost never happens
			textHeap.setZeroLength(idx);
			//must move cursor off the second byte
			val = primitiveReader.readTextASCIIByte(); //< .1%
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);			
		} else {
			//happens rarely when it equals 0x80
		    textHeap.setNull(idx);				
			
		}
	}

	private static void fastHeapAppend(int idx, byte val, TextHeap textHeap, PrimitiveReader primitiveReader) {
		final int offset = idx<<2;
		final int off4 = offset+4;
		final int off1 = offset+1;
		int nextLimit = textHeap.tat[off4];
		int targIndex = textHeap.tat[off1];
						
		if (targIndex>=nextLimit) {
		    textHeap.makeSpaceForAppend(offset, 2); //also space for last char
			targIndex = textHeap.tat[off1];
			nextLimit = textHeap.tat[off4];
		}
		
		if(val<0) {
			//heap.setSingleCharText((char)(0x7F & val), targIndex);
		    textHeap.rawAccess()[targIndex++] = (char)(0x7F & val);
		} else {
			targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, textHeap, primitiveReader);
		}
		textHeap.tat[off1] = targIndex;
	}

	private static int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex, TextHeap textHeap, PrimitiveReader primitiveReader) {
		textHeap.rawAccess()[targIndex++] = (char)val;			

		int len;
		do {
			len = primitiveReader.readTextASCII2(textHeap.rawAccess(), targIndex, nextLimit);
			if (len<0) {
				targIndex-=len;
				textHeap.makeSpaceForAppend(offset, 2); //also space for last char
				nextLimit = textHeap.tat[off4];
			} else {
				targIndex+=len;
			}
		} while (len<0);
		return targIndex;
	}

	public static int readASCIIToHeap(int target, TextHeap textHeap, PrimitiveReader primitiveReader) {
		byte val;
		int chr;
		if (0!=(chr = 0x7F&(val = primitiveReader.readTextASCIIByte()))) {//low 7 bits have data
			readASCIIToHeapValue(val, chr, target, textHeap, primitiveReader);
		} else {
			readASCIIToHeapNone(target, val, textHeap, primitiveReader);
		}
		return target;
	}

	private static void readASCIIToHeapValue(byte val, int chr, int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
										
		if(val<0) {
			textHeap.setSingleCharText((char)chr, idx);
		} else {
			readASCIIToHeapValueLong(val, idx, textHeap, primitiveReader);
		}
	}

	private static void readASCIIToHeapValueLong(byte val, int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		final int offset = idx<<2;
		int targIndex = textHeap.tat[offset]; //because we have zero length
		
		int nextLimit;
		int off4;
		
		//ensure there is enough space for the text
		if (targIndex>=(nextLimit = textHeap.tat[off4 = offset+4])) {
		    textHeap.tat[offset+1] = textHeap.tat[offset];//set to zero length
		    textHeap.makeSpaceForAppend(offset, 2); //also space for last char
			targIndex = textHeap.tat[offset+1];
			nextLimit = textHeap.tat[off4];
		}
		
		//copy all the text into the heap
		textHeap.tat[offset+1] = textHeap.tat[offset];//set to zero length
		textHeap.tat[offset+1] = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, textHeap, primitiveReader);
	}

	public static int readASCIIDeltaOptional2(int readFromIdx, int idx, int optionalTrim, TextHeap textHeap, PrimitiveReader primitiveReader) {
        return (optionalTrim>0 ? readASCIITail(idx, optionalTrim-1, readFromIdx, textHeap, primitiveReader):
                                 readASCIIHead(idx, optionalTrim, readFromIdx, textHeap, primitiveReader));
    }

	public static int readASCIITail(int idx, int trim, int readFromIdx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		
		//TODO: B, if readFromIdx does not match idx must do different work.
		
		if (trim>0) {
			textHeap.trimTail(idx, trim);
		}
		
		//System.err.println("read: trim "+trim);
		
		byte val = primitiveReader.readTextASCIIByte();
		if (val==0) {
			//nothing to append
			//must move cursor off the second byte
			val = primitiveReader.readTextASCIIByte();
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);
		} else {
			if (val==NULL_STOP) {
				//nothing to append and sent value is null
			    textHeap.setNull(idx);				
			} else {		
				if (textHeap.isNull(idx)) {
				    textHeap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val, textHeap, primitiveReader);
			}
		}
		
		return idx;
	}
	
	public static int readASCIITailOptional(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int tail = primitiveReader.readIntegerUnsigned();
		if (0==tail) {
			textHeap.setNull(idx);
			return idx;
		}
		tail--;
				
		textHeap.trimTail(idx, tail);
		byte val = primitiveReader.readTextASCIIByte();
		if (val==0) {
			//nothing to append
			//must move cursor off the second byte
			val = primitiveReader.readTextASCIIByte();
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);
		} else {
			if (val==NULL_STOP) {
				//nothing to append
				//charDictionary.setNull(idx);				
			} else {		
				if (textHeap.isNull(idx)) {
				    textHeap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val, textHeap, primitiveReader);
			}
		}
						
		return idx;
	}
	
	public static int readASCIIHead(int idx, int trim, int readFromIdx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		if (trim<0) {
			textHeap.trimHead(idx, -trim);
		}

		byte value = primitiveReader.readTextASCIIByte();
		int offset = idx<<2;
		int nextLimit = textHeap.tat[offset+4];
		
		if (trim>=0) {
			while (value>=0) {
				nextLimit = textHeap.appendTail(offset, nextLimit, (char)value);
				value = primitiveReader.readTextASCIIByte();
			}
			textHeap.appendTail(offset, nextLimit, (char)(value&0x7F) );
		} else {
			while (value>=0) {
			    textHeap.appendHead(offset, (char)value);
				value = primitiveReader.readTextASCIIByte();
			}
			textHeap.appendHead(offset, (char)(value&0x7F) );
		}
				
		return idx;
	}


	public static void readASCIICopyOptional2(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        byte val = primitiveReader.readTextASCIIByte();
        if (0!=(val&0x7F)) {
        	//real data, this is the most common case;
        	textHeap.setZeroLength(idx);				
        	fastHeapAppend(idx, val, textHeap, primitiveReader);
        } else {
        	readASCIIToHeapNone(idx, val, textHeap, primitiveReader);
        }
    }

	public static int readUTF8Delta(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int trim = primitiveReader.readIntegerSigned();
		int utfLength = primitiveReader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
		    primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
		    primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public static int readUTF8Tail(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int trim = primitiveReader.readIntegerSigned();
		int utfLength = primitiveReader.readIntegerUnsigned(); 

		//append to tail	
		int targetOffset = textHeap.makeSpaceForAppend(idx, trim, utfLength);
		primitiveReader.readTextUTF8(textHeap.rawAccess(), targetOffset, utfLength);
		return idx;
	}
	
	public static void readUTF8Copy2(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        int length = primitiveReader.readIntegerUnsigned();
        primitiveReader.readTextUTF8(textHeap.rawAccess(), 
                textHeap.allocate(idx, length),
        		            length);
    }

	public static int readUTF8CopyOptional2(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        int length = primitiveReader.readIntegerUnsigned()-1;
        primitiveReader.readTextUTF8(textHeap.rawAccess(), 
                textHeap.allocate(idx, length),
        		            length);
        return idx;
    }


	public static int readUTF8DeltaOptional(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int trim = primitiveReader.readIntegerSigned();
		if (0==trim) {
		    textHeap.setNull(idx);
			return idx;
		}
		if (trim>0) {
			trim--;//subtract for optional
		}
		
		int utfLength = primitiveReader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
		    primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
		    primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public static int readUTF8TailOptional(int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int trim = primitiveReader.readIntegerUnsigned();
		if (trim==0) {
		    textHeap.setNull(idx);
			return idx;
		} 
		int utfLength = primitiveReader.readIntegerUnsigned(); //subtract for optional
		primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim-1, utfLength), utfLength);
		return idx;
	}

	public static int readUTF8s(int idx, int offset, TextHeap textHeap, PrimitiveReader primitiveReader) {
		int length = primitiveReader.readIntegerUnsigned()-offset;
		primitiveReader.readTextUTF8(textHeap.rawAccess(), 
		        textHeap.allocate(idx, length),
				            length);
		return idx;
	}


	
}
