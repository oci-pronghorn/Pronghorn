//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderText {

	public static final int INIT_VALUE_MASK = 0x80000000;
	
	private final PrimitiveReader reader;
	public final TextHeap heap;
	public final int MAX_TEXT_INSTANCE_MASK;
	
	public FieldReaderText(PrimitiveReader reader, TextHeap heap) {
		
		assert(null==heap || heap.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==heap || TokenBuilder.isPowerOfTwo(heap.itemCount()));
		
		this.MAX_TEXT_INSTANCE_MASK = null==heap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (heap.itemCount()-1));
		
		this.reader = reader;
		this.heap = heap;
	}
	
	public TextHeap textHeap() {
		return heap;
	}

	final byte NULL_STOP = (byte)0x80;

	public int readASCIICopy(int idx) {
		if (reader.popPMapBit()!=0) {
			return readTextASCIIOptional(idx);
		} else {
			return idx;//source
		}
	}
	
	private void readASCIIToHeapNone(int idx, byte val) {
		// 0x80 is a null string.
		// 0x00, 0x80 is zero length string
		if (0==val) {
			//almost never happens
			heap.setZeroLength(idx);
			//must move cursor off the second byte
			val = reader.readTextASCIIByte(); //< .1%
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);			
		} else {
			//happens rarely when it equals 0x80
			heap.setNull(idx);				
			
		}
	}

	private void fastHeapAppend(int idx, byte val) {
		final int offset = idx<<2;
		final int off4 = offset+4;
		final int off1 = offset+1;
		int nextLimit = heap.tat[off4];
		int targIndex = heap.tat[off1];
						
		if (targIndex>=nextLimit) {
			heap.makeSpaceForAppend(offset, 2); //also space for last char
			targIndex = heap.tat[off1];
			nextLimit = heap.tat[off4];
		}
		
		if(val<0) {
			//heap.setSingleCharText((char)(0x7F & val), targIndex);
			heap.rawAccess()[targIndex++] = (char)(0x7F & val);
		} else {
			targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
		}
		heap.tat[off1] = targIndex;
	}

	private int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex) {
		heap.rawAccess()[targIndex++] = (char)val;			

		int len;
		do {
			len = reader.readTextASCII2(heap.rawAccess(), targIndex, nextLimit);
			if (len<0) {
				targIndex-=len;
				heap.makeSpaceForAppend(offset, 2); //also space for last char
				nextLimit = heap.tat[off4];
			} else {
				targIndex+=len;
			}
		} while (len<0);
		return targIndex;
	}

	public int readConstantOptional(int constInit, int constValue) {
		return (reader.popPMapBit()!=0 ? constInit : constValue);
	}
	
	public int readASCIIToHeap(int target) {
		byte val;
		int chr;
		if (0!=(chr = 0x7F&(val = reader.readTextASCIIByte()))) {//low 7 bits have data
			readASCIIToHeapValue(val, chr, target);
		} else {
			readASCIIToHeapNone(target, val);
		}
		return target;
	}

	private void readASCIIToHeapValue(byte val, int chr, int idx) {
										
		if(val<0) {
			heap.setSingleCharText((char)chr, idx);
		} else {
			readASCIIToHeapValueLong(val, idx);
		}
	}

	private void readASCIIToHeapValueLong(byte val, int idx) {
		final int offset = idx<<2;
		int targIndex = heap.tat[offset]; //because we have zero length
		
		int nextLimit;
		int off4;
		
		//ensure there is enough space for the text
		if (targIndex>=(nextLimit = heap.tat[off4 = offset+4])) {
			heap.tat[offset+1] = heap.tat[offset];//set to zero length
			heap.makeSpaceForAppend(offset, 2); //also space for last char
			targIndex = heap.tat[offset+1];
			nextLimit = heap.tat[off4];
		}
		
		//copy all the text into the heap
		heap.tat[offset+1] = heap.tat[offset];//set to zero length
		heap.tat[offset+1] = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
	}

	public int readASCIIDeltaOptional(int readFromIdx, int idx) {
		return readASCIIDelta(readFromIdx, idx);//TODO: C, ASCII need null logic here.
	}

	public int readASCIIDelta(int readFromIdx, int idx) {
		int trim = reader.readIntegerSigned();
		
		if (trim>=0) {
			return readASCIITail(idx, trim, readFromIdx);
		} else {
			return readASCIIHead(idx, trim, readFromIdx);
		}
	}

	public int readASCIITail(int idx, int trim, int readFromIdx) {
		if (trim>0) {
			heap.trimTail(idx, trim);
		}
		
		//System.err.println("read: trim "+trim);
		
		byte val = reader.readTextASCIIByte();
		if (val==0) {
			//nothing to append
			//must move cursor off the second byte
			val = reader.readTextASCIIByte();
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);
		} else {
			if (val==NULL_STOP) {
				//nothing to append and sent value is null
				heap.setNull(idx);				
			} else {		
				if (heap.isNull(idx)) {
					heap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val);
			}
		}
		
		return idx;
	}
	
	public int readASCIITailOptional(int idx) {
		int tail = reader.readIntegerUnsigned();
		if (0==tail) {
			heap.setNull(idx);
			return idx;
		}
		tail--;
				
		heap.trimTail(idx, tail);
		byte val = reader.readTextASCIIByte();
		if (val==0) {
			//nothing to append
			//must move cursor off the second byte
			val = reader.readTextASCIIByte();
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);
		} else {
			if (val==NULL_STOP) {
				//nothing to append
				//charDictionary.setNull(idx);				
			} else {		
				if (heap.isNull(idx)) {
					heap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val);
			}
		}
						
		return idx;
	}
	
	private int readASCIIHead(int idx, int trim, int readFromIdx) {
		if (trim<0) {
			heap.trimHead(idx, -trim);
		}

		byte value = reader.readTextASCIIByte();
		int offset = idx<<2;
		int nextLimit = heap.tat[offset+4];
		
		if (trim>=0) {
			while (value>=0) {
				nextLimit = heap.appendTail(offset, nextLimit, (char)value);
				value = reader.readTextASCIIByte();
			}
			heap.appendTail(offset, nextLimit, (char)(value&0x7F) );
		} else {
			while (value>=0) {
				heap.appendHead(offset, (char)value);
				value = reader.readTextASCIIByte();
			}
			heap.appendHead(offset, (char)(value&0x7F) );
		}
				
		return idx;
	}


	public int readASCIICopyOptional(int idx) {
		if (reader.popPMapBit()!=0) {
			byte val = reader.readTextASCIIByte();
			if (0!=(val&0x7F)) {
				//real data, this is the most common case;
				heap.setZeroLength(idx);				
				fastHeapAppend(idx, val);
			} else {
				readASCIIToHeapNone(idx, val);
			}
		}
		return idx;
	}

	public int readUTF8Default(int idx) {
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			return readUTF8(idx);
		}
	}
	

	public int readUTF8DefaultOptional(int idx) {
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned()-1;
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readUTF8Delta(int idx) {
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readUTF8Tail(int idx) {
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned(); 

		//append to tail	
		int targetOffset = heap.makeSpaceForAppend(idx, trim, utfLength);
		reader.readTextUTF8(heap.rawAccess(), targetOffset, utfLength);
		return idx;
	}
	
	public int readUTF8Copy(int idx) {
		if (reader.popPMapBit()!=0) {
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
		}
		return idx;
	}

	public int readUTF8CopyOptional(int idx) {
		if (reader.popPMapBit()!=0) {			
			int length = reader.readIntegerUnsigned()-1;
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
		}
		return idx;
	}


	public int readUTF8DeltaOptional(int idx) {
		int trim = reader.readIntegerSigned();
		if (0==trim) {
			heap.setNull(idx);
			return idx;
		}
		if (trim>0) {
			trim--;//subtract for optional
		}
		
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readUTF8TailOptional(int idx) {
		int trim = reader.readIntegerUnsigned();
		if (trim==0) {
			heap.setNull(idx);
			return idx;
		} 
		trim--;
		
		int utfLength = reader.readIntegerUnsigned(); //subtract for optional

		//append to tail	
		reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		
		return idx;
	}

	public int readASCII(int idx) {
		return readASCIIToHeap(idx);
	}
	
	public int readTextASCIIOptional(int idx) {
		return readASCIIToHeap(idx);
	}

	public int readUTF8(int idx) {
		return readUTF8s(idx,0);
	}
	public int readUTF8Optional(int idx) {
		return readUTF8s(idx,1);
	}

	private int readUTF8s(int idx, int offset) {
		int length = reader.readIntegerUnsigned()-offset;
		reader.readTextUTF8(heap.rawAccess(), 
				            heap.allocate(idx, length),
				            length);
		return idx;
	}


	
}
