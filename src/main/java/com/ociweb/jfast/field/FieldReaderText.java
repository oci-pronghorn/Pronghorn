//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public final class FieldReaderText {

	private static final int INIT_VALUE_MASK = 0x80000000;
	private static final int INT_VALUE_SHIFT = 31;
	
	private final PrimitiveReader reader;
	public final TextHeap heap;
	private final char[] targ;
	public final int MAX_TEXT_INSTANCE_MASK;
	
	public FieldReaderText(PrimitiveReader reader, TextHeap heap) {
		
		assert(null==heap || heap.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==heap || TokenBuilder.isPowerOfTwo(heap.itemCount()));
		
		this.MAX_TEXT_INSTANCE_MASK = null==heap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (heap.itemCount()-1));
		
		this.reader = reader;
		this.heap = heap;
		this.targ = null==heap?null:heap.rawAccess();
	}
	
	public TextHeap textHeap() {
		return heap;
	}
	
	public void reset() {
		if (null!=heap) {
			heap.reset();		
		}
	}
	
	static boolean isPowerOfTwo(int length) {
		
		while (0==(length&1)) {
			length = length>>1;
		}
		return length==1;
	}

	final byte NULL_STOP = (byte)0x80;

	//PATTERN: return (popPMapBit(pmapIdx, bitBlock)==0 ? dictionary[source] : (dictionary[target] = readLongUnsignedPrivate()));
	public int readASCIICopy(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			byte val = reader.readTextASCIIByte();
			if (0!=(val&0x7F)) {
				//real data, this is the most common case;
				heap.setZeroLength(idx);				
				fastHeapAppend(idx, val);
			} else {
				readASCIIToHeapNone(idx, val);
			}
			return idx;//target
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
			targ[targIndex++] = (char)(0x7F & val);
		} else {
			targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
		}
		heap.tat[off1] = targIndex;
	}

	private int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex) {
		targ[targIndex++] = (char)val;			

		int len;
		do {
			len = reader.readTextASCII2(targ, targIndex, nextLimit);
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

	public int readASCIIConstant(int token, int readFromIdx) {
		//always return this required value.
		return (token & MAX_TEXT_INSTANCE_MASK) | INIT_VALUE_MASK;
	}
	
	public int readASCIIConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()!=0 ? (token & MAX_TEXT_INSTANCE_MASK)|INIT_VALUE_MASK : token & MAX_TEXT_INSTANCE_MASK);
	}

	public int readUTF8Constant(int token, int readFromIdx) {
		//always return this required value.
		return (token & MAX_TEXT_INSTANCE_MASK) | INIT_VALUE_MASK;
	}
	
	public int readUTF8ConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()!=0 ? (token & MAX_TEXT_INSTANCE_MASK)|INIT_VALUE_MASK : token & MAX_TEXT_INSTANCE_MASK);
	}
	
	//PATTERN:  (popPMapBit(pmapIdx, bitBlock)==0 ? constDefault : readLongUnsignedPrivate());
	public int readASCIIDefault(int target) {
		
		//TODO: can shift the high bit from the value of popPMapBit.
		//if >=0 target then compute the value.
		
//		int result = (((int)reader.popPMapBit()-1)&INIT_VALUE_MASK)|target;
//		if (result>=0) {
//			temp(target, reader.readTextASCIIByte());
//		}
//		return result;
		
		
		//NOTE: also supports optional case due to ASII optinal encoding.
		if (reader.popPMapBit()==0) {//50% of time here in pop pmap
			//  1/3 of calls here
			return INIT_VALUE_MASK|target;//use default
		} else {
			temp(target, reader.readTextASCIIByte());
			return target;
		}
		
	}

	private void temp(int target, byte val) {
		int tmp;
		if (0!=(tmp = 0x7F&val)) {//low 7 bits have data
			readASCIIDefault2(val, tmp, target);
		} else {
			readASCIIToHeapNone(target, val);
		}
	}

	private void readASCIIDefault2(byte val, int tmp, int idx) {
		int[] tat = heap.tat;
		//real data, this is the most common case;
		///  2/3 of calls here
		final int offset = idx<<2;
		final int off4 = offset+4;
		final int off1 = offset+1;
		int nextLimit = tat[off4];
		int targIndex = tat[offset]; //because we have zero length
						
		if (targIndex>=nextLimit) {
			tat[off1] = tat[offset];//set to zero length
			heap.makeSpaceForAppend(offset, 2); //also space for last char
			targIndex = tat[off1];
			nextLimit = tat[off4];
		}
		
		if(val<0) {
			targ[targIndex++] = (char)tmp;
		} else {//System.err.println("not called in complex text");
			tat[off1] = tat[offset];//set to zero length
			targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
		}
		tat[off1] = targIndex;
	}

	public int readASCIIDeltaOptional(int token, int readFromIdx) {
		return readASCIIDelta(token, readFromIdx);//TODO: need null logic here.
	}

	public int readASCIIDelta(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		
		if (trim>=0) {
			return readASCIITail(idx, trim, readFromIdx);
		} else {
			return readASCIIHead(idx, trim, readFromIdx);
		}
	}

	public int readASCIITail(int token, int readFromIdx) {
		return readASCIITail(token & MAX_TEXT_INSTANCE_MASK, reader.readIntegerUnsigned(), readFromIdx);
	}

	private int readASCIITail(int idx, int trim, int readFromIdx) {
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
	
	public int readASCIITailOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
				
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
		
	//	System.out.println("new ASCII string:"+charDictionary.get(idx, new StringBuilder()));
		
		return idx;
	}


	public int readASCIICopyOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
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





	public int readUTF8Default(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}
	

	public int readUTF8DefaultOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
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

	public int readUTF8Delta(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		//	System.out.println("new UTF8 string:"+charDictionary.get(idx, new StringBuilder()));
		}
		
		return idx;
	}

	public int readUTF8Tail(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned(); 

		//append to tail	
		int targetOffset = heap.makeSpaceForAppend(idx, trim, utfLength);
		reader.readTextUTF8(heap.rawAccess(), targetOffset, utfLength);
		return idx;
	}
	
	public int readUTF8Copy(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
		}
		return idx;
	}

	public int readUTF8CopyOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {			
			int length = reader.readIntegerUnsigned()-1;
			reader.readTextUTF8(heap.rawAccess(), 
					            heap.allocate(idx, length),
					            length);
		}
		return idx;
	}


	public int readUTF8DeltaOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
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
			//System.err.println("oldString :"+charDictionary.get(idx, new StringBuilder())+" TAIL");
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
			//System.err.println("new UTF8 Opp   trim tail "+trim+" added to head "+utfLength+" string:"+charDictionary.get(idx, new StringBuilder()));
		} else {
			//append to head
			//System.err.println("oldString :"+charDictionary.get(idx, new StringBuilder())+" HEAD");
			reader.readTextUTF8(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
			//System.err.println("new UTF8 Opp   trim head "+trim+" added to head "+utfLength+" string:"+charDictionary.get(idx, new StringBuilder()));
		}
		
		return idx;
	}

	public int readUTF8TailOptional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		
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

	public int readASCII(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		byte val = reader.readTextASCIIByte();
		if (0!=(val&0x7F)) {
			//real data, this is the most common case;
			heap.setZeroLength(idx);				
			fastHeapAppend(idx, val);
		} else {
			readASCIIToHeapNone(idx, val);
		}
		return idx;
	}
	
	public int readTextASCIIOptional(int token, int readFromIdx) {
		return readASCII(token, readFromIdx);
	}

	public int readUTF8(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readTextUTF8(heap.rawAccess(), 
				            heap.allocate(idx, length),
				            length);
		return idx;
	}

	public int readUTF8Optional(int token, int readFromIdx) {
		int idx = token & MAX_TEXT_INSTANCE_MASK;
		int length = reader.readIntegerUnsigned()-1;
		reader.readTextUTF8(heap.rawAccess(), 
				            heap.allocate(idx, length),
				            length);
		return idx;
	}

	
}
