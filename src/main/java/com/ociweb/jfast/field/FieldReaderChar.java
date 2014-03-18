//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderChar {

	public static final int INIT_VALUE_MASK = 0x80000000;
	private final PrimitiveReader reader;
	private final TextHeap textHeap;
	private final char[] targ;
	private final int INSTANCE_MASK;
	
	public FieldReaderChar(PrimitiveReader reader, TextHeap heap) {
		
		assert(null==heap || heap.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==heap || FieldReaderInteger.isPowerOfTwo(heap.itemCount()));
		
		this.INSTANCE_MASK = null==heap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (heap.itemCount()-1));
		
		this.reader = reader;
		this.textHeap = heap;
		this.targ = null==textHeap?null:textHeap.rawAccess();
	}
	
	public TextHeap textHeap() {
		return textHeap;
	}
	
	public void reset() {
		if (null!=textHeap) {
			textHeap.reset();		
		}
	}
	
	static boolean isPowerOfTwo(int length) {
		
		while (0==(length&1)) {
			length = length>>1;
		}
		return length==1;
	}

	final byte NULL_STOP = (byte)0x80;

	public int readASCIICopy(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			byte val = reader.readTextASCIIByte();
			if (0!=(val&0x7F)) {
				//real data, this is the most common case;
				textHeap.setZeroLength(idx);				
				fastHeapAppend(idx, val);
			} else {
				readASCIIToHeapNone(idx, val);
			}
		}
		return idx;
	}

	
	private void readASCIIToHeapNone(int idx, byte val) {
		// 0x80 is a null string.
		// 0x00, 0x80 is zero length string
		if (0==val) {
			//almost never happens
			textHeap.setZeroLength(idx);
			//must move cursor off the second byte
			val = reader.readTextASCIIByte(); //< .1%
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);			
		} else {
			//happens rarely when it equals 0x80
			textHeap.setNull(idx);				
			
		}
	}

	private void fastHeapAppend(int idx, byte val) {
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
			targ[targIndex++] = (char)(0x7F & val);
		} else {
			targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
		}
		textHeap.tat[off1] = targIndex;
	}

	private int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex) {
		targ[targIndex++] = (char)val;			

		int len;
		do {
			len = reader.readTextASCII2(targ, targIndex, nextLimit);
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

	public int readASCIIConstant(int token, int readFromIdx) {
		//always return this required value.
		return (token & INSTANCE_MASK) | INIT_VALUE_MASK;
	}
	
	public int readASCIIConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()!=0 ? (token & INSTANCE_MASK)|INIT_VALUE_MASK : token & INSTANCE_MASK);
	}

	public int readUTF8Constant(int token, int readFromIdx) {
		//always return this required value.
		return (token & INSTANCE_MASK) | INIT_VALUE_MASK;
	}
	
	public int readUTF8ConstantOptional(int token, int readFromIdx) {
		return (reader.popPMapBit()!=0 ? (token & INSTANCE_MASK)|INIT_VALUE_MASK : token & INSTANCE_MASK);
	}
	
	public int readASCIIDefault(int token, int readFromIdx) {

		if (reader.popPMapBit()==0) {
			//  1/3 of calls here
			return INIT_VALUE_MASK|(INSTANCE_MASK&token);//use default
		} else {
			int idx = token & INSTANCE_MASK;
			byte val = reader.readTextASCIIByte();
			int tmp = 0x7F&val;
			if (0!=tmp) {//low 7 bits have data
				//real data, this is the most common case;
				///  2/3 of calls here
				textHeap.setZeroLength(idx);				
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
					targ[targIndex++] = (char)tmp;
				} else {
					targIndex = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex);
				}
				textHeap.tat[off1] = targIndex;
			} else {
				readASCIIToHeapNone(idx, val);
			}
			return idx;
		}
	}
	
	public int readASCIIDefaultOptional(int token, int readFromIdx) {
		//for ASCII we don't need special behavior for optional
		return readASCIIDefault(token, readFromIdx); 
	}

	public int readASCIIDeltaOptional(int token, int readFromIdx) {
		return readASCIIDelta(token, readFromIdx);//TODO: need null logic here.
	}

	public int readASCIIDelta(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		
		if (trim>=0) {
			return readASCIITail(idx, trim, readFromIdx);
		} else {
			return readASCIIHead(idx, trim, readFromIdx);
		}
	}

	public int readASCIITail(int token, int readFromIdx) {
		return readASCIITail(token & INSTANCE_MASK, reader.readIntegerUnsigned(), readFromIdx);
	}

	private int readASCIITail(int idx, int trim, int readFromIdx) {
		if (trim>0) {
			textHeap.trimTail(idx, trim);
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
				textHeap.setNull(idx);				
			} else {		
				if (textHeap.isNull(idx)) {
					textHeap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val);
			}
		}
		
		return idx;
	}
	
	public int readASCIITailOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
				
		int tail = reader.readIntegerUnsigned();
		if (0==tail) {
			textHeap.setNull(idx);
			return idx;
		}
		tail--;
				
		textHeap.trimTail(idx, tail);
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
				if (textHeap.isNull(idx)) {
					textHeap.setZeroLength(idx);
				}
				fastHeapAppend(idx, val);
			}
		}
						
		return idx;
	}
	
	private int readASCIIHead(int idx, int trim, int readFromIdx) {
		if (trim<0) {
			textHeap.trimHead(idx, -trim);
		}

		byte value = reader.readTextASCIIByte();
		int offset = idx<<2;
		int nextLimit = textHeap.tat[offset+4];
		
		if (trim>=0) {
			while (value>=0) {
				nextLimit = textHeap.appendTail(offset, nextLimit, (char)value);
				value = reader.readTextASCIIByte();
			}
			textHeap.appendTail(offset, nextLimit, (char)(value&0x7F) );
		} else {
			while (value>=0) {
				textHeap.appendHead(offset, (char)value);
				value = reader.readTextASCIIByte();
			}
			textHeap.appendHead(offset, (char)(value&0x7F) );
		}
		
	//	System.out.println("new ASCII string:"+charDictionary.get(idx, new StringBuilder()));
		
		return idx;
	}


	public int readASCIICopyOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			byte val = reader.readTextASCIIByte();
			if (0!=(val&0x7F)) {
				//real data, this is the most common case;
				textHeap.setZeroLength(idx);				
				fastHeapAppend(idx, val);
			} else {
				readASCIIToHeapNone(idx, val);
			}
		}
		return idx;
	}





	public int readUTF8Default(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(textHeap.rawAccess(), 
					            textHeap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}
	

	public int readUTF8DefaultOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned()-1;
			reader.readTextUTF8(textHeap.rawAccess(), 
					            textHeap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readUTF8Delta(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		//	System.out.println("new UTF8 string:"+charDictionary.get(idx, new StringBuilder()));
		}
		
		return idx;
	}

	public int readUTF8Tail(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned(); 

		//append to tail	
		int targetOffset = textHeap.makeSpaceForAppend(idx, trim, utfLength);
		reader.readTextUTF8(textHeap.rawAccess(), targetOffset, utfLength);
		return idx;
	}
	
	public int readUTF8Copy(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(textHeap.rawAccess(), 
					            textHeap.allocate(idx, length),
					            length);
		}
		return idx;
	}

	public int readUTF8CopyOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {			
			int length = reader.readIntegerUnsigned()-1;
			reader.readTextUTF8(textHeap.rawAccess(), 
					            textHeap.allocate(idx, length),
					            length);
		}
		return idx;
	}


	public int readUTF8DeltaOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		if (0==trim) {
			textHeap.setNull(idx);
			return idx;
		}
		if (trim>0) {
			trim--;//subtract for optional
		}
		
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			//System.err.println("oldString :"+charDictionary.get(idx, new StringBuilder())+" TAIL");
			reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
			//System.err.println("new UTF8 Opp   trim tail "+trim+" added to head "+utfLength+" string:"+charDictionary.get(idx, new StringBuilder()));
		} else {
			//append to head
			//System.err.println("oldString :"+charDictionary.get(idx, new StringBuilder())+" HEAD");
			reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
			//System.err.println("new UTF8 Opp   trim head "+trim+" added to head "+utfLength+" string:"+charDictionary.get(idx, new StringBuilder()));
		}
		
		return idx;
	}

	public int readUTF8TailOptional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerUnsigned();
		if (trim==0) {
			textHeap.setNull(idx);
			return idx;
		} 
		trim--;
		
		int utfLength = reader.readIntegerUnsigned(); //subtract for optional

		//append to tail	
		reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		
		return idx;
	}

	public int readASCII(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		byte val = reader.readTextASCIIByte();
		if (0!=(val&0x7F)) {
			//real data, this is the most common case;
			textHeap.setZeroLength(idx);				
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
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readTextUTF8(textHeap.rawAccess(), 
				            textHeap.allocate(idx, length),
				            length);
		return idx;
	}

	public int readUTF8Optional(int token, int readFromIdx) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned()-1;
		reader.readTextUTF8(textHeap.rawAccess(), 
				            textHeap.allocate(idx, length),
				            length);
		return idx;
	}

	public void reset(int idx) {
		if (null!=textHeap) {
			textHeap.setNull(idx);
		}
	}

	
}
