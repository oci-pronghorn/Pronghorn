package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderChar {

	private static final int INIT_VALUE_MASK = 0x80000000;
	private final PrimitiveReader reader;
	private final TextHeap charDictionary;
	private final int INSTANCE_MASK;
	
	public FieldReaderChar(PrimitiveReader reader, TextHeap charDictionary) {
		
		assert(charDictionary.textCount()<TokenBuilder.MAX_INSTANCE);
		assert(isPowerOfTwo(charDictionary.textCount()));
		
		this.INSTANCE_MASK = (charDictionary.textCount()-1);
		
		this.reader = reader;
		this.charDictionary = charDictionary;
	}
	
	public TextHeap textHeap() {
		return charDictionary;
	}
	
	static boolean isPowerOfTwo(int length) {
		
		while (0==(length&1)) {
			length = length>>1;
		}
		return length==1;
	}

	public int readASCIICopy(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			readASCIIToHeap(idx);
		}
		return idx;
	}

	final byte NULL_STOP = (byte)0x80;
	
	private void readASCIIToHeap(int idx) {
		// 0x80 is a null string.
		// 0x00, 0x80 is zero length string
		byte val = reader.readTextASCIIByte();
		if (val==0) {
			charDictionary.setZeroLength(idx);
			//must move cursor off the second byte
			val = reader.readTextASCIIByte();
			//at least do a validation because we already have what we need
			assert((val&0xFF)==0x80);
		} else {
			if (val==NULL_STOP) {
				charDictionary.setNull(idx);				
			} else {
				charDictionary.setZeroLength(idx);
				//will be converted to real text if a char is found
				int offset = charDictionary.offset(idx);
				while (val>=0) {
					charDictionary.appendTail(offset, (char)val);
					val = reader.readTextASCIIByte();
				}
				//val is last byte
				charDictionary.appendTail(offset, (char)(0x7F & val));
			}
		}
	}
	
	public int readUTF8Copy(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
			
		}
		
		return idx;
	}

	public int readASCIIConstant(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			charDictionary.setZeroLength(idx);
			byte b = reader.readTextASCIIByte();
			int offset = charDictionary.offset(idx);
			while (b>=0) {
				charDictionary.appendTail(offset, (char)b);
				b = reader.readTextASCIIByte();
			}
			charDictionary.appendTail(offset, (char)(b&0x07));
			return idx;
		}
	}
	
	public int readASCIIDefault(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			charDictionary.setZeroLength(idx);
			byte b = reader.readTextASCIIByte();
			int offset = charDictionary.offset(idx);
			while (b>=0) {
				charDictionary.appendTail(offset, (char)b);
				b = reader.readTextASCIIByte();
			}
			charDictionary.appendTail(offset, (char)(b&0x07));
			return idx;
		}
	}

	public int readASCIIDelta(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		
		if (trim>=0) {
			charDictionary.trimTail(idx, trim);
		} else {
			charDictionary.trimHead(idx, -trim);
		}
		
		byte value = reader.readTextASCIIByte();
		int offset = charDictionary.offset(idx);
		while (value>=0) {
			charDictionary.appendTail(offset, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(offset, (char)(value&0x7F) );
				
		return idx;
	}

	public int readASCIITail(int token) {
		int idx = token & INSTANCE_MASK;
		
		charDictionary.trimTail(idx, reader.readIntegerSigned());
				
		byte value = reader.readTextASCIIByte();
		int offset = charDictionary.offset(idx);
		while (value>=0) {
			charDictionary.appendTail(offset, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(offset, (char)(value&0x7F) );
				
		return idx;
	}

	public int readASCIICopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			readASCIIToHeap(idx);
		}
		return idx;
	}

	public int readASCIIDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readASCIIDeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		
		if (trim>=0) {
			charDictionary.trimTail(idx, trim);
		} else {
			charDictionary.trimHead(idx, -trim);
		}
		
		byte value = reader.readTextASCIIByte();
		int offset = charDictionary.offset(idx);
		while (value>=0) {
			charDictionary.appendTail(offset, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(offset, (char)(value&0x7F) );
				
		return idx;
	}

	public int readASCIITailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		charDictionary.trimTail(idx, reader.readIntegerSigned());
				
		byte value = reader.readTextASCIIByte();
		int offset = charDictionary.offset(idx);
		while (value>=0) {
			charDictionary.appendTail(offset, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(offset, (char)(value&0x7F) );
				
		return idx;
	}


	public int readUTF8Constant(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
				
	}

	public int readUTF8Default(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readUTF8Delta(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail	
			reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForAppend(trim, idx, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForPrepend(trim, idx, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readUTF8Tail(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned(); 

		//append to tail	
		reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForAppend(trim, idx, utfLength), utfLength);
		
		return idx;
	}

	public int readUTF8CopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
			
		}
		
		return idx;
	}

	public int readUTF8DefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.rawAccess(), 
					            charDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readUTF8DeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned()-1; //subtract for optional
		if (trim>=0) {
			//append to tail	
			reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForAppend(trim, idx, utfLength), utfLength);
		} else {
			//append to head
			reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForPrepend(trim, idx, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readUTF8TailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned()-1; //subtract for optional

		//append to tail	
		reader.readTextUTF8(charDictionary.rawAccess(), charDictionary.makeSpaceForAppend(trim, idx, utfLength), utfLength);
		
		return idx;
	}

	public int readASCII(int token) {
		int idx = token & INSTANCE_MASK;
		readASCIIToHeap(idx);
		return idx;
	}
	
	public int readTextASCIIOptional(int token) {
		return readASCII(token);
	}

	public int readUTF8(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readTextUTF8(charDictionary.rawAccess(), 
				            charDictionary.allocate(idx, length),
				            length);
		return idx;
	}

	public int readUTF8Optional(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned()-1;
		reader.readTextUTF8(charDictionary.rawAccess(), 
				            charDictionary.allocate(idx, length),
				            length);
		return idx;
	}

	
}
