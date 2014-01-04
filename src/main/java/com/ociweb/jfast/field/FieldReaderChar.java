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

	private void readASCIIToHeap(int idx) {
		charDictionary.setZeroLength(idx);
		byte val = reader.readTextASCIIByte();
		while (val>=0) {
			charDictionary.appendTail(idx, (char)val);
			val = reader.readTextASCIIByte();
		}
		//val is last byte
		if (0x80!=val) {
			charDictionary.appendTail(idx, (char)(0x7F & val));
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
			while (b>=0) {
				charDictionary.appendTail(idx, (char)b);
				b = reader.readTextASCIIByte();
			}
			charDictionary.appendTail(idx, (char)(b&0x07));
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
			while (b>=0) {
				charDictionary.appendTail(idx, (char)b);
				b = reader.readTextASCIIByte();
			}
			charDictionary.appendTail(idx, (char)(b&0x07));
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
		while (value>=0) {
			charDictionary.appendTail(idx, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(idx, (char)(value&0x7F) );
				
		return idx;
	}

	public int readASCIITail(int token) {
		int idx = token & INSTANCE_MASK;
		
		charDictionary.trimTail(idx, reader.readIntegerSigned());
				
		byte value = reader.readTextASCIIByte();
		while (value>=0) {
			charDictionary.appendTail(idx, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(idx, (char)(value&0x7F) );
				
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
		while (value>=0) {
			charDictionary.appendTail(idx, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(idx, (char)(value&0x7F) );
				
		return idx;
	}

	public int readASCIITailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		charDictionary.trimTail(idx, reader.readIntegerSigned());
				
		byte value = reader.readTextASCIIByte();
		while (value>=0) {
			charDictionary.appendTail(idx, (char)value);
			value = reader.readTextASCIIByte();
		}
		charDictionary.appendTail(idx, (char)(value&0x7F) );
				
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

	public int readTextASCII(int token) {
		int idx = token & INSTANCE_MASK;
		readASCIIToHeap(idx);
		return idx;
	}
	
	public int readTextASCIIOptional(int token) {
		return readTextASCII(token);
	}

	public int readTextUTF8(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readTextUTF8(charDictionary.rawAccess(), 
				            charDictionary.allocate(idx, length),
				            length);
		return idx;
	}

	public int readTextUTF8Optional(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned()-1;
		reader.readTextUTF8(charDictionary.rawAccess(), 
				            charDictionary.allocate(idx, length),
				            length);
		return idx;
	}

	
}
