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
		return idx;
	}
	
	public int readUTF8Copy(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()!=0) {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.data, 
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
		
		
		
		
		return 0;
	}

	public int readASCIITail(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readASCIICopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		
		return 0;
	}

	public int readASCIIDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readASCIIDeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readASCIITailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}



	public int readUTF8Constant(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(charDictionary.data, 
					            charDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
				
	}

	public int readUTF8Default(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8Delta(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8Tail(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8CopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8DefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8DeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		return 0;
	}

	public int readUTF8TailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		
		
		
		return 0;
	}

	public int readTextASCII(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readTextUTF8(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readTextUTF8Optional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	
}
