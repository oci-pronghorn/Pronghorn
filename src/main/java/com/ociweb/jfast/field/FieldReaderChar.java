package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderChar {

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
	
	static boolean isPowerOfTwo(int length) {
		
		while (0==(length&1)) {
			length = length>>1;
		}
		return length==1;
	}

	public void readASCIICopy(int token, TextDelegate delegate) {
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
		delegate.setValue(idx);		
	}
	
	public int readASCIICopy(int token, char[] target, int offset) {
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
		
		return charDictionary.get(idx, target, offset);
		
	}

	public int readASCIIConstant(int token, char[] target, int offset) {
		if (reader.popPMapBit()==0) {
			return charDictionary.get(token & INSTANCE_MASK, target, offset);
		} else {
			return reader.readTextASCII(target,offset); 
		}
	}

	public int readASCIIDefault(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIIDelta(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIITail(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIICopyOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIIDefaultOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIIDeltaOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readASCIITailOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8Copy(int token, char[] target, int offset) {
		
		if (reader.popPMapBit()==0) {
			return charDictionary.get(token & INSTANCE_MASK, target, offset);
		} else {
			//don't know length so must write it someplace to find out.
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(target, offset, length);
			//update dictionary requires copy. TODO: re-evaluate this when the rest of the methods are done.
			charDictionary.set(token & INSTANCE_MASK, target, offset, length);
			return length;
		}
		
	}

	public int readUTF8Constant(int token, char[] target, int offset) {
		
		if (reader.popPMapBit()==0) {
			return charDictionary.get(token & INSTANCE_MASK, target, offset);
		} else {
			int length = reader.readIntegerUnsigned();
			reader.readTextUTF8(target, offset, length);
			return length;
		}
		
	}

	public int readUTF8Default(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8Delta(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8Tail(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8CopyOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8DefaultOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8DeltaOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readUTF8TailOptional(int token, char[] target, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Object readASCIICopy(int token, Appendable target) {
		// TODO Auto-generated method stub
		return null;
	}

	public void readASCIIConstant(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIIDefault(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIIDelta(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIITail(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIICopyOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIIDefaultOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIIDeltaOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readASCIITailOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8Copy(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8Constant(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8Default(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8Delta(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8Tail(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8CopyOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8DefaultOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8DeltaOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

	public void readUTF8TailOptional(int token, Appendable target) {
		// TODO Auto-generated method stub
		
	}

}
