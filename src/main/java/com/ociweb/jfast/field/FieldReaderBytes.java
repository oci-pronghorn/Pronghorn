package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderBytes {

	private static final int INIT_VALUE_MASK = 0x80000000;
	final byte NULL_STOP = (byte)0x80;
	private final PrimitiveReader reader;
	private final ByteHeap byteDictionary;
	private final int INSTANCE_MASK;
	
	public FieldReaderBytes(PrimitiveReader reader, ByteHeap byteDictionary) {
		assert(byteDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(byteDictionary.itemCount()));
		
		this.INSTANCE_MASK = (byteDictionary.itemCount()-1);
		
		this.reader = reader;
		this.byteDictionary = byteDictionary;
	}

	public int readBytes(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readByteData(byteDictionary.rawAccess(), 
							byteDictionary.allocate(idx, length),
				            length);
		return idx;
	}


	public int readBytesTail(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int length = reader.readIntegerUnsigned(); 

		//append to tail	
		int targetOffset = byteDictionary.makeSpaceForAppend(idx, trim, length);
		reader.readByteData(byteDictionary.rawAccess(), targetOffset, length);
		return idx;
	}
	
	public int readBytesConstant(int token) {
		//always return this required value
		return token & INSTANCE_MASK;
	}

	public int readBytesDelta(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned();
		if (trim>=0) {
			//append to tail
			reader.readByteData(byteDictionary.rawAccess(), byteDictionary.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readByteData(byteDictionary.rawAccess(), byteDictionary.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readBytesCopy(int token) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {
			int length = reader.readIntegerUnsigned();
			reader.readByteData(byteDictionary.rawAccess(), 
								byteDictionary.allocate(idx, length),
					            length);
		}
		return idx;
	}

	public int readBytesDefault(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned();
			reader.readByteData(byteDictionary.rawAccess(), 
								byteDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readBytesOptional(int token) {
		return readBytes(token);
	}

	public int readBytesTailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned()-1; //subtract for optional

		//append to tail	
		reader.readByteData(byteDictionary.rawAccess(), byteDictionary.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		
		return idx;
	}

	public int readBytesConstantOptional(int token) {
		return (reader.popPMapBit()==0 ? (token & INSTANCE_MASK)|INIT_VALUE_MASK : token & INSTANCE_MASK);
	}

	public int readBytesDeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		int utfLength = reader.readIntegerUnsigned()-1; //subtract for optional
		if (trim>=0) {
			//append to tail
			reader.readByteData(byteDictionary.rawAccess(), byteDictionary.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readByteData(byteDictionary.rawAccess(), byteDictionary.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readBytesCopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {			
			int length = reader.readIntegerUnsigned()-1;
			reader.readByteData(byteDictionary.rawAccess(), 
								byteDictionary.allocate(idx, length),
					            length);
		}	
		return idx;
	}

	public int readBytesDefaultOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			
			int length = reader.readIntegerUnsigned()-1;
			reader.readByteData(byteDictionary.rawAccess(), 
								byteDictionary.allocate(idx, length),
					            length);
						
			return idx;
		}
	}


}
