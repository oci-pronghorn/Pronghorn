//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderBytes {

	private static final int INIT_VALUE_MASK = 0x80000000;
	final byte NULL_STOP = (byte)0x80;
	private final PrimitiveReader reader;
	private final ByteHeap byteHeap;
	private final int INSTANCE_MASK;
	
	//TODO: improvement reader/writer bytes/chars should never build this object when it is not in use.
	public FieldReaderBytes(PrimitiveReader reader, ByteHeap byteDictionary) {
		assert(null==byteDictionary || byteDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==byteDictionary || FieldReaderInteger.isPowerOfTwo(byteDictionary.itemCount()));
		
		this.INSTANCE_MASK = null==byteDictionary ? 0 :Math.min(TokenBuilder.MAX_INSTANCE, byteDictionary.itemCount()-1);
		
		this.reader = reader;
		this.byteHeap = byteDictionary;
	}

	public int readBytes(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned();
		reader.readByteData(byteHeap.rawAccess(), 
							byteHeap.allocate(idx, length),
				            length);
		return idx;
	}


	public int readBytesTail(int token) {
		
		//return readBytesCopy(token);
		
		int idx = token & INSTANCE_MASK;
				
		int trim = reader.readIntegerUnsigned();
		int length = reader.readIntegerUnsigned(); 
		
		//append to tail	
		int targetOffset = byteHeap.makeSpaceForAppend(idx, trim, length);
		reader.readByteData(byteHeap.rawAccess(), targetOffset, length);
				
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
			reader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readBytesCopy(int token) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {
			int length = reader.readIntegerUnsigned();
			reader.readByteData(byteHeap.rawAccess(), 
								byteHeap.allocate(idx, length),
					            length);
		}
		return idx;
	}

	public int readBytesDefault(int token) {
		int idx = token & INSTANCE_MASK;
		
		if (reader.popPMapBit()==0) {
			//System.err.println("z");
			return idx|INIT_VALUE_MASK;//use constant
		} else {
			//System.err.println("a");
			int length = reader.readIntegerUnsigned();
			if (length>65535 || length<0) {
				throw new FASTException("do you really want ByteArray of size "+length);
			}
			assert(length>=0) : "Unsigned int are never negative";
			reader.readByteData(byteHeap.rawAccess(), 
								byteHeap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public int readBytesOptional(int token) {
		int idx = token & INSTANCE_MASK;
		int length = reader.readIntegerUnsigned()-1;
		reader.readByteData(byteHeap.rawAccess(), 
							byteHeap.allocate(idx, length),
				            length);
		return idx;
	}

	public int readBytesTailOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerUnsigned();
		if (trim==0) {
			byteHeap.setNull(idx);
			return idx;
		} 
		trim--;
		
		int utfLength = reader.readIntegerUnsigned();

		//append to tail	
		reader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		
		return idx;
	}

	public int readBytesConstantOptional(int token) {
		return (reader.popPMapBit()==0 ? (token & INSTANCE_MASK)|INIT_VALUE_MASK : token & INSTANCE_MASK);
	}

	public int readBytesDeltaOptional(int token) {
		int idx = token & INSTANCE_MASK;
		
		int trim = reader.readIntegerSigned();
		if (0==trim) {
			byteHeap.setNull(idx);
			return idx;
		}
		if (trim>0) {
			trim--;//subtract for optional
		}
		
		int utfLength = reader.readIntegerUnsigned();

		if (trim>=0) {
			//append to tail
			reader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
		} else {
			//append to head
			reader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
		}
		
		return idx;
	}

	public int readBytesCopyOptional(int token) {
		int idx = token & INSTANCE_MASK;
		if (reader.popPMapBit()!=0) {			
			int length = reader.readIntegerUnsigned()-1;
			reader.readByteData(byteHeap.rawAccess(), 
								byteHeap.allocate(idx, length),
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
			if (length>65535 || length<0) {
				throw new FASTException("do you really want ByteArray of size "+length);
			}
			reader.readByteData(byteHeap.rawAccess(), 
								byteHeap.allocate(idx, length),
					            length);
						
			return idx;
		}
	}

	public ByteHeap byteHeap() {
		return byteHeap;
	}


}
