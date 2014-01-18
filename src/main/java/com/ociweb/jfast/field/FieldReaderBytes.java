package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderBytes {

	private static final int INIT_VALUE_MASK = 0x80000000;
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
		//readASCIIToHeap(idx);
		return idx;
	}

//	private void readBytesToHeap(int idx) {
//		
//		// 0x80 is a null string.
//		// 0x00, 0x80 is zero length string
//		byte val = reader.readTextASCIIByte();
//		if (val==0) {
//			byteDictionary.setZeroLength(idx);
//			//must move cursor off the second byte
//			val = reader.readTextASCIIByte();
//			//at least do a validation because we already have what we need
//			assert((val&0xFF)==0x80);
//		} else {
//			if (val==NULL_STOP) {
//				byteDictionary.setNull(idx);				
//			} else {
//				byteDictionary.setZeroLength(idx);				
//				fastHeapAppend(idx, val);
//			}
//		}
//	}
	
	public int readBytesTail(int token) {
		return readBytesTail(token & INSTANCE_MASK, reader.readIntegerUnsigned());
	}
	
	private int readBytesTail(int idx, int trim) {
		if (trim>0) {
			byteDictionary.trimTail(idx, trim);
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
//			if (val==NULL_STOP) {
//				//nothing to append
//				//charDictionary.setNull(idx);				
//			} else {		
//				if (byteDictionary.isNull(idx)) {
//					byteDictionary.setZeroLength(idx);
//				}
//				fastHeapAppend(idx, val);
//			}
		}
		
		return idx;
	}

	public int readBytesConstant(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesDelta(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesCopy(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesDefault(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesTailOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesConstantOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesDeltaOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesCopyOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int readBytesDefaultOptional(int token) {
		// TODO Auto-generated method stub
		return 0;
	}


}
