package com.ociweb.jfast.field;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterBytes {

	private final ByteHeap heap;
	private final PrimitiveWriter writer;
	private final int INSTANCE_MASK;
	
	public FieldWriterBytes(PrimitiveWriter writer, ByteHeap byteDictionary) {
		assert(byteDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(byteDictionary.itemCount()));
		
		this.INSTANCE_MASK = (byteDictionary.itemCount()-1);
		this.heap = byteDictionary;
		this.writer = writer;
	}

	public void writeNull(int token) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytes(ByteBuffer value) {
		//writer.writeIntegerUnsigned(length);
		//writer.writeTextUTF(value,offset,length);
	}

	public void writeBytesTail(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesConstant(int token) {
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}

	public void writeBytesDelta(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesCopy(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDefault(int token, ByteBuffer value) {

	}

	public void writeBytesOptional(ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesTailOptional(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesConstantOptional(int token) {
		writer.writePMapBit((byte)1);
		//the writeNull will take care of the rest.
	}

	public void writeBytesDeltaOptional(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesCopyOptional(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDefaultOptional(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesOptional(byte[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length+1);
		writer.writeByteArrayData(value,offset,length);
	}

	public void writeBytesTailOptional(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDeltaOptional(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesCopyOptional(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDefaultOptional(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytes(byte[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length);
		writer.writeByteArrayData(value,offset,length);
	}

	public void writeBytesTail(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
	//	writeBytesTail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length, 0);

		
//		int trimTail = heap.length(idx)-headCount;
//		writer.writeIntegerUnsigned(trimTail);
//		
//		int valueSend = length-headCount;
//		int startAfter = offset+headCount;
//		int sendLen = valueSend+optional;
//		
//		writeUTF8Tail(idx, trimTail, valueSend, value, startAfter, sendLen);
	}

	public void writeBytesDelta(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
	//		writeBytesTail(idx, headCount, value, offset+headCount, length, 0);
		} else {
	//		writeBytesHead(idx, tailCount, value, offset, length, 0);
		}
	}

	
	public void writeBytesCopy(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		}
		else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeByteArrayData(value,offset,length);
			heap.set(idx, value, offset, length);
		}
	}

	public void writeBytesDefault(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeByteArrayData(value,offset,length);
		}
	}


}
