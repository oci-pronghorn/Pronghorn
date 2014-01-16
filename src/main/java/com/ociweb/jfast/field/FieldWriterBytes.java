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
		// TODO Auto-generated method stub
		
	}

	public void writeBytesTail(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesConstant(int token) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDelta(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesCopy(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDefault(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesOptional(ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesTailOptional(int token, ByteBuffer value) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesConstantOptional(int token) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
	}

	public void writeBytesTail(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDelta(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesCopy(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeBytesDefault(int token, byte[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}


}
