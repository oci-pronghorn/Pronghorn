package com.ociweb.jfast.field;

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

}
