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

}
