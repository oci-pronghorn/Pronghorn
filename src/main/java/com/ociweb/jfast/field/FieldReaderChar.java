package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderChar {

	private final PrimitiveReader reader;
	private final TextHeap charDictionary;
	
	public FieldReaderChar(PrimitiveReader reader, TextHeap charDictionary) {
		this.reader = reader;
		this.charDictionary = charDictionary;
	}

}
