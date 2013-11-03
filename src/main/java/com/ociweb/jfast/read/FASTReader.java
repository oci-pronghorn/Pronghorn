package com.ociweb.jfast.read;

import java.nio.ByteBuffer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.ValueDictionary;

public interface FASTReader {
	
	void visitOptional(ByteBuffer source, FASTAccept visitor, ValueDictionary dictionary);
	void visitPresent(ByteBuffer source, FASTAccept visitor, ValueDictionary dictionary);
	
}
