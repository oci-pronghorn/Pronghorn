package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.util.ByteConsumer;

public interface JSONStreamVisitor {

	void nameSeparator();

	void endObject();

	void beginObject();

	void beginArray();

	void endArray();

	void valueSeparator();

	void whiteSpace(byte b);

	void literalTrue();

	void literalNull();

	void literalFalse();

	void numberValue(long m, byte e);

	void stringBegin();

	ByteConsumer stringAccumulator();

	void stringEnd();

	void customString(int id);
	
}
