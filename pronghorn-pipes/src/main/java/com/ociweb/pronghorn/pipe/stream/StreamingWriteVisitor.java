package com.ociweb.pronghorn.pipe.stream;

import java.nio.ByteBuffer;


public interface StreamingWriteVisitor {

	boolean paused();

	int pullMessageIdx();
	
	boolean isAbsent(String name, long id); //is field absent (eg null) may need one of these per type

	long pullSignedLong(String name, long id);
    long pullUnsignedLong(String name, long id);
    int pullSignedInt(String name, long id);
    int pullUnsignedInt(String name, long id);    
    long pullDecimalMantissa(String name, long id);
    int pullDecimalExponent(String name, long id);
    CharSequence pullASCII(String name, long id);
    CharSequence pullUTF8(String name, long id);
    ByteBuffer pullByteBuffer(String name, long id);
    int pullSequenceLength(String name, long id);
	
	void startup();

	void shutdown();

    void templateClose(String name, long id);
    void sequenceClose(String name, long id);
    void fragmentClose(String name, long id);
    void fragmentOpen(String string, long l);
	
	
}
