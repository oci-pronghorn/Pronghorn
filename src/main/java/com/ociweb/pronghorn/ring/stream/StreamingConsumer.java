package com.ociweb.pronghorn.ring.stream;

import java.nio.ByteBuffer;

public interface StreamingConsumer {

	/**
	 * While this is true no more fragments will be read.  
	 * Since fragments are atomic the current one will continue to its natural end.
	 * 
	 * @return
	 */
	boolean paused();

	void visitTemplateOpen(String name, long id);

	void visitFragmentOpen(String name, long id);

	void visitFragmentClose(String name, long id);

	void visitSequenceOpen(String name, long id, int length);
	
	void visitSequenceClose(String name, long id);	

	void visitSignedInteger(String name, long id, int value);

	void visitUnsignedInteger(String name, long id, long value);

	//this method will NOT be called if the value is absent from the stream
	void visitOptionalSignedInteger(String name, long id, int value);

	//this method will NOT be called if the value is absent from the stream
	void visitOptionalUnsignedInteger(String name, long id, long value);

	void visitSignedLong(String name, long id, long value);

	///caution Java does not support unsigned long and high bit may be set
	void visitUnsignedLong(String name, long id, long value);
	
	//this method will NOT be called if the value is absent from the stream
	void visitOptinoalSignedLong(String name, long id, long value);

	///caution Java does not support unsigned long and high bit may be set
	//this method will NOT be called if the value is absent from the stream
	void visitOptionalUnsignedLong(String name, long id, long value);

	void visitDecimal(String name, long id, int exp, long mant);
	
	//this method will NOT be called if the value is absent from the stream
	void visitOptionalDecimal(String name, long id, int exp, long mant);
	
	//the ASCII text is written to the target Appendable 
	Appendable targetASCII(String name, long id);
	void visitASCII(String name, long id, Appendable value);
	
	//the ASCII text is written to the target Appendable 
	//this method will NOT be called if the value is absent from the stream
	Appendable targetOptionalASCII(String name, long id);
	void visitOptionalASCII(String name, long id, Appendable target);
	
	//the UTF8 text is written to the target Appendable 
	Appendable targetUTF8(String name, long id);
	void visitUTF8(String name, long id, Appendable target);
	
	//the UTF8 text is written to the target Appendable 
	//this method will NOT be called if the value is absent from the stream
	Appendable targetOptionalUTF8(String name, long id);
	void visitOptionalUTF8(String name, long id, Appendable target);
	
	//the bytes are are written to the target ByteBuffer that is also returned.
	ByteBuffer targetBytes(String name, long id);
	void visitBytes(String name, long id, ByteBuffer target);
	
	//the bytes are written to the target ByteBuffer that is also returned.
	//this method will NOT be called if the value is absent from the stream
	ByteBuffer targetOptionalBytes(String name, long id);
	void visitOptionalBytes(String name, long id, ByteBuffer target);


	
	
}
