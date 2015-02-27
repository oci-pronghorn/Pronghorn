package com.ociweb.pronghorn.ring.stream;

import java.nio.ByteBuffer;

public class StreamingConsumerAdapter implements StreamingConsumer {

	StringBuilder tempStringBuilder =  new StringBuilder(128); 
	ByteBuffer tempByteBuffer = ByteBuffer.allocate(512);
	
	@Override
	public boolean paused() {
		return false;
	}

	@Override
	public void visitTemplateOpen(String name, long id) {
	}

	@Override
	public void visitFragmentOpen(String name, long id) {
	}

	@Override
	public void visitFragmentClose(String name, long id) {
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
	}

	@Override
	public void visitSequenceClose(String name, long id) {
	}

	@Override
	public void visitSignedInteger(String string, long id, int value) {
	}

	@Override
	public void visitUnsignedInteger(String string, long id, long value) {
	}

	@Override
	public void visitOptionalSignedInteger(String string, long id, int value) {
	}

	@Override
	public void visitOptionalUnsignedInteger(String string, long id, long value) {	
	}

	@Override
	public void visitSignedLong(String string, long id, long value) {
	}

	@Override
	public void visitUnsignedLong(String string, long id, long value) {	
	}

	@Override
	public void visitOptinoalSignedLong(String string, long id, long value) {	
	}

	@Override
	public void visitOptionalUnsignedLong(String string, long id, long value) {	
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {	
	}

	@Override
	public void visitOptionalDecimal(String name, long id, int exp, long mant) {	
	}


	@Override
	public void visitOptionalASCII(String name, long id, Appendable value) {
	}

	@Override
	public void visitUTF8(String name, long id, Appendable value) {
	}

	@Override
	public void visitOptionalUTF8(String name, long id, Appendable value) {
	}

	@Override
	public Appendable targetASCII(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public Appendable targetUTF8(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
		tempByteBuffer.clear();
		if (tempByteBuffer.capacity()<length) {
			tempByteBuffer = ByteBuffer.allocate(length*2);
		}
		return tempByteBuffer;
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
	}

	@Override
	public void visitOptionalBytes(String name, long id, ByteBuffer value) {
	}

	@Override
	public void visitASCII(String name, long id, Appendable value) {
	}

	@Override
	public Appendable targetOptionalASCII(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public Appendable targetOptionalUTF8(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public ByteBuffer targetOptionalBytes(String name, long id, int length) {
		tempByteBuffer.clear();
		if (tempByteBuffer.capacity()<length) {
			tempByteBuffer = ByteBuffer.allocate(length*2);
		}
		return tempByteBuffer;
	}



}
