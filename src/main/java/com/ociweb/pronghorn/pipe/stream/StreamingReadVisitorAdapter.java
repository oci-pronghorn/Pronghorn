package com.ociweb.pronghorn.pipe.stream;

import java.nio.ByteBuffer;

public class StreamingReadVisitorAdapter implements StreamingReadVisitor {

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
	public void visitTemplateClose(String name, long id) {
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
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
	public void visitSignedInteger(String name, long id, int value) {
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
	}

	@Override
	public void visitUnsignedLong(String name, long id, long value) {	
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {	
	}

	@Override
	public void visitUTF8(String name, long id, Appendable value) {
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
	public void visitASCII(String name, long id, Appendable value) {
	}

	@Override
	public void startup() {
	}

	@Override
	public void shutdown() {
	}

}
