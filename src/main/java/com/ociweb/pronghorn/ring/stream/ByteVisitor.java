package com.ociweb.pronghorn.ring.stream;

public interface ByteVisitor {

	public void visit(byte[] data, int offset, int length);
	public void visit(byte[] data1, int offset1, int length1, int offset2, int length2);

	public void close();

}
