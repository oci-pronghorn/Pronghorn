package com.ociweb.pronghorn.util;

public interface TrieParserVisitor {

	void visit(byte[] backing, int length, long value);
	
}
