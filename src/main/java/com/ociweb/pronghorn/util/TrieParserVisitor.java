package com.ociweb.pronghorn.util;

public interface TrieParserVisitor {

	void visit(byte[] pattern, int length, long value);
	
}
