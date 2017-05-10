package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.util.TrieParser;

public class RouteDef {

	private final TrieParser runtimeParser;
	private int indexCount;
	
	public RouteDef(boolean trustText) {
		runtimeParser = new TrieParser(64, 2, trustText, true);
	}
	
	public TrieParser getRuntimeParser() {
		return runtimeParser;
	}
	
	public void setIndexCount(int indexCount) {
		this.indexCount = indexCount;
	}
	
	public int getIndexCount() {
		return this.indexCount;
	}
	
}
