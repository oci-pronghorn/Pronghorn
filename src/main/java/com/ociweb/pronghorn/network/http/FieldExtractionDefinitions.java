package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.util.TrieParser;

public class FieldExtractionDefinitions {

	private final TrieParser runtimeParser;
	private int indexCount;
	private long routeValue;
	
	public FieldExtractionDefinitions(boolean trustText, long routeValue) {
		this.runtimeParser = new TrieParser(64, 2, trustText, true);
		this.routeValue = routeValue;
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
	
	///////////////////////////////////////////
	//add default value lookups to routeDef.getRuntimeParser() for any 
	//field which did not appear.  These defaults will have
	//the high bit flag set so the runtime knows the data found elsewhere and
	//not in the payload
	//////////////////////////////////////////	
	public void addDefault(String key, String value) {
		
		//if key is not found continue
		//get next index
		//store value in 4 arrays at that index in each form
		//add to map key bytes and the index with mask.
		
		
	}
	
	
}
