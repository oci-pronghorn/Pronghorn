package com.ociweb.jfast.field.util;



public class ValueDictionary {

	public final ValueDictionaryEntry[] entry;
	
	public ValueDictionary(int maxId) {
				
		this.entry = new ValueDictionaryEntry[maxId];
		
		int j = maxId;
		while (--j>=0) {
			this.entry[j] = new ValueDictionaryEntry(this);
		}
		
	}
		
}
