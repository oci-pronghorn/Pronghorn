package com.ociweb.pronghorn.util;

public interface TrieKeyable<T extends Enum<T>& TrieKeyable> {

	public CharSequence getKey();
	
	
	
}
