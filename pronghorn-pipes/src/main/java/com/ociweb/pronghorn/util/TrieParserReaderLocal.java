package com.ociweb.pronghorn.util;

public class TrieParserReaderLocal {

	     private static final ThreadLocal<TrieParserReader> tpr =
	         new ThreadLocal<TrieParserReader>() {
	             @Override protected TrieParserReader initialValue() {
	                boolean alwaysCompletePayloads = true;
					return new TrieParserReader(alwaysCompletePayloads);
	         }
	     };

	     public static TrieParserReader get() {
	         return tpr.get();
	     }	 
}
