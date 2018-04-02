package com.ociweb.pronghorn.util;

public class TrieParserReaderLocal {

	     private static final ThreadLocal<TrieParserReader> tpr =
	         new ThreadLocal<TrieParserReader>() {
	             @Override protected TrieParserReader initialValue() {
	                boolean alwaysCompletePayloads = true;
					int maxCapturedFields = 63;
					return new TrieParserReader(maxCapturedFields, alwaysCompletePayloads);
	         }
	     };

	     public static TrieParserReader get() {
	         return tpr.get();
	     }	 
}
