package com.ociweb.pronghorn.util;

public class CharSequenceToUTF8Local {

	     private static final ThreadLocal<CharSequenceToUTF8> tpr =
	         new ThreadLocal<CharSequenceToUTF8>() {
	             @Override protected CharSequenceToUTF8 initialValue() {
	            	return new CharSequenceToUTF8();
	         }
	     };

	     public static CharSequenceToUTF8 get() {
	         return tpr.get();
	     }	 
}
