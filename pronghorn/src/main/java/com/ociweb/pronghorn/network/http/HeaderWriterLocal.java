package com.ociweb.pronghorn.network.http;

public class HeaderWriterLocal {

    private static final ThreadLocal<HeaderWriter> tpr =
	         new ThreadLocal<HeaderWriter>() {
	             @Override protected HeaderWriter initialValue() {
					return new HeaderWriter();
	         }
	     };

	     public static HeaderWriter get() {
	         return tpr.get();
	     }	
}
