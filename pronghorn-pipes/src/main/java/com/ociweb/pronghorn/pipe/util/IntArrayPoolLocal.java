package com.ociweb.pronghorn.pipe.util;

public class IntArrayPoolLocal {

	private static final int maxSize = 32;
	
    private static final ThreadLocal<IntArrayPool> tpr =
	         new ThreadLocal<IntArrayPool>() {
	             @Override protected IntArrayPool initialValue() {
					return new IntArrayPool(maxSize);
	         }
	     };

	     public static IntArrayPool get() {
	         return tpr.get();
	     }	 
}
