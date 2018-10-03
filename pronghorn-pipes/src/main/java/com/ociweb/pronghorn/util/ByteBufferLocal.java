package com.ociweb.pronghorn.util;

import java.nio.ByteBuffer;

public class ByteBufferLocal {

	     private static final ThreadLocal<ByteBuffer> tpr =
	         new ThreadLocal<ByteBuffer>() {
	             @Override protected ByteBuffer initialValue() {
					return null;
	         }
	     };

	     public static ByteBuffer get(int size) {
	    	 ByteBuffer b  = tpr.get();
	    	 if (b==null || b.capacity()<size) {
	    		 b = ByteBuffer.allocateDirect(size);
	    		 tpr.set(b);;
	    	 }
	         return b;
	     }	 
	     
}
