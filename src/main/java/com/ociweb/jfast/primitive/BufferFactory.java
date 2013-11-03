package com.ociweb.jfast.primitive;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class BufferFactory {

	//build appropriate buffer for the platform, for Android use byte[] 
	//for those that support Unsafe use it.
	
	public static void main(String[] args) {
		System.err.println(getUnsafe());
		
	}
	
	//ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()) another buffer to test
	
	public static Object getUnsafe() {
	    try {
	    	//Must continue to compile on platforms without this class so reflection must be used.
	        Class unsafeClass = Class.forName("sun.misc.Unsafe");
	        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
	        unsafeField.setAccessible(true);
	        Object unsafeObj = unsafeField.get(unsafeClass);
	 
	        //allocateMemory(long) returns long addr
	        //putByte(long,byte)
	        //getByte(long)
	        //copyMemory(long,long,long) src, target, size
	        
	        for(Method m:unsafeClass.getMethods()) {
	        	System.err.println("method:"+m);
	        }
	        
	        return unsafeObj;
	    }
	    catch (Exception e) {
	        return null;//UNSAFE_ERROR_CODE;
	    }
	}
	
}
