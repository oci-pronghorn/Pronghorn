package com.ociweb.jfast.example;

public class ExamplePOJO {
	
	final long    		 longValue;//each needs a null?
	final int       	 intValue;
	final byte[]  		 bytesValue;
	final String 	     charSequenceValue;
	
	final long  	     mantissaValue; //TODO: may want to make new money type?
	final int   		 exponentValue;
	
	
	public ExamplePOJO(long longValue, int intValue, 
			            byte[] bytesValue, String charSequenceValue,
			            long mantissaValue, int exponentValue) {
		super();
		this.longValue = longValue;
		this.intValue = intValue;
		this.bytesValue = bytesValue;
		this.charSequenceValue = charSequenceValue;
		this.mantissaValue = mantissaValue;
		this.exponentValue = exponentValue;
	}


	public ExamplePOJO(int seed) {
		this(seed,seed,
			  new byte[] {(byte)(seed>>24),(byte)(seed>>16),(byte)(seed>>8),(byte)seed},
			  String.valueOf(seed),
			  seed<<16,
			  4);
	}
		
	
	
}
