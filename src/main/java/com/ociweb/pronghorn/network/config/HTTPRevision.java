package com.ociweb.pronghorn.network.config;

public interface HTTPRevision {
    
	static final int BITS = 4;//max of 16 supported
	static final int MASK = (1<<4)-1;
	
	int ordinal();
    CharSequence getKey(); 
    byte[] getBytes();
    
   
}
