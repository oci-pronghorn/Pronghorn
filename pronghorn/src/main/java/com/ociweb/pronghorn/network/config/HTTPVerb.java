package com.ociweb.pronghorn.network.config;

public interface HTTPVerb {
    
	static final int BITS = 4;//max of 16 verbs supported
	static final int MASK = (1<<4)-1;
	
    int ordinal();
    CharSequence getKey(); 
    
}
