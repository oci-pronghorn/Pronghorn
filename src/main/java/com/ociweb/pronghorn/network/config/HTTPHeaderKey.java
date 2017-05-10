package com.ociweb.pronghorn.network.config;

public interface HTTPHeaderKey {

	public static final int HEADER_BIT = 1<<28;
	
    int ordinal();
    @Deprecated
    CharSequence getKey();  
    @Deprecated
    CharSequence getRoot();
    
    CharSequence readingTemplate();
    CharSequence writingRoot();
    
}
