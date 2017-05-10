package com.ociweb.pronghorn.network.config;

public interface HTTPHeader {

	public static final int HEADER_BIT = 1<<28;
	
    int ordinal();
    
    CharSequence readingTemplate();
    CharSequence writingRoot();
    byte[] rootBytes();
    
}
