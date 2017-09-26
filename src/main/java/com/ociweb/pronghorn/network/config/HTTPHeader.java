package com.ociweb.pronghorn.network.config;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface HTTPHeader {

	public static final int HEADER_BIT = 1<<28;
	
    int ordinal();
    
    CharSequence readingTemplate();
    
    CharSequence writingRoot();
    <A extends Appendable> A writeValue(A target, HTTPSpecification httpSpec, ChannelReader reader);
    
    byte[] rootBytes();
    
}
