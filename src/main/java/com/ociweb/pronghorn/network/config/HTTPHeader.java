package com.ociweb.pronghorn.network.config;

import com.ociweb.pronghorn.pipe.BlobReader;

public interface HTTPHeader {

	public static final int HEADER_BIT = 1<<28;
	
    int ordinal();
    
    CharSequence readingTemplate();
    
    CharSequence writingRoot();
    <A extends Appendable> A writeValue(A target, BlobReader reader);
    
    byte[] rootBytes();
    
}
