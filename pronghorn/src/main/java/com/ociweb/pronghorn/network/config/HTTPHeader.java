package com.ociweb.pronghorn.network.config;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface HTTPHeader {
	
    int ordinal();
    
    CharSequence readingTemplate();
    
    CharSequence writingRoot();
    
    <A extends Appendable> A writeValue(A target, HTTPSpecification<?,?,?,?> httpSpec, ChannelReader reader);
    
    void skipValue(ChannelReader reader);
    <A extends Appendable> A consumeValue(ChannelReader reader, A target);
    long consumeValue(ChannelReader reader);
    
    byte[] rootBytes();
    
}
