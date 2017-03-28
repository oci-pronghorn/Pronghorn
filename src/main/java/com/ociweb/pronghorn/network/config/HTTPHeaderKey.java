package com.ociweb.pronghorn.network.config;

public interface HTTPHeaderKey {

    int ordinal();
    @Deprecated
    CharSequence getKey();  
    @Deprecated
    CharSequence getRoot();
    
    CharSequence readingTemplate();
    CharSequence writingRoot();
    
}
