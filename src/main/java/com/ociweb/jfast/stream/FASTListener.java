package com.ociweb.jfast.stream;

public interface FASTListener {

    void fragment(int templateId, FASTRingBuffer buffer);
    void fragment();
    
}
