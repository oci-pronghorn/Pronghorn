package com.ociweb.jfast.stream;

import com.ociweb.jfast.ring.FASTRingBuffer;

public interface FASTListener {

    void fragment(int templateId, FASTRingBuffer buffer);
    void fragment();
    
}
