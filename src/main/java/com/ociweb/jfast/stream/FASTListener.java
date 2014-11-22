package com.ociweb.jfast.stream;

import com.ociweb.pronghorn.ring.RingBuffer;

public interface FASTListener {

    void fragment(int templateId, RingBuffer buffer);
    void fragment();
    
}
