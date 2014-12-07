package com.ociweb.jfast.stream;

import com.ociweb.pronghorn.ring.RingBuffer;


public interface GeneratorDriving {

    int getActiveScriptCursor();
    void setActiveScriptCursor(int cursor);
    
        
    void runBeginMessage();
    void runFromCursor(RingBuffer mockRB);
    
    int getActiveToken();
    long getActiveFieldId();
    String getActiveFieldName();
    int scriptLength();
    
}
