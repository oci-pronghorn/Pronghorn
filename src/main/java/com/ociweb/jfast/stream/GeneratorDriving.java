package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveReader;

public interface GeneratorDriving {

    int getActiveScriptCursor();
    void setActiveScriptCursor(int cursor);
    
    void setActiveScriptLimit(int limit);
        
    void runBeginMessage();
    void runFromCursor();
    
    int getActiveToken();
    int getActiveFieldId();
    String getActiveFieldName();
    int scriptLength();
    
}
