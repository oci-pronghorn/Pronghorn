package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveReader;

public interface GeneratorDriving {

    int getActiveScriptCursor();
    void setActiveScriptCursor(int cursor);
    
    void setActiveScriptLimit(int limit);
    
    void callBeginMessage(PrimitiveReader reader);
    int decode(PrimitiveReader reader);
    
    int getActiveToken();
    int getActiveFieldId();
    String getActiveFieldName();
    
}
