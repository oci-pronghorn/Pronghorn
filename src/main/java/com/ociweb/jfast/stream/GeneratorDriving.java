package com.ociweb.jfast.stream;


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
