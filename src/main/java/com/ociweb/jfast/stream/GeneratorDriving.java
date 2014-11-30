package com.ociweb.jfast.stream;


public interface GeneratorDriving {

    int getActiveScriptCursor();
    void setActiveScriptCursor(int cursor);
    
        
    void runBeginMessage();
    void runFromCursor();
    
    int getActiveToken();
    int getActiveFieldId();
    String getActiveFieldName();
    int scriptLength();
    
}
