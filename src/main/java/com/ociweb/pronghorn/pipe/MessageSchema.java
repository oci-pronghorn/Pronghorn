package com.ociweb.pronghorn.pipe;

public abstract class MessageSchema {

    protected final FieldReferenceOffsetManager from;
    
    protected MessageSchema(FieldReferenceOffsetManager from) {
        this.from = from;
    }
    
    public static final FieldReferenceOffsetManager from(MessageSchema schema) {
        return schema.from;
    }
    
}
