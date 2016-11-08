package com.ociweb.pronghorn.pipe;

public abstract class MessageSchema {

    protected final FieldReferenceOffsetManager from;

    protected MessageSchema(FieldReferenceOffsetManager from) {
        this.from = from;
    }
    
    public static final FieldReferenceOffsetManager from(MessageSchema schema) {
        return schema.from;
    }
        
    public int getLocator(String messageName, String fieldName) {
    	return from.getLoc(messageName, fieldName);
    }

    public int getLocator(long messageId, long fieldId) {
    	return from.getLoc(messageId, fieldId);
    }
    
}
