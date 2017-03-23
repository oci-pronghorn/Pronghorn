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
    
    public <T extends MessageSchema> PipeConfig<T> newPipeConfig(int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
    	return new PipeConfig<T>((T)this, minimumFragmentsOnRing, maximumLenghOfVariableLengthFields);
    };
    
    public <T extends MessageSchema> PipeConfig<T> newPipeConfig(int minimumFragmentsOnRing) {
    	return new PipeConfig<T>((T)this, minimumFragmentsOnRing, 0);
    };
    
    public <T extends MessageSchema> Pipe<T> newPipe(int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
    	return new Pipe<T>((PipeConfig<T>) newPipeConfig(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields));
    };
    
}
