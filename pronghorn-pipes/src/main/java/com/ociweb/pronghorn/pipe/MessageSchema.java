package com.ociweb.pronghorn.pipe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public abstract class MessageSchema<T extends MessageSchema<T>> {

    protected final FieldReferenceOffsetManager from; //will be null for schema-less

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
    
    public PipeConfig<T> newPipeConfig(int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
    	return new PipeConfig<T>((T)this, minimumFragmentsOnRing, maximumLenghOfVariableLengthFields);
    };
    
    public PipeConfig<T> newPipeConfig(int minimumFragmentsOnRing) {
    	return new PipeConfig<T>((T)this, minimumFragmentsOnRing, 0);
    };
    
    public Pipe<T> newPipe(int minimumFragmentsOnRing, int maximumLenghOfVariableLengthFields) {
    	return new Pipe<T>((PipeConfig<T>) newPipeConfig(minimumFragmentsOnRing, maximumLenghOfVariableLengthFields));
    }
    
    public Pipe<T> newPipe(int minimumFragmentsOnRing) {
    	return new Pipe<T>((PipeConfig<T>) newPipeConfig(minimumFragmentsOnRing, 0));
    }

    //TODO: write a second one for assert only which confirms only 1 static instance field.
	public static <S extends MessageSchema<S>> S findInstance(Class<S> clazz) {
		
		Field[] declaredFields = clazz.getDeclaredFields();
		for(int i=0;i<declaredFields.length;i++) {
			Field f = declaredFields[i];
			//only look at static fields		
			if (Modifier.isStatic(f.getModifiers())) {				
				if (f.getGenericType().getTypeName().equals(clazz.getName())) {					
					try {
						return (S)f.get(null);
					} catch (IllegalArgumentException e) {
						throw new RuntimeException(e);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}    			
				}
			}
		}
		return null;
	};
    
}
