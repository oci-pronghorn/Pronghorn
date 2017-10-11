package com.ociweb.pronghorn.pipe;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

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

	public static <S extends MessageSchema<S>> S findInstance(Class<S> clazz) {
		S found = null;
		for(Field f:clazz.getFields()) {    		
			
			Type type = f.getGenericType();    	
			
			if (type.getTypeName().equals(clazz.getName())) { 
				    			
				try {
					if (null!=found) {
						FROMValidation.logger.error("found multiple instance members for this schema");
						return null;
					}
					found = (S)f.get(null);
				} catch (IllegalArgumentException e) {
					throw new RuntimeException(e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}    			
			}
		}
	
		return found;
	};
    
}
