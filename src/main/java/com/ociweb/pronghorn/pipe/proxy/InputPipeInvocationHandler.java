package com.ociweb.pronghorn.pipe.proxy;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class InputPipeInvocationHandler extends PipeInvokeHandler implements InvocationHandler {
	//TODO: NOTE: this approach does NOT support nested structures at all.

	private static final Logger log = LoggerFactory.getLogger(InputPipeInvocationHandler.class);
	
	//This only supports one template message
	private final InputPipeReaderMethod[] readers;
	
	
	public InputPipeInvocationHandler(Pipe pipe, int msgIdx, Class<?> clazz) {
		super(clazz.getMethods());
		
		FieldReferenceOffsetManager from = Pipe.from(pipe);
		final Method[] methods = clazz.getMethods();
							

		readers = new InputPipeReaderMethod[MAX_METHODS];
		int j = methods.length;
		while (--j>=0) {
			final Method method = methods[j];			
			ProngTemplateField fieldAnnotation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnotation) {
				
				int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldAnnotation.fieldId(), msgIdx, from);
				
				int key = buildKey(this, method.getName());
				if (null!=readers[key]) {
					throw new UnsupportedOperationException();
				}
				readers[key] = InputPipeReaderMethod.buildReadForYourType(pipe, fieldLoc, (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE, from);
								
			}
		}
	}

		
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {							
		return readers[buildKey(this,method.getName())].read(args);
	}

}
