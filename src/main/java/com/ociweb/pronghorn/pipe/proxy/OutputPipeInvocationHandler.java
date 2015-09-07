package com.ociweb.pronghorn.pipe.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;

public class OutputPipeInvocationHandler extends PipeInvokeHandler implements InvocationHandler {
	//TODO: NOTE: this approach does NOT support nested structures at all.

	private static final Logger log = LoggerFactory.getLogger(OutputPipeInvocationHandler.class);
	
	//This only supports one template message
	private final OutputPipeWriterMethod[] writers;
	
	
	public OutputPipeInvocationHandler(Pipe outputRing, int msgIdx, Class<?> clazz) {
		super(clazz.getMethods());
		
		FieldReferenceOffsetManager from = Pipe.from(outputRing);
		final Method[] methods = clazz.getMethods();
					

		writers = new OutputPipeWriterMethod[MAX_METHODS];
		int j = methods.length;
		while (--j>=0) {
			final Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				
				int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldAnnonation.fieldId(), msgIdx, from);		
				
				int key = buildKey(this, method.getName());
				if (null!=writers[key]) {
					throw new UnsupportedOperationException();
				}
				writers[key] = OutputPipeWriterMethod.buildWriteForYourType(outputRing, fieldAnnonation.decimalPlaces(), fieldLoc, (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE, from);
								
			}
		}
	}

		
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {							
		writers[buildKey(this,method.getName())].write(args);
		return null;
	}

}
