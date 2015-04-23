package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class InputRingInvocationHandler extends RingHandler implements InvocationHandler {
	//TODO: NOTE: this approach does NOT support nested structures at all.

	private static final Logger log = LoggerFactory.getLogger(InputRingInvocationHandler.class);
	
	//This only supports one template message
	private final InputRingReaderMethod[] readers;
	
	
	public InputRingInvocationHandler(RingBuffer inputRing, int msgIdx, Class<?> clazz) {
		super(clazz.getMethods());
		
		FieldReferenceOffsetManager from = RingBuffer.from(inputRing);
		final Method[] methods = clazz.getMethods();
							

		readers = new InputRingReaderMethod[MAX_METHODS];
		int j = methods.length;
		while (--j>=0) {
			final Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				
				int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldAnnonation.fieldId(), msgIdx, from);		
				
				int key = buildKey(this, method.getName());
				if (null!=readers[key]) {
					throw new UnsupportedOperationException();
				}
				readers[key] = InputRingReaderMethod.buildReadForYourType(inputRing, fieldAnnonation.decimalPlaces(), fieldLoc, (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE, from);
								
			}
		}
	}

		
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {							
		return readers[buildKey(this,method.getName())].read(args);
	}

}
