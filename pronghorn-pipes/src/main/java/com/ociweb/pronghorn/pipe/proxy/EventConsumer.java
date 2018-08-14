package com.ociweb.pronghorn.pipe.proxy;

import java.lang.reflect.Proxy;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class EventConsumer {

	private final Pipe output;	
	private Object cached;
	private int    cachedMsgId;
	
	public EventConsumer(Pipe output) {
		this.output = output;
	}

	@SuppressWarnings("unchecked")
	public static <T> T create(final EventConsumer consumer, Class<T> clazz) {
		
		//re-use old proxy if possible
		if (null!=consumer.cached && clazz.isAssignableFrom(consumer.cached.getClass())) {
			if (PipeWriter.tryWriteFragment(consumer.output, consumer.cachedMsgId)) {
				return (T)consumer.cached;			
			} else {
				return null;
			}
		}
		return slowCreate(consumer, clazz);		
	}

	private static <T> T slowCreate(final EventConsumer consumer, Class<T> clazz) {
		int msgIdx = FieldReferenceOffsetManager.lookupTemplateLocator(clazz.getAnnotation(ProngTemplateMessage.class).templateId(), Pipe.from(consumer.output));

		if (PipeWriter.tryWriteFragment(consumer.output, msgIdx)) {
			
			T result = (T) Proxy.newProxyInstance(
								clazz.getClassLoader(),
								new Class[] { clazz },
								new OutputPipeInvocationHandler(consumer.output, msgIdx, clazz));	
			
			consumer.cached = result; //TODO: needs smarter pool but this is fine for now.
			consumer.cachedMsgId = msgIdx;
			
			return result;
			
		} else {
			return null;
		}
	}

	public static void publish(EventConsumer consumer, Object dq) {
	    assert(null==consumer.cached || dq==consumer.cached);
		PipeWriter.publishWrites(consumer.output);
	}

}
