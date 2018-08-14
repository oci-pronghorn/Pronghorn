package com.ociweb.pronghorn.pipe.proxy;

import java.lang.reflect.Proxy;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;

public class EventProducer {

	private final Pipe input;	
	private Object cached;
	private int    cachedMsgId;
	
	public EventProducer(Pipe input) {
		this.input = input;
	}

	@SuppressWarnings("unchecked")
	public static <T> T take(final EventProducer consumer, Class<T> clazz) {
		
		//re-use old proxy if possible
		if (null!=consumer.cached && clazz.isAssignableFrom(consumer.cached.getClass())) {
			if (PipeReader.tryReadFragment(consumer.input)) {
				assert(consumer.cachedMsgId == PipeReader.getMsgIdx(consumer.input));
				return (T)consumer.cached;			
			} else {
				return null;
			}
		}
		return slowCreate(consumer, clazz);		
	}

	private static <T> T slowCreate(final EventProducer consumer, Class<T> clazz) {
		int msgIdx = FieldReferenceOffsetManager.lookupTemplateLocator(clazz.getAnnotation(ProngTemplateMessage.class).templateId(), Pipe.from(consumer.input));

		if (PipeReader.tryReadFragment(consumer.input)) {
			
			T result = (T) Proxy.newProxyInstance(
								clazz.getClassLoader(),
								new Class[] { clazz },
								new InputPipeInvocationHandler(consumer.input, PipeReader.getMsgIdx(consumer.input), clazz));	
			
			consumer.cached = result; //TODO: needs smarter pool but this is fine for now.
			consumer.cachedMsgId = PipeReader.getMsgIdx(consumer.input);
			
			return result;
			
		} else {
			return null;
		}
	}

	public static void dispose(EventProducer consumer, Object dq) {
	    assert(null==consumer.cached || dq==consumer.cached);
		PipeReader.releaseReadLock(consumer.input);
	}

}
