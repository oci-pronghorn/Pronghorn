package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.Proxy;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;

public class EventProducer {

	private final RingBuffer input;	
	private Object cached;
	private int    cachedMsgId;
	
	public EventProducer(RingBuffer input) {
		this.input = input;
	}

	@SuppressWarnings("unchecked")
	public static <T> T take(final EventProducer consumer, Class<T> clazz) {
		
		//re-use old proxy if possible
		if (null!=consumer.cached && clazz.isAssignableFrom(consumer.cached.getClass())) {
			if (RingReader.tryReadFragment(consumer.input)) {
				assert(consumer.cachedMsgId == RingReader.getMsgIdx(consumer.input));
				return (T)consumer.cached;			
			} else {
				return null;
			}
		}
		return slowCreate(consumer, clazz);		
	}

	private static <T> T slowCreate(final EventProducer consumer, Class<T> clazz) {
		int msgIdx = FieldReferenceOffsetManager.lookupTemplateLocator(clazz.getAnnotation(ProngTemplateMessage.class).templateId(), RingBuffer.from(consumer.input));

		if (RingReader.tryReadFragment(consumer.input)) {
			
			T result = (T) Proxy.newProxyInstance(
								clazz.getClassLoader(),
								new Class[] { clazz },
								new InputRingInvocationHandler(consumer.input, RingReader.getMsgIdx(consumer.input), clazz));	
			
			consumer.cached = result; //TODO: needs smarter pool but this is fine for now.
			consumer.cachedMsgId = RingReader.getMsgIdx(consumer.input);
			
			return result;
			
		} else {
			return null;
		}
	}

	public static void dispose(EventProducer consumer, Object dq) {
	    assert(null==consumer.cached || dq==consumer.cached);
		RingReader.releaseReadLock(consumer.input);
	}

}
