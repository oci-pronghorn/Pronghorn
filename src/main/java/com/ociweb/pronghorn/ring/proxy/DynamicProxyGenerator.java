package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import com.ociweb.pronghorn.ring.RingBuffer;

/**
 * 
 * This is the 6th attempt at a user friendly API for the ring buffer.
 * 
 * This technique will focus on using the built in dynamic proxy facilities commonly used by
 * ORM tools like Hibernate.  This API is expected to be less performant than all the 
 * previous techniques but it is also expected to be the most convenient.  Since we how to make
 * the code faster we can always use an automated script to convert to the other techniques 
 * later if the performance is needed.
 * 
 * @author Nathan Tippy
 *
 */
public class DynamicProxyGenerator<T> {

	public T newOutputInstance(Class<T> clazz, RingBuffer outputRing) {
		
		return (T) Proxy.newProxyInstance(
									clazz.getClassLoader(),
		                            new Class[] { clazz },
		                            new OutputRingInvocationHandler(outputRing));
		
	}
	
	//TODO: create newInputInstance
	//TODO: create newMixedInstance may come in a few flavors
	
	
}
