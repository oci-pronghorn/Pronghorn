package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import com.ociweb.pronghorn.ring.RingBuffer;

public class OutputRingInvocationHandler implements InvocationHandler {

	RingBuffer outputRing;
	
	public OutputRingInvocationHandler(RingBuffer outputRing) {
		this.outputRing = outputRing;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		
		Class returnClazz = method.getReturnType();
		
		String methodName = method.getName();
		
		//  void writeThing(int value)
		// find Thing field in Message ?? should be of type int.
		// writeBegin
		// publish()
		
		return null;
	}

}
