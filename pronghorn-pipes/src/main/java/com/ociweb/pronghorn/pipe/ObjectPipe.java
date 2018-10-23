package com.ociweb.pronghorn.pipe;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPipe<T> {

	private int size;
	private int mask;	
	private final T[] objects;
	private final AtomicInteger head = new AtomicInteger(); //TODO: this can be optimized by caching the head
	private final AtomicInteger tail = new AtomicInteger(); //TODO: this can be optimized by caching the tail
	private final AtomicInteger count = new AtomicInteger();
	
	
	public ObjectPipe(int bits, Class<T> clazz, ObjectPipeObjectCreator<T> opoc) {
		size = 1<<bits;
		mask = size-1;
		
		objects = (T[]) Array.newInstance(clazz, size);
		
		int i = size;
		while (--i>=0) {
			objects[i] = opoc.newInstance();		
		}
	}
	

	/**
	 * 
	 * @return true if this can move
	 */
	public boolean tryMoveHeadForward() {
		if (count.get()<mask) {
			count.incrementAndGet();
			head.incrementAndGet();
			return true;
		} else {
			return false;
		}
	}

	public void moveHeadForward() {
			count.incrementAndGet();
			head.incrementAndGet();		
	}
	
	public int count() {
		return count.get();
	}

	public boolean hasRoomFor(int count) {
		return (this.count.get()+count)<=mask;
	}
	
	/**
	 * 
	 * @return true if this can move
	 */
	public boolean tryMoveTailForward() {
		if (count.get()>0) {
			count.decrementAndGet();
			tail.incrementAndGet();
			return true;
		} else {
			return false;
		}		
	}
	
	public T tailObject() {
		if (count.get()>0) {
			return objects[tail.get()&mask];
		} else {
			return null;
		}
	}
	
	public T headObject() {
		if (count.get()<mask) {
		    return objects[head.get()&mask];
		} else {
			return null;
		}
	}

	
}
