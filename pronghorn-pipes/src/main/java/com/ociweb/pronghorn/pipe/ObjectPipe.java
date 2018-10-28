package com.ociweb.pronghorn.pipe;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPipe<T> {

	private final int size;
	private final int mask;	
	private final T[] objects;
	private final AtomicInteger head = new AtomicInteger(); //TODO: this can be optimized by caching the head
	private final AtomicInteger tail = new AtomicInteger(); //TODO: this can be optimized by caching the tail
	private final AtomicInteger count = new AtomicInteger();

	private Thread headThread; //for assert to ensure only 2 threads are used, one read and one write
	private Thread tailThread; //for assert to ensure only 2 threads are used, one read and one write
	
	public ObjectPipe(int bits, Class<T> clazz, ObjectPipeObjectCreator<T> opoc) {
		this.size = 1<<bits;
		this.mask = size-1;
		
		this.objects = (T[]) Array.newInstance(clazz, size);
		
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
		assert(isHeadThread());
		if (count.get()<mask) {
			count.incrementAndGet();
			head.incrementAndGet();
			return true;
		} else {
			return false;
		}
	}

	public void moveHeadForward() {
		assert(isHeadThread());
		assert(count.get()<mask);
		count.incrementAndGet();
		head.incrementAndGet();		
	}
	
	public T headObject() {
		assert(isHeadThread());
		if (count.get()<mask) {
		    return objects[head.get()&mask];
		} else {
			return null;
		}
	}

	private boolean isHeadThread() {
		Thread t = Thread.currentThread();
		if (null == headThread) {
			headThread = t;
		}
		return t==headThread;
	}
	

	/**
	 * 
	 * @return true if this can move
	 */
	public boolean tryMoveTailForward() {
		assert(isTailThread());
		if (count.get()>0) {
			count.decrementAndGet();
			tail.incrementAndGet();
			return true;
		} else {
			return false;
		}		
	}
	
	public void moveTailForward() {
		assert(isTailThread());
		assert (count.get()>0);
		count.decrementAndGet();
		tail.incrementAndGet();		
	}
	
	public T tailObject() {
		assert(isTailThread());
		if (count.get()>0) {
			return objects[tail.get()&mask];
		} else {
			return null;
		}
	}

    private boolean isTailThread() {
		Thread t = Thread.currentThread();
		if (null == tailThread) {
			tailThread = t;
		}
		return t==tailThread;
	}


	////////////////////////////////
	
	public int count() {
		return count.get();
	}

	public boolean hasRoomFor(int count) {
		return (this.count.get()+count)<=mask;
	}
	
}
