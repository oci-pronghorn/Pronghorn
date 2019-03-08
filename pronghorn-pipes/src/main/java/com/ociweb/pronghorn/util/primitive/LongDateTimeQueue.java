package com.ociweb.pronghorn.util.primitive;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

/**
 * Custom datastructure for holding a sequence of timestamps stored as longs.
 * We assume the longs are in order and "near" each other.  This is used for
 * producing deltas which allow for smaller storage.
 * 
 * This works as a smaller custom version of a Pipe and is needed in cases
 * like server connections where we would like to have many K objects.
 * 
 * @author Nathan Tippy
 *
 */
public class LongDateTimeQueue {
	
	private final int size;
	private final int mask;
	private final byte[] data;
	private int markedHead;
	private int markedTail;
	
	private AtomicInteger head = new AtomicInteger();
	private AtomicInteger tail = new AtomicInteger();
	
	private final int[] temp = new int[1];
	
	private long lastInputValue;
	private long lastOutputValue;
	
	public LongDateTimeQueue(int bits) {

		size = 1<<bits;
		mask = size-1;
		data = new byte[size];
		
	}
	
	//TODO: add a run lenght encoding for values of zero dif since this happens very freqntly.
	//      this will allow for 2-4x more connections...
	
	AtomicInteger c = new AtomicInteger();
	
	//returns new head
	public int tryEnqueue(long time) {
		
		int h = head.get();
		int t = tail.get();
		int room;
		if (t>h) {
			room = t-h;
		} else {
			room = (size-h)+t;
		}
		if (room<11) {
			return -1;
		}
		
		long dif = time-lastInputValue;
		//int oldHead = head;
		int newHead = mask  & DataOutputBlobWriter.writePackedLong(dif, data, mask, head.get());		
		
		//temp[head-oldHead]++;
		//System.out.println("lengthwritten: "+Arrays.toString(temp)); //normally 1
		//                lengthwritten: [0, 64611, 21, 260, 18, 5, 0, 1, 0, 0, 0, 0] //mostly 1
		c.incrementAndGet();
		lastInputValue = time;
		return newHead;
	}
	
	public void publishHead(int headPos) {
		head.set(headPos);
	}
	
	public boolean hasData() {
		return tail.get()!=head.get();
	}

	public long dequeue() {
		assert(tail.get()!=head.get());
			
		c.decrementAndGet();
	
		temp[0]=tail.get();
		long value = lastOutputValue+DataInputBlobReader.readPackedLong(data, mask, temp);
		tail.set(temp[0]&mask);
		lastOutputValue = value;
		return value;
		
	}

	//store in case we need to roll back this write
	public void markHead() {
		markedHead = head.get();
	}
	
	//roll back the write to the last mark
	public void resetHead() {
		head.set(markedHead); //TODO: head goes backards read is bad
	}
	

	public boolean hasRoom() {
		int h = head.get();
		int t = tail.get();
		int room;
		if (t>h) {
			room = t-h;
		} else {
			room = (size-h)+t;
		}
		if (room<11) {//TOOD: key bug...
			System.out.println("no room but holding "+c+" size "+size+" tail "+t+" head "+h);
			return false;
		}
		return true;
	}
	
}
