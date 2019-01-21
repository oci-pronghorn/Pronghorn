package com.ociweb.pronghorn.util.primitive;

import java.util.Arrays;

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
	private int head;
	private int markedHead;
	private int markedTail;
	
	private final int[] tail = new int[1];
	private long lastInputValue;
	private long lastOutputValue;
	
	public LongDateTimeQueue(int bits) {

		size = 1<<bits;
		mask = size-1;
		data = new byte[size];
		
	}
	
	//int[] temp = new int[12];
	
	public boolean tryEnqueue(long time) {
		
		int room;
		if (tail[0]>head) {
			room = tail[0]-head;
		} else {
			room = (size-head)+tail[0];
		}
		if (room<11) {
			return false;
		}
		
		long dif = time-lastInputValue;
		//int oldHead = head;
		head = mask  & DataOutputBlobWriter.writePackedLong(dif, data, mask, head);		
		
		//temp[head-oldHead]++;
		//System.out.println("lengthwritten: "+Arrays.toString(temp)); //normally 1
		//                lengthwritten: [0, 64611, 21, 260, 18, 5, 0, 1, 0, 0, 0, 0] //mostly 1
		
		lastInputValue = time;
		return true;
	}
	
	public boolean hasData() {
		return tail[0]!=head;
	}
	
	public long dequeue() {
		
		assert (tail[0]!=head);		
		long value = lastOutputValue+DataInputBlobReader.readPackedLong(data, mask, tail);
		tail[0] = tail[0]&mask;
		lastOutputValue = value;
		return value;
		
	}

	//store in case we need to roll back this write
	public void markHead() {
		markedHead = head;
	}
	
	//roll back the write to the last mark
	public void resetHead() {
		head = markedHead;
	}
	
	//store in case we need to roll back this read
	public void markTail() {
		markedTail = tail[0];
	}
	
	//roll back the read to the last mark
	public void resetTail() {
		tail[0] = markedTail;
	}
	
	

	public boolean hasRoom() {
		int room;
		if (tail[0]>head) {
			room = tail[0]-head;
		} else {
			room = (size-head)+tail[0];
		}
		if (room<11) {
			return false;
		}
		return true;
	}
	
}
