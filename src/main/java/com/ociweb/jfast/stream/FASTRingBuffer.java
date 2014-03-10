package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;

/**
 * Specialized ring buffer for holding decoded values from a FAST stream.
 * Ring buffer has blocks written which correspond to whole messages or sequence items.
 * Within these blocks the consumer is provided random (eg. direct) access capabilities.
 * 
 * 
 * 
 * @author Nathan Tippy
 *
 */
public class FASTRingBuffer {

	final AtomicInteger removeCount = new AtomicInteger();
	final int[] buffer;
	final int mask;
	final AtomicInteger addCount = new AtomicInteger();
	final int maxSize;
	
	public FASTRingBuffer(byte bits) {
		assert(bits>=1);
		maxSize = 1<<bits;
		mask = maxSize-1;
		buffer = new int[maxSize];
	}
	
	//adjust these from the offset of the biginning of the message.
	
	public void release() {
		//step forward and allow write to previous location.
	}
	
	public void lockMessage() {
		//step forward to next message or sequence?
		//provide offset to the beginning of this message.
		
	}
	
	//fields by type are fixed length however char is not so then what?
	//we know the type by the order however we do NOT have random access?
	//or we could add direct jump value table in the front.
	//or we could skip the fields we do not want decoded.
	//or we could read the buffer sequentially.
	
	public int getInt(int idx) {
		return buffer[idx];
	}
	
	public long getLong(int idx) {
		return (((long)buffer[idx])<<32)|(0xFFFFFFFFl&buffer[idx+1]);
	}
	
	public boolean hasRoom(int need) {
		return false;//TODO: has room
	}

	//need blocking append ?
	
	public boolean append(int value) {
				
		int pos = addCount.get();
		if (pos-removeCount.get()==maxSize) {
			return false;
		}
		
		buffer[mask&pos] = value;
		addCount.lazySet(1+pos);		
		
		//TODO: what happens if there is no room to push on to queue?
		return true;
	}
	
	public void append(long value) {
		
		int pos = addCount.get();
		buffer[mask&pos] = (int)(value>>>32);
		buffer[mask&(pos+1)] = (int)(value&0xFFFFFFFF);
		
		addCount.lazySet(2+pos);		
		
	}

	public void append(int idx, TextHeap heap) {
		
		int pos = addCount.get();	
		int length = heap.length(idx); //required to ensure we have the space.
		
		pos+=heap.getIntoRing(idx, buffer, pos, mask);
		addCount.lazySet(pos);
		
	}

	public void append(int idx, ByteHeap heap) {
		
		int pos = addCount.get();	
		int length = heap.length(idx); //required to ensure we have the space.
		
		pos+=heap.getIntoRing(idx, buffer, pos, mask);
		addCount.lazySet(pos);
		
	}

	public void append(int readDecimalExponent, long readDecimalMantissa) {
		
		int pos = addCount.get();
		buffer[mask&pos] = readDecimalExponent;
		buffer[mask&(pos+1)] = (int)(readDecimalMantissa>>>32);
		buffer[mask&(pos+2)] = (int)(readDecimalMantissa&0xFFFFFFFF);
		
		addCount.lazySet(3+pos);
		
	}
	
	
}
