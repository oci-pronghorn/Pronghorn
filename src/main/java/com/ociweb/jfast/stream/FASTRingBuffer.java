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
	

	//NOTE: FAST decoding is assumed to be running on 1 thread on 1 core which
	//is dedicated to this purpose. In order to minimize jitter and maximize 
	//low-latency a spin-lock is used when the ringBuffer does not have enough
	//room for new items. Saving CPU is less important here.
	//when lock spinning the monitor class must be notified so we know that the 
	//consumer is too slow or the ringBuffer is too small.
	
	//TODO: new smaller ring buffer for text that changes, then this ring can use fixed
	//sizes for the fields so an offset table can be used.  The caller must lookup the 
	//appropriate offset given the fieldID.  
	//TODO: the sequence return is wasting cycles at the top level and must be removed.
	
	
	public void append(int value) {
				
		int pos = addCount.get();
		
		int temp = maxSize-1;
		while (pos-removeCount.get()>=temp) {	
		}
		
		buffer[mask&pos] = value;
		addCount.lazySet(1+pos);		
		
	}
	
	public void append(long value) {
		
		int pos = addCount.get();
		
		int temp = maxSize-2;
		while (pos-removeCount.get()>=temp) {	
		}
		
		buffer[mask&pos] = (int)(value>>>32);
		buffer[mask&(pos+1)] = (int)(value&0xFFFFFFFF);
		
		addCount.lazySet(2+pos);		
		
	}

	public void append(int idx, TextHeap heap) {
		
		//TODO: bulk of time is here need to implement zero copy strings.
		
//		int pos = addCount.get();	
//		int length = heap.length(idx); //required to ensure we have the space.
//		//System.err.println("'append "+length+" "+heap.get(idx,new StringBuilder()));
//		int temp = maxSize-length;
//		if (temp<0) {
//			throw new UnsupportedOperationException();
//		}
//		while (pos-removeCount.get()>=temp) {	
//		}		
//		//System.err.println(Integer.toBinaryString(idx)+" "+heap.get(idx, new StringBuilder()));
//		
//		//TODO: according to the template all of these should have been constants.
//		//TODO: if it is a constant/default then point to that rather than make a copy.
//		
//		pos+=heap.getIntoRing(idx, buffer, pos, mask);
//		addCount.lazySet(pos);
		
	}

	public void append(int idx, ByteHeap heap) {
		
		int pos = addCount.get();	
		int length = heap.length(idx)>>2; //required to ensure we have the space.
		int temp = maxSize-length;
		if (temp<0) {
			throw new UnsupportedOperationException();
		}
		while (pos-removeCount.get()>=temp) {	
		}	
		
		pos+=heap.getIntoRing(idx, buffer, pos, mask);
		addCount.lazySet(pos);
		
	}

	public void append(int readDecimalExponent, long readDecimalMantissa) {
		
		int pos = addCount.get();
		
		int temp = maxSize-3;
		while (pos-removeCount.get()>=temp) {	
		}
		
		buffer[mask&pos] = readDecimalExponent;
		buffer[mask&(pos+1)] = (int)(readDecimalMantissa>>>32);
		buffer[mask&(pos+2)] = (int)(readDecimalMantissa&0xFFFFFFFF);
		
		addCount.lazySet(3+pos);
		
	}
	
	public void dump() {
		//TODO: must periodically remove maxSize from both remove and add
		
		//System.err.println("dumping:"+(addCount.get()-removeCount.get()));
		removeCount.set(addCount.get());
	}
	
	
}
