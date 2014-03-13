package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderChar;
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
	
	final int maxCharSize;
	final int charMask;
	final char[] charBuffer;
	
	public FASTRingBuffer(byte bits, byte charBits) {
		assert(bits>=1);
		this.maxSize = 1<<bits;
		this.mask = maxSize-1;
		this.buffer = new int[maxSize];
			
		this.maxCharSize = 1<<charBits;
		this.charMask = maxCharSize-1;
		this.charBuffer = new char[maxCharSize];
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
		//3 different modes but this always consumes a single int in ring buffer.
		//Dynamic RingBuffer -     00 length (reader will index each text to jump w/o array?)
		//Constant TextHeap -      10 full index
		//Up to 3 ascii chars here 110000nn up to 3 ascii chars 
		
		int pos = addCount.get();
		
		int temp = maxSize-1;
		while (pos-removeCount.get()>=temp) {	
		}
		
		if (idx<0) {//points to constant, high bit already set.
			buffer[mask&pos] = idx;			
		} else {
			if ((buffer[mask&pos] = heap.triASCIIToken(idx))>0) {
				
				//TODO: Must copy full string to secondary RingBuffer.
				System.err.println("unsupported");
				
			}
			
		}
		
		addCount.lazySet(1+pos);	
				
	}

	public void append(int idx, ByteHeap heap) {
		
		throw new UnsupportedOperationException();//TODO: copy text soution
		
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
