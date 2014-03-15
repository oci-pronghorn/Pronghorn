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
	
	int addPos = 0;
	int remPos = 0;
	
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
	
    public void blockOnNeed(int step) { //TODO: call upon start of group when we know the size?
    	
    	int temp = maxSize-step;
    	while (addPos-removeCount.get()>=temp) {	
    	}
    	
    }
	
	
	public void append(int value) {
						
		buffer[mask&addPos] = value;
		addPos++;
		
	}
	
	public void append(long value) {
		
		buffer[mask&addPos] = (int)(value>>>32);
		buffer[mask&(addPos+1)] = (int)(value&0xFFFFFFFF);
		
		addPos+=2;		
		
	}

	public void append(int idx, TextHeap heap) {
		//3 different modes but this always consumes a single int in ring buffer.
		//Dynamic RingBuffer -     00 length (reader will index each text to jump w/o array?)
		//Constant TextHeap -      10 full index
		//Up to 3 ascii chars here 110000nn up to 3 ascii chars 
		
		if (idx<0) {//points to constant, high bit already set.
			buffer[mask&addPos] = idx;			
		} else {
			if ((buffer[mask&addPos] = heap.triASCIIToken(idx))>0) {
				//TODO: Must copy full string to secondary RingBuffer.
				System.err.println("unsupported");
			}
		}
		
		addPos++;	
				
	}

	public void append(int idx, ByteHeap heap) {
		
		throw new UnsupportedOperationException();//TODO: copy text soution
		
	}

	public void append(int readDecimalExponent, long readDecimalMantissa) {
		
		buffer[mask&addPos] = readDecimalExponent;
		buffer[mask&(addPos+1)] = (int)(readDecimalMantissa>>>32);
		buffer[mask&(addPos+2)] = (int)(readDecimalMantissa&0xFFFFFFFF);
		
		addPos+=3;
		
	}
	
	//only called once the end of a group is reached and we want to allow the consumer to have access to the fields.
	public void moveForward() {
		addCount.lazySet(addPos); 
	}
	
	public void removeForward(int step) {
		//remPos = removeCount.get()+step;
		//removeCount.lazySet(remPos);
	}
	
	public void dump() {
		//move the removePosition up to the addPosition
		//System.err.println("resetup to "+addPos);
		remPos = addPos;
		removeCount.lazySet(addPos);
	}

	public void printPos(String label) {
		System.err.println(label+" remPos:"+remPos+"  addPos:"+addPos);
	}
	
	public int readInteger(int idx) {
		return buffer[mask&(remPos+idx)];
	}
	
	public long readLong(int idx) {
		
		int i = remPos+idx;
		return (((long)buffer[mask&i])<<32) | (((long)buffer[mask&(i+1)])&0xFFFFFFFFl);

	}
	
}
