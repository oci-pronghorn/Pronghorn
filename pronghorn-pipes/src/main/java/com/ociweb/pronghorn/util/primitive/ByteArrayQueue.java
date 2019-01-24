package com.ociweb.pronghorn.util.primitive;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class ByteArrayQueue {
	
	private final int size;
	private final int dataMask;
	private final byte[] data;
	
	private AtomicInteger head;
	private AtomicInteger tail;
	
	private final int[] temp = new int[1];
	
	private byte[] lastInputValue = new byte[256];
	private int    lastInputLength = 0;
	
	private byte[] lastOutputValue = new byte[256];
	private int    lastOutputLength = 0;
	
	private final int maxLength;
	
	public ByteArrayQueue(int bits, int maxLength) {

		this.size = 1<<bits;
		this.dataMask = size-1;
		this.data = new byte[size];
		this.maxLength = maxLength;
		
		this.head = new AtomicInteger();
		this.tail = new AtomicInteger();
	}
	
	public boolean tryEnqueue(byte[] backing, int mask, int position, int length) {
		
		int t = tail.get();
		int h = head.get();
		
		int room;
		if (t>h) {
			room = t-h;
		} else {
			room = (size-h)+t;
		}
		if (room<length) {
			return false;
		}
		
		
		int r = 0;
		while (r<lastInputLength && r<length && lastInputValue[r]==backing[mask&(position+r)]) {
			r++;
		}
		
		//write length of match from pevious data
		h = mask  & DataOutputBlobWriter.writePackedLong(r, data, mask, h);		
		
		//write length of remaining bytes
		int remaining = length-r;
		h = mask  & DataOutputBlobWriter.writePackedLong(remaining, data, mask, h);	
		
		//write the remaining bytes
		int base = position+r;
		for(int i=0; i<remaining; i++) {			
			data[dataMask&(h++)] = backing[mask&(i+base)];			
		}
		head.set(h);
		
		////////////////////////////////////////////////////////
		if (lastInputValue.length<length) {
			lastInputValue = new byte[length*2];
		}
		Pipe.copyBytesFromArrayToRing(backing, position, 
				                      lastInputValue, 0, 
				                      mask, length);
		lastInputLength = length;
		return true;
	}
	
	public boolean hasData() {
		return tail.get()!=head.get();
	}
	
	public int dequeue(byte[] target, int mask, int position) {
		
		
		assert (tail.get()!=head.get());		
		
		int startPos = position;
		//read lead length
		temp[0] = tail.get();
		final int copyLength = (int)DataInputBlobReader.readPackedLong(data, dataMask, temp);
		
		Pipe.copyBytesFromToRing(lastOutputValue, 0, Integer.MAX_VALUE, 
				                 target, position, mask, 
				                 copyLength);	
		position += copyLength;
		
		//read len of chars
		final int bytesLength = (int)DataInputBlobReader.readPackedLong(data, dataMask, temp);
		for(int i=0; i<bytesLength; i++) {
			target[mask&(position++)] = data[temp[0]++];
		}
				
		int outputLength = copyLength + bytesLength;
		
		tail.set(temp[0]);
		////////////////////////////////////////////////////////
		if (lastOutputValue.length<outputLength) {
			lastOutputValue = new byte[outputLength*2];
		}
		Pipe.copyBytesFromToRing(target, startPos, mask,
								lastOutputValue, 0, Integer.MAX_VALUE,
								outputLength);
				                 
		return lastOutputLength = outputLength;
		
	}

	public boolean hasRoom() {
		int t = tail.get();
		int h = head.get();
		
		int room;
		if (t>h) {
			room = t-h;
		} else {
			room = (size-h)+t;
		}
		if (room<maxLength) {
			return false;
		}
		return true;
	}
}
