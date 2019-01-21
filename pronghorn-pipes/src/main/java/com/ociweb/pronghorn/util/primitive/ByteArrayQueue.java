package com.ociweb.pronghorn.util.primitive;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class ByteArrayQueue {
	
	private final int size;
	private final int dataMask;
	private final byte[] data;
	
	private int head;
	private int markedHead;
	private int markedTail;	
	
	private final int[] tail = new int[1];
	
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
		
	}
	
	public boolean tryEnqueue(byte[] backing, int mask, int position, int length) {
		
		int room;
		if (tail[0]>head) {
			room = tail[0]-head;
		} else {
			room = (size-head)+tail[0];
		}
		if (room<length) {
			return false;
		}
		
		
		int r = 0;
		while (r<lastInputLength && r<length && lastInputValue[r]==backing[mask&(position+r)]) {
			r++;
		}
		
		//write length of match from pevious data
		head = mask  & DataOutputBlobWriter.writePackedLong(r, data, mask, head);		
		
		//write length of remaining bytes
		int remaining = length-r;
		head = mask  & DataOutputBlobWriter.writePackedLong(remaining, data, mask, head);	
		
		//write the remaining bytes
		int base = position+r;
		for(int i=0; i<remaining; i++) {			
			data[dataMask&(head++)] = backing[mask&(i+base)];			
		}
		
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
		return tail[0]!=head;
	}
	
	public int dequeue(byte[] target, int mask, int position) {
		
		assert (tail[0]!=head);		
		
		int startPos = position;
		//read lead length
		final int copyLength = (int)DataInputBlobReader.readPackedLong(data, dataMask, tail);
		
		Pipe.copyBytesFromToRing(lastOutputValue, 0, Integer.MAX_VALUE, 
				                 target, position, mask, 
				                 copyLength);	
		position += copyLength;
		
		//read len of chars
		final int bytesLength = (int)DataInputBlobReader.readPackedLong(data, dataMask, tail);
		for(int i=0; i<bytesLength; i++) {
			target[mask&(position++)] = data[tail[0]++];
		}
				
		int outputLength = copyLength + bytesLength;
		
		////////////////////////////////////////////////////////
		if (lastOutputValue.length<outputLength) {
			lastOutputValue = new byte[outputLength*2];
		}
		Pipe.copyBytesFromToRing(target, startPos, mask,
								lastOutputValue, 0, Integer.MAX_VALUE,
								outputLength);
				                 
		return lastOutputLength = outputLength;
		
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
		if (room<maxLength) {
			return false;
		}
		return true;
	}
}
