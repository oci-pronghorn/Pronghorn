package com.ociweb.jfast.read;

import com.ociweb.jfast.ByteConsumer;

public class PMapData implements ByteConsumer{

	final byte[] data;
	int limit;
	int bitIndex;
	int position;
	int lastValidIndex;
	
	//TODO: this is the new argument to all readers/writers?
	public PMapData(int size) {
		data = new byte[size]; //this never changes
		bitIndex = 7;
	}
	
	public final void offerBit(int bit) {
		
		--bitIndex;
		data[limit] |= 	(bit<<bitIndex);
		if (0 == bitIndex) {
			if (data[limit]!=0) {
				//must keep track of how far we are to write.
				lastValidIndex = limit;
			}
			limit++;
			bitIndex = 7;
			data[limit] = 0;
		}
		
	}
	
	//must ensure buffer has room for lastValid index
	//then system copy the bytes in
	//then modify the last byte high bit.
	
	//TODO: send, must send each byte writer should be responsible for stop bit.
	//      must keep track of last 1 bit and not send bytes of zeros.
	
	
	public final int takeBit() {
		if (position==limit) {
			//run out of bits so continue to return zeros
			return 0;
		}
		--bitIndex;
		//bitIndex = bitIndex&7; //part of test code to remove the conditional
		int result = (data[position]>>bitIndex)&1;
				
		//position+=((bitIndex-1)>>32);//part of test code to remove the conditional
		if (0 == bitIndex) {
			position++;
			bitIndex = 7;
		}
		return result;		
	}
	
	
	public void clear() {
		limit = 0;
		position = 0;
		bitIndex = 7;
		data[limit] = 0;
		lastValidIndex = 0;
	}
	
	public int size() {
		return data.length;
	}

	public void put(byte value) {
		data[limit++] = value;
	}
	
	public byte get(int index) {
		if (index<limit) {
			return data[index];
		} else {
			return 0;
		}
	}

	public final void reset(byte[] source, int offset, int total, int plusOne) {
		
		System.arraycopy(source, offset, data, 0, total-1);
		data[total-1]=(byte)plusOne;
		limit = total;
		
		position = 0;
		bitIndex = 7;
	}

	@Override
	public int length() {
		return limit;
	}
	
}
