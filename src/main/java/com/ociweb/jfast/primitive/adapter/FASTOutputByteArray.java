package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTOutput;


public class FASTOutputByteArray implements FASTOutput {

	private final byte[] buffer;
	private int position;
	
	
	public FASTOutputByteArray(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public void reset() {
		position = 0;
	}
	
	public final int flush(byte[] source, int offset, int length, int need) {
		//if need is >= length then this call must block until length is written
		//else this call can return early after need is written			
		int requiredFlush = need>=length? length : need;
		
		int remaining = buffer.length - position;
		if (requiredFlush>remaining) {
			
			//TODO: we have a problem the buffer must be swapped and grown
			
		}		
		
		if (length > remaining) {
			length = buffer.length - position;
		}	
						
		System.arraycopy(source,offset,buffer,position,length);
		position+=length;
		return length;
	}

	public int position() {
		return position;
	}

}
