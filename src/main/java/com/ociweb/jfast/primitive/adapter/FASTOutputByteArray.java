package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;


public final class FASTOutputByteArray implements FASTOutput {

	public final byte[] buffer;
	public int position;
	private DataTransfer dataTransfer;
		
	
	public FASTOutputByteArray(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public void reset() {
		position = 0;
	}
	
	public final int flush(byte[] source, int offset, int length) {
		int remaining = buffer.length - position;
		
		if (length > remaining) {
			length = buffer.length - position;
		}	

		if (length==1) {
			buffer[position++] = source[offset];
			return 1;
		}
		
		
//		int i = length;
//		int j = offset;
//		while (--i>=0) {
//			buffer[position++] = source[j++];
//		}
		
		System.arraycopy(source,offset,buffer,position,length);
		position+=length;
		return length;
	}

	public int position() {
		return position;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}

	@Override
	public void flush() {
		
		int size = dataTransfer.nextBlockSize();
		while (size>0) {
			
			System.arraycopy(dataTransfer.rawBuffer(), 
			         		 dataTransfer.nextOffset(), 
			         		 buffer, position, size);
			
			position+=size;
			size = dataTransfer.nextBlockSize();
		}
	}


}
