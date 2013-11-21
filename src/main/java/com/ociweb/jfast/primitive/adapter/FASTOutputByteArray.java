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
