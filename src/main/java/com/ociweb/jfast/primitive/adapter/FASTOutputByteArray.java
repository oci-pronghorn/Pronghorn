//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
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
		//int iter = 0;
		while (size>0) {
		    //System.err.println("flush size:"+size+" "+(iter++));
			//System.err.println("position "+position);
			System.arraycopy(dataTransfer.rawBuffer(), 
			         		 dataTransfer.nextOffset(), 
			         		 buffer, position, size);
			
			position+=size;
			size = dataTransfer.nextBlockSize();
		}
	}


}
