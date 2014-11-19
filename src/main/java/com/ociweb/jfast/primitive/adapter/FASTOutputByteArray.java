//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.util.Arrays;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;


public final class FASTOutputByteArray implements FASTOutput {

	public final byte[] buffer;
	public int position;
	private DataTransfer dataTransfer;
	private PrimitiveWriter writer;
		
	
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
		this.writer = dataTransfer.writer;
	}

	@Override
	public void flush() {
		
		int size = PrimitiveWriter.nextBlockSize(writer);
		while (size>0) {
		    int srcOffset = PrimitiveWriter.nextOffset(writer);
		    if (position+size>buffer.length) {
		        throw new ArrayIndexOutOfBoundsException(position+size);		        
		    }
		    System.arraycopy(writer.buffer, 
			         		 srcOffset, 			         		 
			         		 buffer,
			         		 position, 			         		 
			         		 size);
			
			position+=size;
			size = PrimitiveWriter.nextBlockSize(writer);
		}
	}


}
