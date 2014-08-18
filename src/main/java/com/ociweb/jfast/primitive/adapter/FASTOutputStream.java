//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTOutputStream implements FASTOutput{

	private final OutputStream ostr;
	private DataTransfer dataTransfer;
	
	public FASTOutputStream(OutputStream ostr) {
		this.ostr = ostr;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {
		try {
			int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
			while (size>0) {
				ostr.write(dataTransfer.writer.buffer, 
				     	   PrimitiveWriter.nextOffset(dataTransfer.writer), size);
				size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);		
			}
			ostr.flush();
		} catch (IOException e) {
			throw new FASTException(e);
		} 
	}

}
