package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.read.FASTException;

public class FASTOutputStream implements FASTOutput{

	private final OutputStream ostr;
	private DataTransfer dataTransfer;
	
	public FASTOutputStream(OutputStream ostr) {
		this.ostr = ostr;
	}
	
	public int flush(byte[] buffer, int offset, int length) {
		try {
			//blocks until length is written so the logic is simple
			ostr.write(buffer, offset, length);
			ostr.flush();
			return length;
		} catch (IOException e) {
			throw new FASTException(e);
		}
	}
	
	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {
		
		try {
			int size = dataTransfer.nextBlockSize();
			while (size>0) {
			//	System.err.println("flush:"+size);
				
				ostr.write(dataTransfer.rawBuffer(), 
				     	   dataTransfer.nextOffset(), size);

				size = dataTransfer.nextBlockSize();
				
				ostr.flush();
			}
		} catch (IOException e) {
			throw new FASTException(e);
		} 
	}

}
