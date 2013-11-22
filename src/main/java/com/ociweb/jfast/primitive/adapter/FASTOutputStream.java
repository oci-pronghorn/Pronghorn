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

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {
		try {
			int size = dataTransfer.nextBlockSize();
			while (size>0) {
				ostr.write(dataTransfer.rawBuffer(), 
				     	   dataTransfer.nextOffset(), size);
				size = dataTransfer.nextBlockSize();		
			}
			ostr.flush();
		} catch (IOException e) {
			throw new FASTException(e);
		} 
	}

}
