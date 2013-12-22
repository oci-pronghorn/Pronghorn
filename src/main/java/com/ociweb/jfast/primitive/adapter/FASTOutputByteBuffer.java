package com.ociweb.jfast.primitive.adapter;

import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;

public class FASTOutputByteBuffer implements FASTOutput {

	private final ByteBuffer byteBuffer;
	private DataTransfer dataTransfer;
	
	public FASTOutputByteBuffer(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	@Override
	public void flush() {

		int size = dataTransfer.nextBlockSize();

		int total = 0;
	//	int c = 0;
	//	StringBuilder temp = new StringBuilder();
		while (size>0) {
			//temp = temp.append(size).append(",");
		//	c++;

			byteBuffer.put(dataTransfer.rawBuffer(), 
			     	       dataTransfer.nextOffset(), size);

			size = dataTransfer.nextBlockSize();
			total+=size;
		}
	//	System.out.println("flush out :"+total+" in "+c+" parts "+temp);
		
	}

	public void reset() {
		byteBuffer.clear();
		
	}

}
