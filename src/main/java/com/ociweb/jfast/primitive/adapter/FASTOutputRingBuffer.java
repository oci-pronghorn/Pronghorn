package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.ring.FASTRingBuffer;

public class FASTOutputRingBuffer implements FASTOutput {

	private final FASTRingBuffer ringBuffer;
	private DataTransfer dataTransfer;
		
	public FASTOutputRingBuffer(FASTRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
	}
	
	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {		
		int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		while (size>0) {			
			FASTRingBuffer.addByteArray(dataTransfer.writer.buffer, PrimitiveWriter.nextOffset(dataTransfer.writer), size, ringBuffer);
			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);		
		}
		FASTRingBuffer.publishWrites(ringBuffer);		
	}
}