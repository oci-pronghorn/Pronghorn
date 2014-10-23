package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTRingBuffer;

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
			
			//TODO: A, need to decide which way. What if we send the bytes as a single block?  
			//  * it would be more natural for the way the ring buffer works.
			//  * it would enable the downstream systems to chunk more effectively 
			//  * bytes stay bytes and need not be merged.
			
			//ostr.write(dataTransfer.writer.buffer,   PrimitiveWriter.nextOffset(dataTransfer.writer), size);
			
			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);		
		}
		FASTRingBuffer.publishWrites(ringBuffer);
		
	}


}
