package com.ociweb.jfast.primitive.adapter;

import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.ring.RingBuffer;

public class FASTOutputRingBuffer implements FASTOutput {

	private final RingBuffer ringBuffer;
	private DataTransfer dataTransfer;
    private int fill;
	private long tailPosCache;
		
	public FASTOutputRingBuffer(RingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.fill =  1 + ringBuffer.mask - 2;
		this.tailPosCache = tailPosition(ringBuffer);
	}
	
	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {		
		int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		while (size>0) {		
			tailPosCache = RingBuffer.spinBlockOnTail(tailPosCache, headPosition(ringBuffer)-fill, ringBuffer);			
			RingBuffer.addMsgIdx(ringBuffer, 0);
			RingBuffer.addByteArray(dataTransfer.writer.buffer, PrimitiveWriter.nextOffset(dataTransfer.writer), size, ringBuffer);
			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);		
		}
		RingBuffer.publishWrites(ringBuffer);		
	}
}