package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.io.IOException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;

public class AppendableUTF8Ring implements Appendable {

	private final RingBuffer ringBuffer;
	private final char[] temp = new char[1];
	private long outputTarget;
	private long tailPosCache;
	
	private final static int step = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	public AppendableUTF8Ring(RingBuffer ringBuffer) {

		this.ringBuffer = ringBuffer;
		if (RingBuffer.from(ringBuffer) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		int messagesPerRing = (1<<(ringBuffer.pBits-1));
		outputTarget = step-messagesPerRing;//this value is negative		
		tailPosCache = tailPosition(ringBuffer);
		
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        RingBuffer.addMsgIdx(ringBuffer, 0);
		RingBuffer.validateVarLength(ringBuffer, csq.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, RingBuffer.bytesWriteBase(ringBuffer), ringBuffer.byteWorkingHeadPos.value, RingBuffer.copyUTF8ToByte(csq, 0, csq.length(), ringBuffer));

		RingBuffer.publishWrites(ringBuffer);

		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        RingBuffer.addMsgIdx(ringBuffer, 0);
		RingBuffer.validateVarLength(ringBuffer, csq.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos,  RingBuffer.bytesWriteBase(ringBuffer), ringBuffer.byteWorkingHeadPos.value,  RingBuffer.copyUTF8ToByte(csq, start, end-start, ringBuffer));
		
		RingBuffer.publishWrites(ringBuffer);

		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
	    RingBuffer.addMsgIdx(ringBuffer, 0);
		RingBuffer.validateVarLength(ringBuffer, temp.length<<3);
		 //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, RingBuffer.bytesWriteBase(ringBuffer), ringBuffer.byteWorkingHeadPos.value, RingBuffer.copyUTF8ToByte(temp, 0, temp.length, ringBuffer));

		RingBuffer.publishWrites(ringBuffer);

		return this;
	}
	
	public void flush() {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=2;
		RingStreams.writeEOF(ringBuffer);
	}

}
