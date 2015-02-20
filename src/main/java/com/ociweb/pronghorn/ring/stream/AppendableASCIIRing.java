package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.io.IOException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;

public class AppendableASCIIRing implements Appendable {

	RingBuffer ringBuffer;
	char[] temp = new char[1];
	private long outputTarget;
	private long tailPosCache;
	
	private int countDownInit = 0;
	private int countDown;
	private final static int step = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	public AppendableASCIIRing(RingBuffer ringBuffer) {
		
		this.ringBuffer = ringBuffer;
		if (RingBuffer.from(ringBuffer) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		int messagesPerRing = (1<<(ringBuffer.pBits-1));
		outputTarget = step-messagesPerRing;//this value is negative		
		tailPosCache = tailPosition(ringBuffer);
		
		countDownInit = messagesPerRing>>2;
		countDown = countDownInit;
		
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        RingBuffer.addMsgIdx(ringBuffer, 0);
		RingBuffer.validateVarLength(ringBuffer, csq.length());
		int sourceLen = csq.length();
		final int p = RingBuffer.addASCIIToBytes(csq, 0, sourceLen, ringBuffer); 
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos,  RingBuffer.bytesWriteBase(ringBuffer), p, sourceLen);
		
		if ((--countDown)<=0) {
			RingBuffer.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        RingBuffer.addMsgIdx(ringBuffer, 0);
		int length = end-start;
		RingBuffer.validateVarLength(ringBuffer, csq.length());
		final int p = RingBuffer.addASCIIToBytes(csq, start, length, ringBuffer); 
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, RingBuffer.bytesWriteBase(ringBuffer), p, length);
		
		if ((--countDown)<=0) {
			RingBuffer.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
	    RingBuffer.addMsgIdx(ringBuffer, 0);
		RingBuffer.validateVarLength(ringBuffer,temp.length);
		int sourceLen = temp.length;
		final int p = RingBuffer.addASCIIToBytes(temp, 0, sourceLen,	ringBuffer); 
		RingBuffer.addBytePosAndLen(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, RingBuffer.bytesWriteBase(ringBuffer), p, sourceLen);
		
		if ((--countDown)<=0) {
			RingBuffer.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	public void flush() {
		RingWriter.blockWriteFragment(ringBuffer,0);
		RingStreams.writeEOF(ringBuffer);
	}
}
