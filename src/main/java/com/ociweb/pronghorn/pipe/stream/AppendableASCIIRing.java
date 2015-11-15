package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnTail;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class AppendableASCIIRing implements Appendable {

	Pipe ringBuffer;
	char[] temp = new char[1];
	private long outputTarget;
	private long tailPosCache;
	
	private int countDownInit = 0;
	private int countDown;
	private final static int step = RawDataSchema.FROM.fragDataSize[0];
	
	public AppendableASCIIRing(Pipe ringBuffer) {
		
		this.ringBuffer = ringBuffer;
		if (Pipe.from(ringBuffer) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		int messagesPerRing = (1<<(ringBuffer.bitsOfSlabRing-1));
		outputTarget = step-messagesPerRing;//this value is negative		
		tailPosCache = tailPosition(ringBuffer);
		
		countDownInit = messagesPerRing>>2;
		countDown = countDownInit;
		
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        Pipe.addMsgIdx(ringBuffer, 0);
		Pipe.validateVarLength(ringBuffer, csq.length());
		int sourceLen = csq.length();
		final int p = Pipe.copyASCIIToBytes(csq, 0, sourceLen, ringBuffer); 
		Pipe.addBytePosAndLen(ringBuffer, p, sourceLen);
		
		if ((--countDown)<=0) {
			Pipe.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        Pipe.addMsgIdx(ringBuffer, 0);
		int length = end-start;
		Pipe.validateVarLength(ringBuffer, csq.length());
		final int p = Pipe.copyASCIIToBytes(csq, start, length, ringBuffer); 
		Pipe.addBytePosAndLen(ringBuffer, p, length);
		
		if ((--countDown)<=0) {
			Pipe.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
	    Pipe.addMsgIdx(ringBuffer, 0);
		Pipe.validateVarLength(ringBuffer,temp.length);
		int sourceLen = temp.length;
		final int p = Pipe.copyASCIIToBytes(temp, 0, sourceLen,	ringBuffer); 
		Pipe.addBytePosAndLen(ringBuffer, p, sourceLen);
		
		if ((--countDown)<=0) {
			Pipe.publishWrites(ringBuffer);
			countDown = countDownInit;
		}
		return this;
	}

	public void flush() {
		PipeWriter.blockWriteFragment(ringBuffer,0);
		RingStreams.writeEOF(ringBuffer);
	}
}
