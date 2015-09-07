package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnTail;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;

public class AppendableUTF8Ring implements Appendable {

	private final Pipe ringBuffer;
	private final char[] temp = new char[1];
	private long outputTarget;
	private long tailPosCache;
	
	private final static int step = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	public AppendableUTF8Ring(Pipe ringBuffer) {

		this.ringBuffer = ringBuffer;
		if (Pipe.from(ringBuffer) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		int messagesPerRing = (1<<(ringBuffer.bitsOfStructuredLayoutRingBuffer-1));
		outputTarget = step-messagesPerRing;//this value is negative		
		tailPosCache = tailPosition(ringBuffer);
		
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        Pipe.addMsgIdx(ringBuffer, 0);
		Pipe.validateVarLength(ringBuffer, csq.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.addBytePosAndLen(ringBuffer, Pipe.bytesWorkingHeadPosition(ringBuffer), Pipe.copyUTF8ToByte(csq, csq.length(), ringBuffer));

		Pipe.publishWrites(ringBuffer);

		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
        Pipe.addMsgIdx(ringBuffer, 0);
		Pipe.validateVarLength(ringBuffer, csq.length()<<3);//UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.addBytePosAndLen(ringBuffer, Pipe.bytesWorkingHeadPosition(ringBuffer),  Pipe.copyUTF8ToByte(csq, end-start, ringBuffer));
		
		Pipe.publishWrites(ringBuffer);

		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=step;
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
	    Pipe.addMsgIdx(ringBuffer, 0);
		Pipe.validateVarLength(ringBuffer, temp.length<<3);
		 //UTF8 encoded bytes are longer than the char count (6 is the max but math for 8 is cheaper)
		Pipe.addBytePosAndLen(ringBuffer, Pipe.bytesWorkingHeadPosition(ringBuffer), Pipe.copyUTF8ToByte(temp, temp.length, ringBuffer));

		Pipe.publishWrites(ringBuffer);

		return this;
	}
	
	public void flush() {
		tailPosCache = spinBlockOnTail(tailPosCache, outputTarget, ringBuffer);
        outputTarget+=2;
		RingStreams.writeEOF(ringBuffer);
	}

}
