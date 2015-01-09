package com.ociweb.pronghorn.ring.stream;

import java.io.IOException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWalker;
import com.ociweb.pronghorn.ring.RingWriter;

public class AppendableASCIIRing implements Appendable {

	RingBuffer ringBuffer;
	char[] temp = new char[1];
	
	public AppendableASCIIRing(RingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		if (RingBuffer.from(ringBuffer) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		RingWriter.writeASCII(ringBuffer, csq);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		RingWriter.writeASCII(ringBuffer, csq, start, end-start);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
		RingWriter.writeASCII(ringBuffer, temp);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}

	public void flush() {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		RingStreams.writeEOF(ringBuffer);
	}
}
