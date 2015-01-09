package com.ociweb.pronghorn.ring.stream;

import java.io.IOException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWalker;
import com.ociweb.pronghorn.ring.RingWriter;

public class AppendableUTF8Ring implements Appendable {

	final RingBuffer ringBuffer;
	final char[] temp = new char[1];
	final int chunk;
	
	public AppendableUTF8Ring(RingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		if (RingBuffer.from(ringBuffer) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		chunk = ringBuffer.maxAvgVarLen>>3;
		//TODO: B, should warn on small chunks and should auto divide content to fit? System.err.println("chunk:"+chunk);
	}
	
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		RingWriter.writeUTF8(ringBuffer, csq);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end)
			throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		RingWriter.writeUTF8(ringBuffer, csq, start, end-start);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		RingWalker.blockWriteFragment(ringBuffer, 0);
		temp[0]=c; //TODO: C, This should be optimized however callers should prefer to use the other two methods.
		RingWriter.writeUTF8(ringBuffer, temp);
		RingBuffer.publishWrites(ringBuffer);
		return this;
	}
	
	public void flush() {
		RingStreams.writeEOF(ringBuffer);
	}

}
