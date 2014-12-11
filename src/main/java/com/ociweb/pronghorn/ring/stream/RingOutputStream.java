package com.ociweb.pronghorn.ring.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class RingOutputStream extends OutputStream {

	private RingBuffer ring;
	private int blockSize;
	private byte[] oneByte = new byte[1];
	
	public RingOutputStream(RingBuffer ring) {
		this.ring = ring;
		blockSize = ring.maxAvgVarLen;
		
		if (ring.consumerData.from != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		oneByte[0] = (byte)(0xFF&b);
		RingStreams.writeBytesToRing(oneByte, 0, 1, ring, blockSize);
	}

	@Override
	public void write(byte[] b) throws IOException {
		RingStreams.writeBytesToRing(b, 0, b.length, ring, blockSize);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		RingStreams.writeBytesToRing(b, off, len, ring, blockSize);
	}

	@Override
	public void close() throws IOException {
		RingStreams.writeEOF(ring);
	}
}
