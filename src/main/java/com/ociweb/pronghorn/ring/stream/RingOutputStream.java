package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.io.OutputStream;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class RingOutputStream extends OutputStream implements AutoCloseable {

	private RingBuffer ring;
	private int blockSize;
	private byte[] oneByte = new byte[1];
	
	public RingOutputStream(RingBuffer ring) {
		this.ring = ring;
		blockSize = ring.maxAvgVarLen;
		
		if (RingBuffer.from(ring) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
	
	@Override
	public void write(int b) {
		oneByte[0] = (byte)(0xFF&b);
		RingStreams.writeBytesToRing(oneByte, 0, 1, ring, blockSize);
	}

	@Override
	public void write(byte[] b) {
		RingStreams.writeBytesToRing(b, 0, b.length, ring, blockSize);
	}

	@Override
	public void write(byte[] b, int off, int len) {
		RingStreams.writeBytesToRing(b, off, len, ring, blockSize);
	}
	
	@Override
	public void close() {
		spinBlockOnTail(tailPosition(ring), headPosition(ring)-(1 + ring.mask - RingBuffer.EOF_SIZE), ring);
        RingBuffer.publishEOF(ring);
	}
}
