package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.headPosition;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;

import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class RingOutputStream extends OutputStream implements AutoCloseable {

	private Pipe pipe;
	private int blockSize;
	private byte[] oneByte = new byte[1];
	
	public RingOutputStream(Pipe pipe) {
		this.pipe = pipe;
		blockSize = pipe.maxVarLen;
		
		if (Pipe.from(pipe) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
	
	@Override
	public void write(int b) {
		oneByte[0] = (byte)(0xFF&b);
		RingStreams.writeBytesToRing(oneByte, 0, 1, pipe, blockSize);
	}

	@Override
	public void write(byte[] b) {
		RingStreams.writeBytesToRing(b, 0, b.length, pipe, blockSize);
	}

	@Override
	public void write(byte[] b, int off, int len) {
		RingStreams.writeBytesToRing(b, off, len, pipe, blockSize);
	}
	
	@Override
	public void close() {
		long lastCheckedValue = tailPosition(pipe);
		while (null==Pipe.slab(pipe) || lastCheckedValue < headPosition(pipe)-(1 + pipe.slabMask - Pipe.EOF_SIZE)) {
			Pipe.spinWork(pipe);
		    lastCheckedValue = Pipe.tailPosition(pipe);
		}
		Pipe.publishEOF(pipe);
	}
}
