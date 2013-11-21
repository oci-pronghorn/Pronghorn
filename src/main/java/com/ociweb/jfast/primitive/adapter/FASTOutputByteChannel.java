package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.read.FASTException;

public class FASTOutputByteChannel implements FASTOutput {

	private final GatheringByteChannel channel;
	private ByteBuffer writerBuffer;
	private DataTransfer dataTransfer;
	
	
	public FASTOutputByteChannel(GatheringByteChannel channel) {
		this.channel = channel;
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.writerBuffer = dataTransfer.wrap();
		this.dataTransfer = dataTransfer;
	}

	@Override
	public void flush() {

		try {
			int size = dataTransfer.nextBlockSize();
			while (size>0) {
				
				int offset = dataTransfer.nextOffset(); //must only call once per iteration
				writerBuffer.clear();
				writerBuffer.position(offset);
				writerBuffer.limit(offset+size);
				
				channel.write(writerBuffer);
				
				size = dataTransfer.nextBlockSize();
				
			}
		} catch (IOException e) {
			throw new FASTException(e);
		}

	}
	
	
	
}
