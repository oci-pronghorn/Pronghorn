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
	public int flush(byte[] buffer, int offset, int length) {
		
		try {
			writerBuffer.clear();
			writerBuffer.position(offset);
			writerBuffer.limit(offset+length);
			
			channel.write(writerBuffer);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
		return length;
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
				
				writerBuffer.clear();
				writerBuffer.position(dataTransfer.nextOffset());
				writerBuffer.limit(dataTransfer.nextOffset()+size);
				
				channel.write(writerBuffer);
				
				size = dataTransfer.nextBlockSize();
				
			}
		} catch (IOException e) {
			throw new FASTException(e);
		}

	}
	
	
	
}
