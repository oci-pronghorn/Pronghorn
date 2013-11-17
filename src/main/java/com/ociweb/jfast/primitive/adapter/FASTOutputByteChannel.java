package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;

public class FASTOutputByteChannel implements FASTOutput {

	private final GatheringByteChannel channel;
	private ByteBuffer writerBuffer;
	
	
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
		writerBuffer = dataTransfer.wrap();
	}
	
	
	
}
