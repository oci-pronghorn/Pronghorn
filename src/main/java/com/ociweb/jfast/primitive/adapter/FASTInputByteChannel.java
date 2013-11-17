package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputByteChannel implements FASTInput {

	private final ReadableByteChannel channel;
	
	public FASTInputByteChannel(ReadableByteChannel sourceChannel) {
		this.channel = sourceChannel;
	}
	
	@Override
	public int fill(byte[] buffer, int offset, int count) {
		
		try {
			//TODO: poor implementation.
			int fetched = channel.read(ByteBuffer.wrap(buffer,offset,count));
			if (fetched<0) {
				return 0;
			} else {
				return fetched;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
