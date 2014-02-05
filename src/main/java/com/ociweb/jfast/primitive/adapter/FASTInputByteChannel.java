//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputByteChannel implements FASTInput {

	private final ReadableByteChannel channel;
	private ByteBuffer byteBuffer;
	
	public FASTInputByteChannel(ReadableByteChannel sourceChannel) {
		this.channel = sourceChannel;
	}
	
	@Override
	public int fill(byte[] buffer, int offset, int count) {
		
		try {
			byteBuffer.clear();
			byteBuffer.position(offset);
			byteBuffer.limit(offset+count);
			
			int fetched = channel.read(byteBuffer);
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

	@Override
	public void init(DataTransfer dataTransfer) {
		byteBuffer = dataTransfer.wrap();
	}

}
