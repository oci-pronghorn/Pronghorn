//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputByteChannel implements FASTInput {

	private final SocketChannel socketChannel;
	private ByteBuffer byteBuffer;
	
	public FASTInputByteChannel(SocketChannel channel) {
		this.socketChannel = channel;
		assert(!channel.isBlocking()) : "Only non blocking SocketChannel is supported.";
	}
	
	@Override
	public int fill(byte[] buffer, int offset, int count) {
		
		try {
			
			byteBuffer.clear();
			byteBuffer.position(offset);
			byteBuffer.limit(offset+count);
			
			//Only non-blocking socket channel is supported so this read call will
			//return only the bytes that are immediately available.
			int fetched = socketChannel.read(byteBuffer);
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
