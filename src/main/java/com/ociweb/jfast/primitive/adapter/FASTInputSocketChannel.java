//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputSocketChannel implements FASTInput {

	private final SocketChannel socketChannel;
	private ByteBuffer targetBuffer;
	
	public FASTInputSocketChannel(SocketChannel channel) {
		this.socketChannel = channel;
		assert(!channel.isBlocking()) : "Only non blocking SocketChannel is supported.";
	}
	
	@Override
	public int fill(int offset, int count) {
		
		try {
			
			targetBuffer.clear();
			targetBuffer.position(offset);
			targetBuffer.limit(offset+count);
			
			//Only non-blocking socket channel is supported so this read call will
			//return only the bytes that are immediately available.
			int fetched = socketChannel.read(targetBuffer);
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
	public void init(byte[] targetBuffer) {
		this.targetBuffer = ByteBuffer.wrap(targetBuffer);
	}

	@Override
	public boolean isEOF() {
		return !socketChannel.isConnected();
	}

    @Override
    public void block() {
        // TODO Auto-generated method stub
        
    }

}
