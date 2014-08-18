//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTOutputSocketChannel implements FASTOutput {

	private final SocketChannel channel;
	private ByteBuffer writerBuffer;
	private DataTransfer dataTransfer;
	
	
	public FASTOutputSocketChannel(SocketChannel channel) {
		this.channel = channel;
		
	}

	@Override
	public void init(DataTransfer dataTransfer) {
		this.writerBuffer = dataTransfer.wrappedByteBuffer;
		this.dataTransfer = dataTransfer;
	}

	@Override
	public void flush() {

		try {
			int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
			while (size>0) {
				
				int offset = PrimitiveWriter.nextOffset(dataTransfer.writer); //must only call once per iteration
				writerBuffer.clear();
				writerBuffer.position(offset);
				writerBuffer.limit(offset+size);
				
				channel.write(writerBuffer);
				
				size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
				
			}
		} catch (IOException e) {
			throw new FASTException(e);
		}

	}
	
	
	
}
