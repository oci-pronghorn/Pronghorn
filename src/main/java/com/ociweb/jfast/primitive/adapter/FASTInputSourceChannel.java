//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe.SourceChannel;
import java.nio.channels.SocketChannel;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputSourceChannel implements FASTInput {

	private final SourceChannel sourceChannel;
	
	private ByteBuffer targetBuffer;
	
	public FASTInputSourceChannel(SourceChannel channel) {
		this.sourceChannel = channel;
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
			int fetched = sourceChannel.read(targetBuffer);
			
			// mask is FFFF for pos and 0000 for neg       ((fetched>>31)-1)
			//branched version is return fetched<0 ? 0 : fetched;
			return fetched & ((fetched>>31)-1);
			
		} catch (IOException e) {
		    throw new FASTException(e);
		}
	}

	@Override
	public void init(byte[] targetBuffer) {
		this.targetBuffer = ByteBuffer.wrap(targetBuffer);
	}

	@Override
	public boolean isEOF() {
		return !sourceChannel.isOpen();
	}

    @Override
    public int blockingFill(int offset, int count) {
       try {
           sourceChannel.configureBlocking(true);
           return fill(offset, count);        
        } catch (IOException e) {
            throw new FASTException(e);
        } finally {
            try {
                sourceChannel.configureBlocking(false);
            } catch (IOException e) {
                throw new FASTException(e);
            }            
        }        
    }

}
