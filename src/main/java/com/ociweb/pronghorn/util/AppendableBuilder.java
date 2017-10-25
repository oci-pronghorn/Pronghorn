package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class AppendableBuilder implements Appendable {

	private int maximumAllocation;
	private static final int defaultSize = 1<<14;
	
    private byte[] buffer;	
	private int byteCount;
	private int charCount;
	private int pos;
    
	//This class is allowed to grow but only up to the maximumAllocation
	public AppendableBuilder(int maximumAllocation) {
		
		this.maximumAllocation = maximumAllocation;		
		this.buffer = new byte[maximumAllocation<defaultSize?maximumAllocation:defaultSize];

		clear();
	}
	
	public int charLength() {
		return charCount;
	}
	
	public int byteLength() {
		return byteCount;
	}
	
	//open for write
	public void clear() {
		this.byteCount = 0;
		this.charCount = 0;
		this.pos = 0;
	}
	
	public int copyTo(int maxBytes, OutputStream target) {		
		int len = Math.min(maxBytes, (byteCount-pos));
		assert(len>=0);
		try {
			target.write(buffer, pos, len);
			pos += len;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return len;
	}	
	
	@Override
	public Appendable append(CharSequence csq) {
		
		int len = csq.length();
		
		int req = byteCount+(len<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
		byteCount+=bytesConsumed;
		charCount+=len;
		
		return this;
	}

	private void growNow(int req) {
		if (req > maximumAllocation) {
			throw new UnsupportedOperationException("Max allocation was limited to "+maximumAllocation+" but more space needed");
		}
		byte[] temp = new byte[req];
		System.arraycopy(buffer, 0, temp, 0, buffer.length);
		buffer = temp;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		
		int len = end-start;
		
		int req = byteCount+(len<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
		byteCount+=bytesConsumed;
		charCount+=len;
		
		return this;
	}

	@Override
	public Appendable append(char c) {
	
		int req = byteCount+(1<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);
		charCount++;
		
		return this;
	}

}
