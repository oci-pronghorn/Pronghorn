package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class AppendableBuilder implements AppendableByteWriter<AppendableBuilder> {

	private int maximumAllocation;
	private static final int defaultSize = 1<<14;
	
    private byte[] buffer;	
	private int byteCount;

    
	//This class is allowed to grow but only up to the maximumAllocation
	public AppendableBuilder(int maximumAllocation) {
		
		this.maximumAllocation = maximumAllocation;		
		this.buffer = new byte[maximumAllocation<defaultSize?maximumAllocation:defaultSize];

	}
	
	public void clear() {
		byteCount = 0;	
	}

	public String toString() {
		return new String(buffer,0,byteCount);
	}
	
	public int byteLength() {
		return byteCount;
	}

	public int copyTo(OutputStream target) {	
		return copyTo(Integer.MAX_VALUE, target);
	}
	
	public int copyTo(int maxBytes, OutputStream target) {	
		int pos = 0;
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

	public void copyTo(Appendable target) {
		
		if (target instanceof DataOutputBlobWriter) {
			((DataOutputBlobWriter)target).write(buffer, 0, byteCount, Integer.MAX_VALUE);
		} else {		
			Appendables.appendUTF8(target, buffer, 0, byteCount, Integer.MAX_VALUE);
		}
		
	}

	
	@Override
	public AppendableBuilder append(CharSequence csq) {
		
		int len = csq.length();
		
		int req = byteCount+(len<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
		byteCount+=bytesConsumed;

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
	public AppendableBuilder append(CharSequence csq, int start, int end) {
		
		int len = end-start;
		
		int req = byteCount+(len<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
		byteCount+=bytesConsumed;

		return this;
	}

	@Override
	public AppendableBuilder append(char c) {
	
		int req = byteCount+(1<<3); 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);

		return this;
	}

	@Override
	public void write(byte[] encodedBlock, int pos, int len) {
		int req = byteCount+len; 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		Pipe.copyBytesFromArrayToRing(encodedBlock, pos, 
				buffer, byteCount, Integer.MAX_VALUE, 
				len);
		this.byteCount+=len;
	}

	@Override
	public void write(byte[] encodedBlock) {
		int req = byteCount+encodedBlock.length; 
		if (req > buffer.length) {
			growNow(req);			
		}
		
		Pipe.copyBytesFromArrayToRing(encodedBlock, 0, 
				buffer, byteCount, Integer.MAX_VALUE, 
				encodedBlock.length);
		this.byteCount+=encodedBlock.length;
	}

	@Override
	public void writeByte(int asciiChar) {
		int req = byteCount+1; 
		if (req > buffer.length) {
			growNow(req);			
		}
		buffer[byteCount++] = (byte)asciiChar;
	}

	public int absolutePosition() {
		return byteCount;
	}

	public void absolutePosition(int absolutePosition) {
		byteCount = absolutePosition;
	}



}
