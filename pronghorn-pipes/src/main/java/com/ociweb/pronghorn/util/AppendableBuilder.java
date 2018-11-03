package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class AppendableBuilder implements AppendableByteWriter<AppendableBuilder> {

	private int maximumAllocation;
	private static final int defaultSize = 1<<15;
	
    private byte[] buffer;	
	private int byteCount;

	public AppendableBuilder() {
		this(Integer.MAX_VALUE);
	}
	
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

	public int copyTo(ChannelWriter target, int sourcePos) {	
		int len = Math.min(target.remaining(), byteCount-sourcePos);		
		target.write(buffer,sourcePos,len);
		return len;
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

	public static void appendLongAsText(AppendableBuilder ab, long value, boolean useNegPara) {
		
		if (ab.byteCount+(18) <= ab.buffer.length) {		
			ab.byteCount = Appendables.longToChars(value, useNegPara, ab.buffer, 
	        		                                        Integer.MAX_VALUE, ab.byteCount);
		} else {
			growNow(ab, ab.byteCount+(18));
			ab.byteCount = Appendables.longToChars(value, useNegPara, ab.buffer, 
                    Integer.MAX_VALUE, ab.byteCount);
		}
	}
	
	@Override
	public AppendableBuilder append(CharSequence csq) {
		
		int len = csq.length();		
		if (byteCount+(len<<3) <= buffer.length) {
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;		
		} else {
			growNow(this,byteCount+(len<<3));			
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, 0, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;
		}

		return this;
	}

	
	private static void growNow(AppendableBuilder that, int req) {
		if (req > that.maximumAllocation) {
			throw new UnsupportedOperationException("Max allocation was limited to "+that.maximumAllocation+" but more space needed");
		}
		byte[] temp = new byte[req];
		System.arraycopy(that.buffer, 0, temp, 0, that.buffer.length);
		that.buffer = temp;
	}

	@Override
	public AppendableBuilder append(CharSequence csq, int start, int end) {
		
		int len = end-start;

		if (byteCount+(len<<3) <= buffer.length) {
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;
		} else {
			growNow(this,byteCount+(len<<3));			
			int bytesConsumed = Pipe.copyUTF8ToByte(csq, start, buffer, Integer.MAX_VALUE, byteCount, len);
			byteCount+=bytesConsumed;

		}

		return this;
	}

	@Override
	public AppendableBuilder append(char c) {	
		if (byteCount+(1<<3) <= buffer.length) {
			byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);
		} else {		
			growNow(this,byteCount+(1<<3));			
			byteCount = Pipe.encodeSingleChar(c, buffer, Integer.MAX_VALUE, byteCount);
		}
		return this;
	}

	@Override
	public void write(byte[] encodedBlock, int pos, int len) {
		
		if (byteCount+len <= buffer.length) {
			Pipe.copyBytesFromArrayToRing(encodedBlock, pos, 
					buffer, byteCount, Integer.MAX_VALUE, 
					len);
		} else {
			growNow(this,byteCount+len);			
			Pipe.copyBytesFromArrayToRing(encodedBlock, pos, 
					buffer, byteCount, Integer.MAX_VALUE, 
					len);			
		}
		
		this.byteCount+=len;
	}

	@Override
	public void write(byte[] encodedBlock) {
		
		if (byteCount+encodedBlock.length <= buffer.length) {
			
			Pipe.copyBytesFromArrayToRing(encodedBlock, 0, 
					buffer, byteCount, Integer.MAX_VALUE, 
					encodedBlock.length);
			this.byteCount+=encodedBlock.length;
		} else {
			growNow(this,byteCount+encodedBlock.length);
			Pipe.copyBytesFromArrayToRing(encodedBlock, 0, 
					buffer, byteCount, Integer.MAX_VALUE, 
					encodedBlock.length);
			this.byteCount+=encodedBlock.length;
		}
	}

	@Override
	public void writeByte(int asciiChar) {
		
		if (byteCount+1 <= buffer.length) {
			buffer[byteCount++] = (byte)asciiChar;
		} else {
			growNow(this,byteCount+1);			
			buffer[byteCount++] = (byte)asciiChar;
		}
	}

	public int absolutePosition() {
		return byteCount;
	}

	public void absolutePosition(int absolutePosition) {
		byteCount = absolutePosition;
	}

	public boolean isEqual(ChannelReader target) {
		return target.equalBytes(buffer, 0, byteCount);
	}

	public int base64Decode(byte[] target, int targetIdx, int targetMask) {
		return Appendables.decodeBase64(buffer, 0, byteCount, Integer.MAX_VALUE,
							    target, targetIdx, targetMask);
	}


}
