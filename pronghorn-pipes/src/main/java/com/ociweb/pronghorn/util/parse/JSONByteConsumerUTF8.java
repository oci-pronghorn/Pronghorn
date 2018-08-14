package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.struct.ByteSequenceValidator;
import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONByteConsumerUTF8 implements ByteConsumer {

	private final byte[][] encodedData;
	private final int[][] indexData;
	private int activeIndex;
	private long utf8Length;
	
	private static final Logger logger = LoggerFactory.getLogger(JSONByteConsumerUTF8.class);
		
	public JSONByteConsumerUTF8(byte[][] encodedData,
			                    int[][] indexData) {
		this.encodedData = encodedData;
		this.indexData = indexData;
	}
	
	public void activeIndex(int activeIndex) {
		this.activeIndex = activeIndex;
		this.utf8Length = 0;		
	}
	
	public long length() {
		return utf8Length;
	}

	/////////////////////////////////////////////////
	//we have leading count of bytes in this UTF8 text
	/////////////////////////////////////////////////
	
	private byte[] targetByteArray(int newPos, int maxPos) {
		byte[] data = encodedData[activeIndex];
		if ( maxPos > data.length) {
			//grow
			byte[] temp = new byte[maxPos*2];
			System.arraycopy(data, 0, temp, 0, newPos);
			data = encodedData[activeIndex] = temp;
		}
		return data;
	}

	@Override
	public void consume(byte[] backing, int pos, int len, int mask) {
			
		utf8Length = (utf8Length+len);
		int[] idx = indexData[activeIndex]; 
		int newPos = idx[0];//bytes count used first
		
		byte[] data = targetByteArray(newPos, newPos+len);
		Pipe.copyBytesFromToRing(backing, pos, mask, 
				                 data, newPos, Integer.MAX_VALUE, 
				                 len);
		
		idx[0] = newPos+len;
	
	}

	@Override
	public void consume(byte value) {
		
		//logger.info("consumed byte {} ", (char)value);
		utf8Length++;
		int[] idx = indexData[activeIndex]; 
		int newPos = idx[0];//bytes count used first		
		targetByteArray(newPos, newPos+1)[newPos++] = value;
		idx[0] = newPos+1;
	}

	public boolean validate(ByteSequenceValidator v) {
		
		if (null!=v) {
			//if we have a validation we need < 2G of text
			if (utf8Length>Integer.MAX_VALUE) {
				return false;
			}
			int len = (int)utf8Length;
			int startPos = (indexData[activeIndex][0]) - len;			
			return v.isValid(encodedData[activeIndex], startPos, len, Integer.MAX_VALUE);

		} else {
			return true;//no validator, all is good
		}
	}
}
