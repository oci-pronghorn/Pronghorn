package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PackedBits {

	//When written out this must be big endian
	
	private byte[] bitsData;
	
	public PackedBits() {
		bitsData = new byte[0];
	}
	
	public void clear() {
		Arrays.fill(bitsData, (byte)0);
	}
	
	public void setValue(int index, int value) {
		assert((0==value) || (1==value)) : "unsupported value";
		
		int aIdx = index/7;
		int bIdx = index%7;
		
		if (bitsData.length <= aIdx) {
			byte[] temp = new byte[aIdx+1];
			System.arraycopy(bitsData, 0, temp, 0, bitsData.length);
			bitsData = temp;
		}
		
		bitsData[aIdx] = (byte)((bitsData[aIdx] & (~(1 << bIdx))) | (value << bIdx));
	}
	
	
	public void write(DataOutput out) throws IOException {
		//this is written out as big endian so we reverse the data
		int i = bitsData.length;
		boolean scanning = true;
		while (--i >= 0) {			
			int b = (int)bitsData[i];
			if ((!scanning) || 0!=b) {
				if (scanning) {
					//stop skipping since we found a non zero
					scanning = false;
				}
				if (i==0) {
					b |= 0x80;//stop on the last one..
				}
				out.write(b);
			}
		}
	}
	
}
