package com.ociweb.pronghorn.util;

import java.util.Arrays;

import com.ociweb.pronghorn.pipe.Pipe;

public class CharSequenceToUTF8 {

	private byte[] backing = new byte[256];
	private int length;
		
	public CharSequenceToUTF8 convert(CharSequence charSeq) {
		convert(charSeq, 0, charSeq.length());
		return this;
	}
	
	public CharSequenceToUTF8 convert(CharSequence charSeq, int pos, int length) {
		if (length*8>backing.length) {
			backing = new byte[length*8];
		}		
		length = Pipe.convertToUTF8(charSeq, pos, length, backing, 0, Integer.MAX_VALUE);
		return this;
	}
	
	public byte[] asBytes() {
		return Arrays.copyOfRange(backing, 0, length);
	}
	
	public void copyInto(byte[] target, int offset, int mask) {
		Pipe.copyBytesFromArrayToRing(backing, 0, 
									  target, offset, 
									  mask, length);
	}

	public boolean isEquals(byte[] target) {
		if (target.length!=length) {
			return false;
		} else {
			int j = length;
			while (--j>=0) {
				if (target[j] != backing[j]) {
					return false;
				}				
			}
			return true;
		}
	}
	
}
