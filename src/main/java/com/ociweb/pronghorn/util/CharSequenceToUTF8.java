package com.ociweb.pronghorn.util;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;

public class CharSequenceToUTF8 {

	private byte[] backing = new byte[256];
	private int length;
	private final static Logger logger = (Logger) LoggerFactory.getLogger(CharSequenceToUTF8.class);
		
	public CharSequenceToUTF8 convert(CharSequence charSeq) {
		convert(charSeq, 0, charSeq.length());
		return this;
	}
	
	public CharSequenceToUTF8 append(CharSequence charSeq) {
		append(charSeq, 0, charSeq.length());
		return this;
	}
	
	public CharSequenceToUTF8 convert(CharSequence charSeq, int pos, int len) {
		if (len*8>backing.length) {
			backing = new byte[len*8];
			if (backing.length>(1<<24)) {
				logger.warn("large string conversion has caused backing to grow to {} ", backing.length);
			}
		}		
		length = Pipe.convertToUTF8(charSeq, pos, len, backing, 0, Integer.MAX_VALUE);
		return this;
	}
	
	public CharSequenceToUTF8 append(CharSequence charSeq, int pos, int len) {
		if ((length+(len*8))>backing.length) {
			byte[] newBacking = new byte[len*8];
			System.arraycopy(backing, 0, newBacking, 0, length);
			backing = newBacking;
			if (backing.length>(1<<24)) {
				logger.warn("large string conversion has caused backing to grow to {} ", backing.length);
			}
			
		}		
		length += Pipe.convertToUTF8(charSeq, pos, len, backing, length, Integer.MAX_VALUE);
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

	public void parseSetup(TrieParserReader trieParserReader) {
		TrieParserReader.parseSetup(trieParserReader, 
				backing, 0, length, 
				Integer.MAX_VALUE);
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

	public void clear() {
		length = 0;
		//do not leak data
		Arrays.fill(backing, (byte)0);
	}

	
}
