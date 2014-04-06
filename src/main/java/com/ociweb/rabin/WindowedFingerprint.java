package com.ociweb.rabin;

import com.ociweb.jfast.field.TokenBuilder;

public class WindowedFingerprint {

	private final byte[] byteWindow;
	private final int byteWindowMask;
	private int windowBytes;
	private int head;
	private int tail;
	
	//These make up the chosen key for Rabin and must be the same for all instances. so putting it in the catalog would be easiest.
	private final int shift;
	private long[] popTable;  //256 length
	private long[] pushTable; //512 length
	
	///TODO: may need two sets of these to get the required error bars.
	public long fingerprint;
	
	WindowedFingerprint(int windowSize, int degree, long[] pushTable, long[] popTable) {
		assert(TokenBuilder.isPowerOfTwo(windowSize));
		this.windowBytes = windowSize; //must be power of 2 for rolling.
		this.byteWindowMask = windowSize-1;
		this.byteWindow = new byte[windowSize];
		
		this.shift = degree - 8;
		
		this.popTable = popTable;
		this.pushTable = pushTable;

		this.fingerprint = 0;
	}
	
	
	public void eat(byte b) {
		fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[(((short)(fingerprint >> shift)) & 0x1FF)];

//		//pull out tail first before we step on it
//		if (windowBytes>0) {
//			windowBytes--;
//		} else {
//			tail = tail&byteWindowMask;
//			fingerprint ^= popTable[(byteWindow[tail++] & 0xFF)];
//		}
		
//		head = head&byteWindowMask;
//		byteWindow[head++] = b;
				
	}
		
		
}
