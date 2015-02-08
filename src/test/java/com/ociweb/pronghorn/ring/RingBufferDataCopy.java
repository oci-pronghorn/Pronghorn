package com.ociweb.pronghorn.ring;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class RingBufferDataCopy {

	static byte[] byteSource = new byte[] {(byte)1,(byte)2,(byte)3,(byte)4};
	static int[] intSource = new int[] {1,2,3,4};
	
	static int sourceMask= 4-1;
	
	@Test
	public void simpleByteCopy() {
		
		
		int sourceloc = 0;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		
		assertTrue(Arrays.equals(byteSource, target));
		
	}
	
	@Test
	public void simpleByteCopyZero() {
		
		
		int sourceloc = 0;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));		
	}
	
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBoundZero() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)3,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOnBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)3,(byte)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOverBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)1,(byte)0,(byte)3,(byte)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitFullArray() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)1,(byte)2,(byte)3,(byte)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBoundAZero() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)4,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOnBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)4,(byte)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOverBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)2,(byte)0,(byte)4,(byte)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitFullArrayA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)2,(byte)3,(byte)4,(byte)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBoundBZero() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitUnderBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOnBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)4,(byte)0,(byte)0,(byte)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitOverBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)4,(byte)1,(byte)0,(byte)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopyDoubleSplitFullArrayB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)4,(byte)1,(byte)2,(byte)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleByteCopySplitSourceUnderBoundZero() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopySplitSourceUnderBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopySplitSourceOnBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)4,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopySplitSourceOverBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)4,(byte)1,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopySplitSourceFullArray() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)4,(byte)1,(byte)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleByteCopySplitTargetUnderBoundZero() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)0,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleByteCopySplitTargetUnderBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)1,(byte)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleByteCopySplitTargetOnBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)0,(byte)0,(byte)1,(byte)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleByteCopySplitTargetAboveBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)0,(byte)1,(byte)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleByteCopySplitTargetFullArray() {
		
		int sourceloc =  0;
		int targetloc = 2;
		
		byte[] target = new byte[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyBytesFromToRing(byteSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		byte[] expected = new byte[] {(byte)3,(byte)4,(byte)1,(byte)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	///same test except for int methods
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	
	
	@Test
	public void simpleintCopy() {
		
		
		int sourceloc = 0;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		
		assertTrue(Arrays.equals(intSource, target));
		
	}
	
	@Test
	public void simpleintCopyZero() {
		
		
		int sourceloc = 0;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));		
	}
	
	
	@Test
	public void simpleintCopyDoubleSplitUnderBoundZero() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitUnderBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)3,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOnBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)3,(int)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOverBound() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)1,(int)0,(int)3,(int)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitFullArray() {
		
		int sourceloc = 2;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)1,(int)2,(int)3,(int)4};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleintCopyDoubleSplitUnderBoundAZero() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitUnderBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)4,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOnBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)4,(int)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOverBoundA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)2,(int)0,(int)4,(int)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitFullArrayA() {
		
		int sourceloc = 3;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)2,(int)3,(int)4,(int)1};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleintCopyDoubleSplitUnderBoundBZero() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitUnderBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOnBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)4,(int)0,(int)0,(int)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitOverBoundB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)4,(int)1,(int)0,(int)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopyDoubleSplitFullArrayB() {
		
		int sourceloc = 2;
		int targetloc = 3;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)4,(int)1,(int)2,(int)3};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	
	//////////////////
	//////////////////
	
	@Test
	public void simpleintCopySplitSourceUnderBoundZero() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopySplitSourceUnderBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopySplitSourceOnBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)4,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopySplitSourceOverBound() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)4,(int)1,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopySplitSourceFullArray() {
		
		int sourceloc =  2;
		int targetloc = 0;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)4,(int)1,(int)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleintCopySplitTargetUnderBoundZero() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 0;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)0,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	@Test
	public void simpleintCopySplitTargetUnderBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 1;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)1,(int)0};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleintCopySplitTargetOnBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 2;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)0,(int)0,(int)1,(int)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleintCopySplitTargetAboveBound() {
		
		
		int sourceloc =  0;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 3;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)0,(int)1,(int)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	@Test
	public void simpleintCopySplitTargetFullArray() {
		
		int sourceloc =  0;
		int targetloc = 2;
		
		int[] target = new int[4];
		int targetMask = 4-1;
		
		int length = 4;
		
		RingBuffer.copyIntsFromToRing(intSource, sourceloc, sourceMask, target, targetloc, targetMask, length);
		
		int[] expected = new int[] {(int)3,(int)4,(int)1,(int)2};
		assertTrue("value was "+Arrays.toString(target), Arrays.equals(expected, target));
		
	}
	
	
	
	
	
	
}
