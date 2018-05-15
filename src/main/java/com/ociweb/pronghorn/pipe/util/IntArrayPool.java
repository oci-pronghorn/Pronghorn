package com.ociweb.pronghorn.pipe.util;

//recycle fixed int arrays by length.
public class IntArrayPool {
	
	private final int[][][] data;
	private final boolean[][] locked;
	private final int[] lockedCount;
	
	IntArrayPool(int maxSize) {
		
		data   = new int[maxSize][0][];
		locked = new boolean[maxSize][];
		lockedCount = new int[maxSize];
		
	}
	
	public static int[] getArray(IntArrayPool that, int size, int instance) {
		return that.data[size][instance];
	}
	
	public static void releaseLock(IntArrayPool that, int size, int instance) {
		assert(that.locked[size][instance]);
		that.locked[size][instance]=false;
		that.lockedCount[size]--;
	}
	
	public static int lockInstance(IntArrayPool that, int size) {
		
		if (that.lockedCount[size] < that.data[size].length) {
			//we know 1 is free so find it.
			boolean[] temp = that.locked[size];
			int i = temp.length;
			//NOTE: if this becomes a long array we may want to start where we left off..
			while (--i>=0) {
				if (!temp[i]) {
					temp[i] = true;
					that.lockedCount[size]++;
					return i;					
				}
			}
			throw new RuntimeException("Internal error, count was off");			
		} else {
			//more rare case where we must grow
			that.locked[size] = grow(that.locked[size]);
			that.data[size] = grow(that.data[size], size);
			int result = that.data[size].length-1;//last index is always the new empty one
			that.locked[size][result] = true;
			that.lockedCount[size]++;
			return result;
		}
		
	}

	private static int[][] grow(int[][] source, int size) {
		int base = null!=source? source.length : 0;
		int[][] result = new int[base+1][];
		if (null!=source) {
			System.arraycopy(source, 0, result, 0, source.length);
		}
		result[base] = new int[size];
		return result;
	}

	private static boolean[] grow(boolean[] source) {
		int base = null!=source? source.length : 0;
		boolean[] result = new boolean[base+1];
		if (null!=source) {
			System.arraycopy(source, 0, result, 0, source.length);
		}
		return result;
	}
	

}
