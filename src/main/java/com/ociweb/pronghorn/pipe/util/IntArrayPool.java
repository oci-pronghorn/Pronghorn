package com.ociweb.pronghorn.pipe.util;

//recycle fixed int arrays by length.
public class IntArrayPool {
	
	private final int[][][] data;
	private final boolean[][] locked;
	private final int[] lockedCount;
	
	public IntArrayPool(int maxSize) {
		
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
		
		if (that.lockedCount[size] == that.data[size].length) {
			that.locked[size] = grow(that.locked[size]);
			that.data[size] = grow(that.data[size]);
		} 		
		
		//we know 1 is free so find it.
		boolean[] temp = that.locked[size];
		int i = temp.length;
		//NOTE: if this becomes a long array we may want to start where we left off..
		while (--i>0) {
			if (!temp[i]) {
				temp[i] = true;
				that.lockedCount[size]++;
				return i;					
			}
		}
		throw new RuntimeException("Internal error, count was off");			
		
	}

	private static int[][] grow(int[][] source) {
		int[][] result = new int[source.length+1][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private static boolean[] grow(boolean[] source) {
		boolean[] result = new boolean[source.length+1];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	

}
