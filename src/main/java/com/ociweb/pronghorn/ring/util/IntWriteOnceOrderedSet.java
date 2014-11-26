package com.ociweb.pronghorn.ring.util;

/**
 * Write once list (immutable after write)
 * Not thread safe
 * Easily in-lines
 * Creates no runtime garbage
 * 
 * @author Nathan Tippy
 *
 */
public class IntWriteOnceOrderedSet {

	final int[] data;
	int idx;
		
	public IntWriteOnceOrderedSet(int bits) {
		//Should not be used for lists with more than 1 million elements.
		assert(bits<20) : "There are much better choices than this algo for large lists.";
		
		int size = 1<<bits;
		data = new int[size];
		idx = 0;		
	}
	
	/*
	 * Returns true if inserted because it was not already found.
	 */
	public static boolean addItem(IntWriteOnceOrderedSet list, int value) {
		int stop = list.idx;
		int i = 0;
		while (i<stop) {
			if (value == list.data[i++]) {
				return false;
			}
		}
		list.data[stop] = value;
		list.idx = stop+1;
		return true;
	}
	
	public static int itemCount(IntWriteOnceOrderedSet list) {
		return list.idx;
	}
	
	public static int getItem(IntWriteOnceOrderedSet list, int idx) {
		return list.data[idx];
	}
	
}
