package com.ociweb.pronghorn.util.primitive;

public class IntArrayHolder {

	public int[] data;
	
	public IntArrayHolder(int initSize) {
		data = new int[initSize];		
	}
	
	public void growIfNeeded(int index) {
		if (index >= data.length) {
			int[] temp = new int[index*2];
			System.arraycopy(data, 0, temp, 0, data.length);
			data = temp;			
		}
	}
	
}
