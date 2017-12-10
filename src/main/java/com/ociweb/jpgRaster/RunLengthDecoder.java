package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;

public class RunLengthDecoder {
	private static class dynamicArray {
		public short byteArray[];
		private int used;
		public int capacity;

		public dynamicArray(int size) {
			this.byteArray = new short[size];
			this.used = 0;
			this.capacity = size;
		}

		public void insert(short val) {
			if (this.used >= this.capacity) {
				short tempArray[] = new short[2*capacity];
				for (int i = 0; i < capacity; ++i) {
					tempArray[i] = this.byteArray[i];
				}
				this.byteArray = tempArray;
			}
			this.byteArray[this.used] = val;
			this.used++;
		}
	}

	public static final int INIT_SIZE = 8;


	private static void decodeRLEMCU(short inputArr[]) {
		dynamicArray arr = new dynamicArray(INIT_SIZE);
		for (int i = 0; i < inputArr.length; ++i) {
			// First value in nibble should be number of zeros in run
			for (int j = 0; j < inputArr[i]; ++j) {
				arr.insert((short)0); // Insert 0 for Zero RLE
			}
			++i;
			// Second value in nibble is an actual value
			arr.insert(inputArr[i]);
		}
		inputArr = arr.byteArray;
		return;
	}
	
	public static void decodeRLE(ArrayList<MCU> mcus) {
		for (int i = 0; i < mcus.size(); ++i) {
			decodeRLEMCU(mcus.get(i).yAc);
			decodeRLEMCU(mcus.get(i).cbAc);
			decodeRLEMCU(mcus.get(i).crAc);
		}
		return;
	}
}
