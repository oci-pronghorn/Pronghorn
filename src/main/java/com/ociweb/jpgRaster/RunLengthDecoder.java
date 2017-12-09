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


	private static short[] decodeRLE(short inputArr[]) {
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
		return arr.byteArray;
	}
	
	public static ArrayList<MCU> decodeRLEMCUs(ArrayList<MCU> mcus) {
		ArrayList<MCU> decodedMCUs = new ArrayList<MCU>(mcus.size());
		for (int i = 0; i < mcus.size(); ++i) {
			decodedMCUs.add(new MCU());
			decodedMCUs.get(i).yDc = mcus.get(i).yDc;
			decodedMCUs.get(i).cbDc = mcus.get(i).cbDc;
			decodedMCUs.get(i).crDc = mcus.get(i).crDc;
			decodedMCUs.get(i).yAc = decodeRLE(mcus.get(i).yAc);
			decodedMCUs.get(i).cbAc = decodeRLE(mcus.get(i).cbAc);
			decodedMCUs.get(i).crAc = decodeRLE(mcus.get(i).crAc);
		}
		return decodedMCUs;
	}
}
