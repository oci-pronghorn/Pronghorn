package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;

public class InverseDCT {
	public static short[] MCUInverseDCT(short[] mcu) {
		/*System.out.print("Before Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcu[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		short[] result = new short[64];
		
		for (int y = 0; y < 8; ++y) {
			for (int x = 0; x < 8; ++x) {
				double sum = 0.0;
				for (int i = 0; i < 8; ++i) {
					for (int j = 0; j < 8; ++j) {
						double Cu = 1.0;
						double Cv = 1.0;
						if (i == 0) {
							Cv = 1 / Math.sqrt(2.0);
						}
						if (j == 0) {
							Cu = 1 / Math.sqrt(2.0);
						}
						sum += Cu * Cv * mcu[i * 8 + j] *
							   Math.cos((2.0 * x + 1.0) * j * Math.PI / 16.0) *
							   Math.cos((2.0 * y + 1.0) * i * Math.PI / 16.0);
					}
				}
				sum /= 4.0;
				result[y * 8 + x] = (short)sum;
			}
		}
		
		/*System.out.print("After Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(result[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		return result;
	}
	
	public static void inverseDCT(ArrayList<MCU> mcus) {
		for (int i = 0; i < mcus.size(); ++i) {
			mcus.get(i).y =  MCUInverseDCT(mcus.get(i).y);
			mcus.get(i).cb = MCUInverseDCT(mcus.get(i).cb);
			mcus.get(i).cr = MCUInverseDCT(mcus.get(i).cr);
		}
		return;
	}
	
	/*public static void main(String[] args) {
		double[][] mcu = new double[][] {
			{  6.1917, -0.3411,  1.2418,  0.1492,  0.1583,  0.2742, -0.0724,  0.0561 },
			{  0.2205,  0.0214,  0.4503,  0.3947, -0.7846, -0.4391,  0.1001, -0.2554 },
			{  1.0423,  0.2214, -1.0017, -0.2720,  0.0789, -0.1952,  0.2801,  0.4713 },
			{ -0.2340, -0.0392, -0.2617, -0.2866,  0.6351,  0.3501, -0.1433,  0.3550 },
			{  0.2750,  0.0226,  0.1229,  0.2183, -0.2583, -0.0742, -0.2042, -0.5906 },
			{  0.0653,  0.0428, -0.4721, -0.2905,  0.4745,  0.2875, -0.0284, -0.1311 },
			{  0.3169,  0.0541, -0.1033, -0.0225, -0.0056,  0.1017, -0.1650, -0.1500 },
			{ -0.2970, -0.0627,  0.1960,  0.0644, -0.1136, -0.1031,  0.1887,  0.1444 }
		};
		short[][] result = MCUInverseDCT(mcu);
		for (int i = 0; i < 8; i++) {
			for (int j = 0; j < 8; j++) {
				System.out.print(String.format("%04d ", result[i][j]));
			}
			System.out.println();
		}
	}*/
}
