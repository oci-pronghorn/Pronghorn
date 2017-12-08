package com.ociweb.jpgRaster;

public class InverseDCT {
	public static short[][] MCUInverseDCT(float[][] mcu) {
		short[][] IDCTresult = new short[8][8];
		for (int y = 0; y < 8; y++) {
			for (int x = 0; x < 8; x++) {
				float outerSum = 0;
				for (int i = 0; i < 8; i++) {
					float innerSum = 0;
					for (int j = 0; j < 8; j++) {
						float Cu = 1.0f;
						float Cv = 1.0f;
						if (i == 0 && j == 0) {
							Cu = Cv = 1 / (float)Math.sqrt(2.0f);
						}
						innerSum += Cu * Cv * mcu[i][j] *
									Math.cos(((2 * x + 1) * j * Math.PI) / 16) *
								    Math.cos(((2 * y + 1) * i * Math.PI) / 16);
					}
					outerSum += innerSum;
				}
				IDCTresult[y][x] = (short)(outerSum / 4.0f);
			}
		}
		return IDCTresult;
	}
	
	public static void main(String[] args) {
		float[][] mcu = new float[][] {{ (float)  -370, (float) -29.7, (float) -2.6, (float) -2.5, (float) -1.1, (float) -3.7, (float) -1.5, (float) -0.08 },
									   { (float)  -231, (float) -44.9, (float) 24.5, (float) -0.3, (float)  9.3, (float)  3.9, (float)  4.3, (float)  -1.4 },
									   { (float)  62.8, (float)  -8.5, (float) -7.6, (float) -2.7, (float)  0.3, (float) -0.4, (float)  0.5, (float)  -0.8 },
									   { (float)  12.5, (float) -14.6, (float) -3.5, (float) -3.4, (float)  2.4, (float) -1.3, (float)  2.7, (float)  -0.4 },
									   { (float)  -4.9, (float)  -3.9, (float)  0.9, (float)  3.6, (float)  0.1, (float)  5.1, (float)  1.1, (float)   0.5 },
									   { (float)  -0.5, (float)   3.1, (float) -1.4, (float)  0.2, (float) -1.1, (float) -1.5, (float) -1.1, (float)   0.9 },
									   { (float)  -4.4, (float)   2.3, (float) -1.7, (float) -1.6, (float)  1.1, (float) -2.7, (float)  1.1, (float)  -1.4 },
									   { (float) -10.2, (float)  -1.8, (float)  5.9, (float) -0.4, (float)  0.3, (float)  0.4, (float) -1.0, (float)   0.0 }};
		short[][] result = MCUInverseDCT(mcu);
		for (int i = 0; i < 8; i++) {
			for (int j = 0; j < 8; j++) {
				System.out.print(String.format("%04d ", result[i][j]));
			}
			System.out.println();
		}
	}
}
