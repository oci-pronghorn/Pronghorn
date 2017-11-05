package com.ociweb.jpgRaster;

public class YCbCrToRGB {
	public static class RGB {
		public short r;
		public short g;
		public short b;
	}
	
	public static short[] convertAllToRGB(short[] ycbcr) {
		assert(ycbcr.length % 3 == 0);
		short[] rgbArray = new short[ycbcr.length];
		for (int i = 0; i < ycbcr.length ; i += 3) {
			RGB rgb = convertToRGB(ycbcr[i + 0], ycbcr[i + 1], ycbcr[i + 2]);
			rgbArray[i + 0] = rgb.r;
			rgbArray[i + 1] = rgb.g;
			rgbArray[i + 2] = rgb.b;
		}
		return rgbArray;
	}
	
	public static RGB convertToRGB(short Y, short Cb, short Cr) {
		RGB rgb = new RGB();
		rgb.r = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.402 * ((double)Cr - 128) + 0.5)), 255);
		rgb.g = (short)Math.min(Math.max(0, Math.floor((double)Y - (0.114 * 1.772 * ((double)Cb - 128) + 0.299 * 1.402 * ((double)Cr - 128)) / 0.587 + 0.5)), 255);
		rgb.b = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.772 * ((double)Cb - 128) + 0.5)), 255);
		return rgb;
	}
	
	public static void main(String[] args) {
		short[] testArray = {120, 120, 120, 180, 180, 180};
		short[] converted = convertAllToRGB(testArray);
		for (int i = 0; i < converted.length ; i += 3) {
			System.out.println(converted[i + 0] + ", " + converted[i + 1] + ", " + converted[i + 2]);
		}
	}
}
