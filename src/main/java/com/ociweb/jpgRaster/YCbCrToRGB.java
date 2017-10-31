package com.ociweb.jpgRaster;

public class YCbCrToRGB {
	public class RGB {
		public int r;
		public int g;
		public int b;
	}
	
	public RGB convertToRGB(int Y, int Cb, int Cr) {
		RGB rgb = new RGB();
		rgb.r = (int)Math.min(Math.max(0, Math.floor((double)Y + 1.402 * ((double)Cr - 128) + 0.5)), 255);
		rgb.g = (int)Math.min(Math.max(0, Math.floor((double)Y - (0.114 * 1.772 * ((double)Cb - 128) + 0.299 * 1.402 * ((double)Cr - 128)) / 0.587 + 0.5)), 255);
		rgb.b = (int)Math.min(Math.max(0, Math.floor((double)Y + 1.772 * ((double)Cb - 128) + 0.5)), 255);
		return rgb;
	}
}
