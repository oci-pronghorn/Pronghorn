package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.RGB;

public class YCbCrToRGB {	
	private static RGB convertToRGB(short Y, short Cb, short Cr) {
		RGB rgb = new RGB();
		rgb.r = (short)((double)Y + 1.402 * ((double)Cr) + 128);
		rgb.g = (short)(((double)(Y) - (0.114 * (Y + 1.772 * (double)Cb)) - 0.299 * (Y + 1.402 * ((double)Cr))) / 0.587 + 128);
		rgb.b = (short)((double)Y + 1.772 * ((double)Cb) + 128);
		if (rgb.r < 0)   rgb.r = 0;
		if (rgb.r > 255) rgb.r = 255;
		if (rgb.g < 0)   rgb.g = 0;
		if (rgb.g > 255) rgb.g = 255;
		if (rgb.b < 0)   rgb.b = 0;
		if (rgb.b > 255) rgb.b = 255;
		//System.out.println("(" + Y + ", " + Cb + ", " + Cr + ") -> (" + rgb.r + ", " + rgb.g + ", " + rgb.b + ")");
		return rgb;
	}
	
	public static ArrayList<RGB> convertYCbCrToRGB(ArrayList<MCU> mcus, int height, int width) {
		int mcuHeight = (height + 7) / 8;
		int mcuWidth = (width + 7) / 8;
		ArrayList<RGB> rgb = new ArrayList<RGB>(mcuHeight * mcuWidth);
		for (int i = 0; i < mcuHeight; ++i) {         // mcu height
			for (int y = 0; y < 8; ++y) {             // pixel height
				for (int j = 0; j < mcuWidth; ++j) {  // mcu width
					for (int x = 0; x < 8; ++x) {     // pixel width
						rgb.add(convertToRGB(mcus.get(i * mcuWidth + j).y[y * 8 + x],
											 mcus.get(i * mcuWidth + j).cb[y * 8 + x],
											 mcus.get(i * mcuWidth + j).cr[y * 8 + x]));
					}
				}
			}
		}
		return rgb;
	}
	
	public static void main(String[] args) {
		ArrayList<MCU> testArray =  new ArrayList<MCU>(1);
		MCU mcu = new MCU();
		mcu.y[0]  = -63;
		mcu.cb[0] =  -1;
		mcu.cr[0] =   6;
		mcu.y[1]  = -74;
		mcu.cb[1] = -12;
		mcu.cr[1] =  11;
		mcu.y[2]  = 106;
		mcu.cb[2] =   6;
		mcu.cr[2] = -16;
		testArray.add(mcu);
		ArrayList<RGB> converted = convertYCbCrToRGB(testArray, 1, 3);
		for (int i = 0; i < converted.size() ; i += 3) {
			System.out.println(converted.get(i).r + ", " + converted.get(i).g + ", " + converted.get(i).b);
		}
	}
}
