package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.RGB;

public class YCbCrToRGB {	
	public static RGB convertToRGB(short Y, short Cb, short Cr) {
		RGB rgb = new RGB();
		rgb.r = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.402 * ((double)Cr) + 0.5)), 255);
		rgb.g = (short)Math.min(Math.max(0, Math.floor((double)Y - (0.114 * 1.772 * ((double)Cb) + 0.299 * 1.402 * ((double)Cr)) / 0.587 + 0.5)), 255);
		rgb.b = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.772 * ((double)Cb) + 0.5)), 255);
		return rgb;
	}
	
	public static ArrayList<RGB> convertYCbCrToRGB(ArrayList<MCU> mcus, Header header) {
		int mcuHeight = (header.height + 7) / 8;
		int mcuWidth = (header.width + 7) / 8;
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
	
	/*public static void main(String[] args) {
		short[] testArray = {120, 120, 120, 180, 180, 180};
		short[] converted = convertAllToRGB(testArray);
		for (int i = 0; i < converted.length ; i += 3) {
			System.out.println(converted[i + 0] + ", " + converted[i + 1] + ", " + converted[i + 2]);
		}
	}*/
}
