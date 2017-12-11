package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.RGB;

public class YCbCrToRGB {	
	public static RGB convertToRGB(short Y, short Cb, short Cr) {
		RGB rgb = new RGB();
		rgb.r = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.402 * ((double)Cr - 128) + 0.5)), 255);
		rgb.g = (short)Math.min(Math.max(0, Math.floor((double)Y - (0.114 * 1.772 * ((double)Cb - 128) + 0.299 * 1.402 * ((double)Cr - 128)) / 0.587 + 0.5)), 255);
		rgb.b = (short)Math.min(Math.max(0, Math.floor((double)Y + 1.772 * ((double)Cb - 128) + 0.5)), 255);
		return rgb;
	}
	
	public static ArrayList<RGB> convertYCbCrToRGB(ArrayList<MCU> mcus, Header header) {
		ArrayList<RGB> rgb = new ArrayList<RGB>(header.height * header.width);
		for (int i = 0; i < mcus.size(); ++i) {
			for (int j = 0; j < 64; ++j) {
				rgb.add(convertToRGB(mcus.get(i).y[j], mcus.get(i).cb[j], mcus.get(i).cr[j]));
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
