package com.ociweb.jpgRaster;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.RGB;

public class BMPDumper {
	public static void Dump(ArrayList<RGB> rgb, int height, int width, String filename) throws IOException {
		int size = 14 + 12 + 3 * rgb.size() + height * (4 - (width * 3) % 4);
		int apparentWidth;
		if (width % 8 == 0) {
			apparentWidth = width;
		}
		else {
			apparentWidth = width + (8 - (width % 8));
		}
		//System.out.println("Width: " + width);
		//System.out.println("Apparent Width: " + apparentWidth);
		
		DataOutputStream file = new DataOutputStream(new FileOutputStream(filename));
		file.writeByte('B');
		file.writeByte('M');
		writeInt(file, size);
		writeInt(file, 0);
		writeInt(file, 0x1A);
		writeInt(file, 12);
		writeShort(file, width);
		writeShort(file, height);
		writeShort(file, 1);
		writeShort(file, 24);
		for (int i = height - 1; i >= 0; i--) {
			for (int j = 0; j < width; j++) {
				file.writeByte(rgb.get(i * apparentWidth + j).b);
				file.writeByte(rgb.get(i * apparentWidth + j).g);
				file.writeByte(rgb.get(i * apparentWidth + j).r);
			}
			if ((width * 3) % 4 != 0) {
				for (int j = 0; j < 4 - (width * 3) % 4; j++) {
					file.writeByte(0);
				}
			}
		}
		file.close();
	}
	
	public static void writeInt(DataOutputStream stream, int v) throws IOException {
		stream.writeByte((v & 0x000000FF));
		stream.writeByte((v & 0x0000FF00) >>  8);
		stream.writeByte((v & 0x00FF0000) >> 16);
		stream.writeByte((v & 0xFF000000) >> 24);
	}
	
	public static void writeShort(DataOutputStream stream, int v) throws IOException {
		stream.writeByte((v & 0x00FF));
		stream.writeByte((v & 0xFF00) >>  8);
	}
	
	public static void main(String[] args) {
		ArrayList<RGB> rgb = new ArrayList<RGB>(8 * 8);
		for (int i = 0; i < 64; i++) {
			rgb.add(new RGB());
		}
		// red
		rgb.get(0 * 8 + 0).r = 255;
		rgb.get(0 * 8 + 0).g = 0;
		rgb.get(0 * 8 + 0).b = 0;
		// green
		rgb.get(0 * 8 + 1).r = 0;
		rgb.get(0 * 8 + 1).g = 255;
		rgb.get(0 * 8 + 1).b = 0;
		// blue
		rgb.get(0 * 8 + 2).r = 0;
		rgb.get(0 * 8 + 2).g = 0;
		rgb.get(0 * 8 + 2).b = 255;
		// cyan
		rgb.get(1 * 8 + 0).r = 0;
		rgb.get(1 * 8 + 0).g = 255;
		rgb.get(1 * 8 + 0).b = 255;
		// magenta
		rgb.get(1 * 8 + 1).r = 255;
		rgb.get(1 * 8 + 1).g = 0;
		rgb.get(1 * 8 + 1).b = 255;
		// yellow
		rgb.get(1 * 8 + 2).r = 255;
		rgb.get(1 * 8 + 2).g = 255;
		rgb.get(1 * 8 + 2).b = 0;
		// black
		rgb.get(2 * 8 + 0).r = 0;
		rgb.get(2 * 8 + 0).g = 0;
		rgb.get(2 * 8 + 0).b = 0;
		// gray
		rgb.get(2 * 8 + 1).r = 128;
		rgb.get(2 * 8 + 1).g = 128;
		rgb.get(2 * 8 + 1).b = 128;
		// white
		rgb.get(2 * 8 + 2).r = 255;
		rgb.get(2 * 8 + 2).g = 255;
		rgb.get(2 * 8 + 2).b = 255;
		try {
			Dump(rgb, 3, 3, "bmp_test.bmp");
		} catch (IOException e) {
			System.out.println("Error - Unknown error creating BMP file");
		}
	}
}
