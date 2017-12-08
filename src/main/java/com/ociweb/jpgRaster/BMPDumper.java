package com.ociweb.jpgRaster;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class BMPDumper {
	public static class RGB {
		public short r;
		public short g;
		public short b;
	}
	
	public static void Dump(RGB[] rgb, short height, short width, String filename) throws IOException {
		int size = 14 + 12 + 3 * rgb.length + height * (4 - (width * 3) % 4);
		
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
				file.writeByte(rgb[i * width + j].b);
				file.writeByte(rgb[i * width + j].g);
				file.writeByte(rgb[i * width + j].r);
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
		RGB[] rgb = new RGB[9];
		for (int i = 0; i < rgb.length; i++) {
			rgb[i] = new RGB();
		}
		rgb[0].r = 255;
		rgb[0].g = 0;
		rgb[0].b = 0;
		rgb[1].r = 0;
		rgb[1].g = 255;
		rgb[1].b = 0;
		rgb[2].r = 0;
		rgb[2].g = 0;
		rgb[2].b = 255;
		rgb[3].r = 255;
		rgb[3].g = 0;
		rgb[3].b = 0;
		rgb[4].r = 0;
		rgb[4].g = 255;
		rgb[4].b = 0;
		rgb[5].r = 0;
		rgb[5].g = 0;
		rgb[5].b = 255;
		rgb[6].r = 255;
		rgb[6].g = 0;
		rgb[6].b = 0;
		rgb[7].r = 0;
		rgb[7].g = 255;
		rgb[7].b = 0;
		rgb[8].r = 0;
		rgb[8].g = 0;
		rgb[8].b = 255;
		try {
			Dump(rgb, (short)3, (short)3, "bmp_test.bmp");
		} catch (IOException e) {
			System.out.println("Error - Unknown error creating BMP file");
		}
	}
}
