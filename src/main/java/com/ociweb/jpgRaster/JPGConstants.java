package com.ociweb.jpgRaster;

public class JPGConstants {
	// Start of Frame markers, non-differential, Huffman coding
	public static final int SOF0 = 0xC0; // Baseline DCT
	public static final int SOF1 = 0xC1; // Extended sequential DCT
	public static final int SOF2 = 0xC2; // Progressive DCT
	public static final int SOF3 = 0xC3; // Lossless (sequential)
	
	// Start of Frame markers, differential, Huffman coding
	public static final int SOF5 = 0xC5; // Differential sequential DCT
	public static final int SOF6 = 0xC6; // Differential progressive DCT
	public static final int SOF7 = 0xC7; // Differential lossless (sequential)
	
	// Huffman Table specification
	public static final int DHT = 0xC4; // Define Huffman Table(s)
	
	// Start of Frame markers, non-differential, arithmetic coding
	public static final int JPG = 0xC8; // Reserved for JPEG extensions
	public static final int SOF9 = 0xC9; // Extended sequential DCT
	public static final int SOF10 = 0xCA; // Progressive DCT
	public static final int SOF11 = 0xCB; // Lossless (sequential)
	
	// Start of Frame markers, differential, arithmetic coding
	public static final int SOF13 = 0xCD; // Differential sequential DCT
	public static final int SOF14 = 0xCE; // Differential progressive DCT
	public static final int SOF15 = 0xCF; // Differential lossless (sequential)
	
	// Arithmetic coding conditioning specification
	public static final int DAC = 0xCC; // Define Arithmetic Coding Conditioning(s)
	
	// Restart interval termination
	public static final int RST0 = 0xD0; // Restart interval modulo 8 count 0
	public static final int RST1 = 0xD1; // Restart interval modulo 8 count 1
	public static final int RST2 = 0xD2; // Restart interval modulo 8 count 2
	public static final int RST3 = 0xD3; // Restart interval modulo 8 count 3
	public static final int RST4 = 0xD4; // Restart interval modulo 8 count 4
	public static final int RST5 = 0xD5; // Restart interval modulo 8 count 5
	public static final int RST6 = 0xD6; // Restart interval modulo 8 count 6
	public static final int RST7 = 0xD7; // Restart interval modulo 8 count 7
	
	// Other Markers
	public static final int SOI = 0xD8; // Start of Image
	public static final int EOI = 0xD9; // End of Image
	public static final int SOS = 0xDA; // Start of Scan
	public static final int DQT = 0xDB; // Define Quantization Table(s)
	public static final int DNL = 0xDC; // Define Number of Lines
	public static final int DRI = 0xDD; // Define Restart Interval
	public static final int DHP = 0xDE; // Define Hierarchical Progression
	public static final int EXP = 0xDF; // Expand Reference Component(s)
	
	// APPN Markers
	public static final int APP0 = 0xE0;
	public static final int APP1 = 0xE1;
	public static final int APP2 = 0xE2;
	public static final int APP3 = 0xE3;
	public static final int APP4 = 0xE4;
	public static final int APP5 = 0xE5;
	public static final int APP6 = 0xE6;
	public static final int APP7 = 0xE7;
	public static final int APP8 = 0xE8;
	public static final int APP9 = 0xE9;
	public static final int APP10 = 0xEA;
	public static final int APP11 = 0xEB;
	public static final int APP12 = 0xEC;
	public static final int APP13 = 0xED;
	public static final int APP14 = 0xEE;
	public static final int APP15 = 0xEF;
	
	// Comment Marker
	public static final int COM = 0xFE;
	
	// Misc Markers
	public static final int JPG0 = 0xF0;
	public static final int JPG13 = 0xFD;
	public static final int TEM = 0x01;
}
