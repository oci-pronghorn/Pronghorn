package com.ociweb.jpgRaster;

import java.util.ArrayList;

public class JPG {
	public static class QuantizationTable {
		short tableID;
		short precision;
		// using ints instead of shorts because
		//  precision might be 16 instead of 8
		int[] table = new int[64];
	}
	
	public static class HuffmanTable {
		short tableID;
		ArrayList<ArrayList<Short>> symbols = new ArrayList<ArrayList<Short>>(16);
	}
	
	public static class ColorComponent {
		short componentID;
		short horizontalSamplingFactor;
		short verticalSamplingFactor;
		short quantizationTableID;
		short huffmanACTableID;
		short huffmanDCTableID;
		boolean used;
	}
	
	public static class Header {
		// DQT
		QuantizationTable[] quantizationTables = new QuantizationTable[4];
		
		// DHT
		HuffmanTable[] huffmanDCTables = new HuffmanTable[4];
		HuffmanTable[] huffmanACTables = new HuffmanTable[4];
		
		// SOF
		String frameType;
		short precision;
		int height;
		int width;
		short numComponents;
		
		// SOS
		short startOfSelection;
		short endOfSelection;
		short successiveApproximationHigh;
		short successiveApproximationLow;
		
		// DRI
		int restartInterval = 0;

		ColorComponent[] colorComponents = new ColorComponent[3];
		ArrayList<Short> imageData = new ArrayList<Short>();
		
		boolean valid = true;
	}
	
	public static class MCU {		
		short[] y = new short[64];
		short[] cb = new short[64];
		short[] cr = new short[64];
	}
	
	public static int[] zigZagMap = new int[]  {
			0,   1,  8, 16,  9,  2,  3, 10,
			17, 24, 32, 25, 18, 11,  4,  5,
			12, 19, 26, 33, 40, 48, 41, 34,
			27, 20, 13,  6,  7, 14, 21, 28,
			35, 42, 49, 56, 57, 50, 43, 36,
			29, 22, 15, 23, 30, 37, 44, 51,
			58, 59, 52, 45, 38, 31, 39, 46,
			53, 60, 61, 54, 47, 55, 62, 63
			};
}
