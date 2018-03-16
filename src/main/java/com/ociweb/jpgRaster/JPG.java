package com.ociweb.jpgRaster;

import java.util.ArrayList;

public class JPG {
	public static class QuantizationTable {
		public short tableID;
		public short precision;
		// using ints instead of shorts because
		//  precision might be 16 instead of 8
		public int[] table = new int[64];
	}
	
	public static class HuffmanTable {
		public short tableID;
		public ArrayList<ArrayList<Short>> symbols = new ArrayList<ArrayList<Short>>(16);
	}
	
	public static class ColorComponent {
		public short componentID;
		public short horizontalSamplingFactor;
		public short verticalSamplingFactor;
		public short quantizationTableID;
		public short huffmanACTableID;
		public short huffmanDCTableID;
		public boolean used;
	}
	
	public static class Header {
		public String filename;
		
		// DQT
		public QuantizationTable[] quantizationTables = new QuantizationTable[4];
		
		// DHT
		public HuffmanTable[] huffmanDCTables = new HuffmanTable[4];
		public HuffmanTable[] huffmanACTables = new HuffmanTable[4];
		
		// SOF
		public String frameType;
		public short precision;
		public int height;
		public int width;
		public short numComponents;
		public boolean zeroBased = false;
		
		// SOS
		public short startOfSelection;
		public short endOfSelection;
		public short successiveApproximationHigh;
		public short successiveApproximationLow;
		
		// DRI
		public int restartInterval = 0;

		public ColorComponent[] colorComponents = new ColorComponent[3];
		public ArrayList<Short> imageData = new ArrayList<Short>();
		
		public boolean valid = true;
	}
	
	public static class MCU {
		public short[] y = new short[64];
		public short[] cb = new short[64];
		public short[] cr = new short[64];
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
