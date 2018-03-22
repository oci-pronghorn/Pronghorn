package com.ociweb.jpgRaster;

import java.util.ArrayList;
import com.ociweb.jpgRaster.j2r.HuffmanDecoder;

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
	
	public static final int[] zigZagMap = {
			0,   1,  8, 16,  9,  2,  3, 10,
			17, 24, 32, 25, 18, 11,  4,  5,
			12, 19, 26, 33, 40, 48, 41, 34,
			27, 20, 13,  6,  7, 14, 21, 28,
			35, 42, 49, 56, 57, 50, 43, 36,
			29, 22, 15, 23, 30, 37, 44, 51,
			58, 59, 52, 45, 38, 31, 39, 46,
			53, 60, 61, 54, 47, 55, 62, 63
			};
	
	private static final int [] qTable0Vals = {
            16,   11,  12,  14,  12,  10,  16,  14,
            13,   14,  18,  17,  16,  19,  24,  40,
            26,   24,  22,  22,  24,  49,  35,  37,
            29,   40,  58,  51,  61,  60,  57,  51,
            56,   55,  64,  72,  92,  78,  64,  68,
            87,   69,  55,  56,  80, 109,  81,  87,
            95,   98, 103, 104, 103,  62,  77, 113,
            121, 112, 100, 120,  92, 101, 103,  99
    		};
	
	private static final int [] qTable1Vals = {
            17,  18,  18,  24,  21,  24,  47,  26,
            26,  47,  99,  66,  56,  66,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99,
            99,  99,  99,  99,  99,  99,  99,  99
    		};
	
	private static final short hDCTable0Lengths[] = {
            0, 1, 5, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0 };
    private static final short hDCTable0Symbols[] = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    
    private static final short hDCTable1Lengths[] = {
            0, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0 };
    private static final short hDCTable1Symbols[] = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    
    private static final short hACTable0Lengths[] = {
            0, 2, 1, 3, 3, 2, 4, 3, 5, 5, 4, 4, 0, 0, 1, 125 };
    private static final short hACTable0Symbols[] = {
            0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12,
            0x21, 0x31, 0x41, 0x06, 0x13, 0x51, 0x61, 0x07,
            0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xa1, 0x08,
            0x23, 0x42, 0xb1, 0xc1, 0x15, 0x52, 0xd1, 0xf0,
            0x24, 0x33, 0x62, 0x72, 0x82, 0x09, 0x0a, 0x16,
            0x17, 0x18, 0x19, 0x1a, 0x25, 0x26, 0x27, 0x28,
            0x29, 0x2a, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
            0x3a, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
            0x4a, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59,
            0x5a, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69,
            0x6a, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
            0x7a, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
            0x8a, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98,
            0x99, 0x9a, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7,
            0xa8, 0xa9, 0xaa, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6,
            0xb7, 0xb8, 0xb9, 0xba, 0xc2, 0xc3, 0xc4, 0xc5,
            0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xd2, 0xd3, 0xd4,
            0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xe1, 0xe2,
            0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea,
            0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8,
            0xf9, 0xfa };
    
    private static final short hACTable1Lengths[] =  {
            0, 2, 1, 2, 4, 4, 3, 4, 7, 5, 4, 4, 0, 1, 2, 119 };
    private static final short hACTable1Symbols[] = {
            0x00, 0x01, 0x02, 0x03, 0x11, 0x04, 0x05, 0x21,
            0x31, 0x06, 0x12, 0x41, 0x51, 0x07, 0x61, 0x71,
            0x13, 0x22, 0x32, 0x81, 0x08, 0x14, 0x42, 0x91,
            0xa1, 0xb1, 0xc1, 0x09, 0x23, 0x33, 0x52, 0xf0,
            0x15, 0x62, 0x72, 0xd1, 0x0a, 0x16, 0x24, 0x34,
            0xe1, 0x25, 0xf1, 0x17, 0x18, 0x19, 0x1a, 0x26,
            0x27, 0x28, 0x29, 0x2a, 0x35, 0x36, 0x37, 0x38,
            0x39, 0x3a, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
            0x49, 0x4a, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58,
            0x59, 0x5a, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
            0x69, 0x6a, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
            0x79, 0x7a, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
            0x88, 0x89, 0x8a, 0x92, 0x93, 0x94, 0x95, 0x96,
            0x97, 0x98, 0x99, 0x9a, 0xa2, 0xa3, 0xa4, 0xa5,
            0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xb2, 0xb3, 0xb4,
            0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xc2, 0xc3,
            0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xd2,
            0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda,
            0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9,
            0xea, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8,
            0xf9, 0xfa };
	
	public static final QuantizationTable qTable0;
	public static final QuantizationTable qTable1;

	public static final HuffmanTable hDCTable0;
	public static final HuffmanTable hDCTable1;
	public static final HuffmanTable hACTable0;
	public static final HuffmanTable hACTable1;
	
	public static final ArrayList<ArrayList<Integer>> DCTableCodes0;
	public static final ArrayList<ArrayList<Integer>> DCTableCodes1;
	public static final ArrayList<ArrayList<Integer>> ACTableCodes0;
	public static final ArrayList<ArrayList<Integer>> ACTableCodes1;
	
	static {
		qTable0 = new QuantizationTable();
		qTable0.precision = 8;
		qTable0.tableID = 0;
		qTable0.table = qTable0Vals;
		qTable1 = new QuantizationTable();
		qTable1.precision = 8;
		qTable1.tableID = 1;
		qTable1.table = qTable1Vals;

		hDCTable0 = new HuffmanTable();
		hDCTable0.tableID = 0;
		int pos = 0;
		for (int i = 0; i < 16; ++i) {
			hDCTable0.symbols.add(new ArrayList<Short>());
			for (int j = 0; j < hDCTable0Lengths[i]; ++j, ++pos) {
				hDCTable0.symbols.get(i).add(hDCTable0Symbols[pos]);
			}
		}
		DCTableCodes0 = HuffmanDecoder.generateCodes(hDCTable0);

		hDCTable1 = new HuffmanTable();
		hDCTable1.tableID = 1;
		pos = 0;
		for (int i = 0; i < 16; ++i) {
			hDCTable1.symbols.add(new ArrayList<Short>());
			for (int j = 0; j < hDCTable1Lengths[i]; ++j, ++pos) {
				hDCTable1.symbols.get(i).add(hDCTable1Symbols[pos]);
			}
		}
		DCTableCodes1 = HuffmanDecoder.generateCodes(hDCTable1);

		hACTable0 = new HuffmanTable();
		hACTable0.tableID = 0;
		pos = 0;
		for (int i = 0; i < 16; ++i) {
			hACTable0.symbols.add(new ArrayList<Short>());
			for (int j = 0; j < hACTable0Lengths[i]; ++j, ++pos) {
				hACTable0.symbols.get(i).add(hACTable0Symbols[pos]);
			}
		}
		ACTableCodes0 = HuffmanDecoder.generateCodes(hACTable0);

		hACTable1 = new HuffmanTable();
		hACTable1.tableID = 1;
		pos = 0;
		for (int i = 0; i < 16; ++i) {
			hACTable1.symbols.add(new ArrayList<Short>());
			for (int j = 0; j < hACTable1Lengths[i]; ++j, ++pos) {
				hACTable1.symbols.get(i).add(hACTable1Symbols[pos]);
			}
		}
		ACTableCodes1 = HuffmanDecoder.generateCodes(hACTable1);
	}
}
