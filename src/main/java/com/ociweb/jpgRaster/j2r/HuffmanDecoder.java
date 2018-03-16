package com.ociweb.jpgRaster.j2r;

import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;

import java.util.ArrayList;

public class HuffmanDecoder {

	private static class BitReader {
		private int nextByte = 0;
		private int nextBit = 0;
		private ArrayList<Short> data;
		
		public BitReader(ArrayList<Short> d) {
			data = d;
		}
		
		public int nextBit() {
			if (nextByte >= data.size()) {
				return -1;
			}
			int bit = (data.get(nextByte) >> (7 - nextBit)) & 1;
			nextBit += 1;
			if (nextBit == 8) {
				nextBit = 0;
				nextByte += 1;
			}
			return bit;
		}
		
		public int nextBits(int length) {
			int bits = 0;
			for (int i = 0; i < length; ++i) {
				int bit = nextBit();
				if (bit == -1) {
					bits = -1;
					break;
				}
				else {
					bits = (bits << 1) | bit;
				}
			}
			return bits;
		}
		
		public boolean hasBits() {
			if (nextByte >= data.size()) {
				return false;
			}
			return true;
		}
		
		public void align() {
			if (nextByte >= data.size()) {
				return;
			}
			if (nextBit != 0) {
				nextBit = 0;
				nextByte += 1;
			}
		}
	}
	
	static Header header;
	static BitReader b;
	static ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes;
	static ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes;

	static short yDCTableID;
	static short yACTableID;
	static short cbDCTableID;
	static short cbACTableID;
	static short crDCTableID;
	static short crACTableID;

	static short previousYDC;
	static short previousCbDC;
	static short previousCrDC;
	
	static int skips;
	
	private static ArrayList<ArrayList<Integer>> generateCodes(HuffmanTable table){
		ArrayList<ArrayList<Integer>> codes = new ArrayList<ArrayList<Integer>>(16);
		for (int i = 0; i < 16; ++i) {
			codes.add(new ArrayList<Integer>());
		}
		
		int code = 0;
		for(int i = 0; i < 16; ++i){
			for(int j = 0; j < table.symbols.get(i).size(); ++j){
				codes.get(i).add(code);
				++code;
			}
			code <<= 1;
		}
		return codes;
	}
	
	private static boolean decodeMCUComponent(ArrayList<ArrayList<Integer>> DCTableCodes,
											  ArrayList<ArrayList<Integer>> ACTableCodes,
											  HuffmanTable DCTable,
											  HuffmanTable ACTable,
											  short[] component,
											  short previousDC,
											  Header header) {
		short large = (short) (1 << header.successiveApproximationLow);
		short small = (short) ((-1) << header.successiveApproximationLow);
		
		int k = header.startOfSelection;
		
		if (skips > 0 && (header.successiveApproximationHigh == 0 || k == 0)) {
			skips -= 1;
			return true;
		}
		
		// get the DC value for this MCU
		if (k == 0) {
			int currentCode = b.nextBit();
			if (header.successiveApproximationHigh != 0) {
				component[0] |= currentCode << header.successiveApproximationLow;
			}
			else {
				boolean found = false;
				for (int i = 0; i < 16; ++i) {
					for (int j = 0; j < DCTableCodes.get(i).size(); ++j) {
						if (currentCode == DCTableCodes.get(i).get(j)) {
							int length = DCTable.symbols.get(i).get(j);
							short coeff = (short)b.nextBits(length);
							if (coeff < (1 << (length - 1))) {
								coeff -= (1 << length) - 1;
							}
							if (header.frameType.equals("Progressive")) {
								component[0] |= coeff << header.successiveApproximationLow;
								//component[0] = (short) (coeff << header.successiveApproximationLow);
							}
							else {
								component[0] = coeff;
							}
							component[0] += previousDC; // ???
							//System.out.println("DC Value: " + component[0]);
							found = true;
							break;
						}
					}
					if (found) {
						break;
					}
					currentCode = (currentCode << 1) | b.nextBit();
				}
				if (!found ) {
					System.err.println("Error - Invalid DC Value");
					return false;
				}
			}
			k += 1;
		}
		
		boolean progressive = header.frameType.equals("Progressive");
		// get the AC values for this MCU
		boolean tripleBreak = false;
		for (; k <= header.endOfSelection; ++k) {
			boolean found = false;
			int currentCode = b.nextBit();
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < ACTableCodes.get(i).size(); ++j) {
					if (currentCode == ACTableCodes.get(i).get(j)) {
						short decoderValue = ACTable.symbols.get(i).get(j);
						if (!progressive && decoderValue == 0) {
							for (; k <= header.endOfSelection; ++k) {
								component[JPG.zigZagMap[k]] = 0;
							}
							return true; // not really necessary
						}
						else {
							short numZeroes = (short)((decoderValue & 0xF0) >> 4);
							short coeffLength = (short)(decoderValue & 0x0F);
							short coeff = 0;

							if (progressive && header.successiveApproximationHigh != 0) {
								if (coeffLength > 0) {
									if (coeffLength != 1) {
										System.err.println("Error - Refinement coeffLength not 1");
									}
									if (b.nextBit() == 1) {
										coeff = large;
									}
									else {
										coeff = small;
									}
								}
								else {
									if (numZeroes != 15) {
										skips = (1 << numZeroes) - 1;
										skips += b.nextBits(numZeroes);
										tripleBreak = true;
										break;
									}
								}
								for (; k <= header.endOfSelection; ++k) {
									if (component[JPG.zigZagMap[k]] != 0) {
										if (b.nextBit() == 1 && (component[JPG.zigZagMap[k]] & large) == 0) {
											if (component[JPG.zigZagMap[k]] > 0) {
												component[JPG.zigZagMap[k]] += large;
											}
											else {
												component[JPG.zigZagMap[k]] += small;
											}
										}
									}
									else {
										numZeroes -= 1;
										if (numZeroes < 0) {
											if (coeff != 0 && k < 64) {
												component[JPG.zigZagMap[k]] = coeff;
											}
											tripleBreak = true;
											break;
										}
									}
								}
							}
							else if (progressive && header.successiveApproximationHigh == 0 && coeffLength == 0) {
								if (numZeroes == 15) {
									k += 15;
								}
								else {
									skips = (1 << numZeroes) - 1;
									skips += b.nextBits(numZeroes);
									return true;
								}
							}
							else {
								for (int l = 0; l < numZeroes; ++l) {
									component[JPG.zigZagMap[k]] = 0;
									k += 1;
								}
								//k += numZeroes;
								if (coeffLength > 11){
									System.out.println("Error - coeffLength > 11");
								}
								
								if (coeffLength != 0) {
									coeff = (short)b.nextBits(coeffLength);
									
									if (coeff < (1 << (coeffLength - 1))) {
										coeff -= (1 << coeffLength) - 1;
									}
									if (header.frameType.equals("Progressive")) {
										component[JPG.zigZagMap[k]] |= coeff << header.successiveApproximationLow;
										//component[JPG.zigZagMap[k]] = (short) (coeff << header.successiveApproximationLow);
									}
									else {
										component[JPG.zigZagMap[k]] = coeff;
									}
								}
							}
						}
						found = true;
						break;
					}
				}
				if (tripleBreak) {
					break;
				}
				if (found) {
					break;
				}
				currentCode = (currentCode << 1) | b.nextBit();
			}
			if (tripleBreak) {
				break;
			}
			if (!found ) {
				System.err.println("Error - Invalid AC Value: " + currentCode);
				return false;
			}
		}
		
		if (skips > 0 && (header.successiveApproximationHigh != 0 && k > 0)) {
			for (; k <= header.endOfSelection; ++k) {
				if (component[JPG.zigZagMap[k]] != 0 && b.nextBit() == 1 && (component[JPG.zigZagMap[k]] & large) == 0) {
					if (component[JPG.zigZagMap[k]] > 0) {
						component[JPG.zigZagMap[k]] += large;
					}
					else {
						component[JPG.zigZagMap[k]] += small;
					}
				}
			}
			
			skips -= 1;
			return true;
		}
		
		return true;
	}
	
	public static boolean decodeHuffmanData(MCU mcu1, MCU mcu2, MCU mcu3, MCU mcu4) {
		if (!b.hasBits()) return false;
		
		int horizontal = header.colorComponents[0].horizontalSamplingFactor;
		int vertical = header.colorComponents[0].verticalSamplingFactor;
		
		ArrayList<ArrayList<Integer>> dcTableCodes;
		ArrayList<ArrayList<Integer>> acTableCodes;
		HuffmanTable dcTable;
		HuffmanTable acTable;
		boolean success;
		
		if (header.colorComponents[0].used) {
			//System.out.println("Decoding Y Component...");
			dcTableCodes = null;
			if (yDCTableID < DCTableCodes.size()) {
				dcTableCodes = DCTableCodes.get(yDCTableID);
			}
			acTableCodes = null;
			if (yACTableID < ACTableCodes.size()) {
				acTableCodes = ACTableCodes.get(yACTableID);
			}
			dcTable = header.huffmanDCTables[yDCTableID];
			acTable = header.huffmanACTables[yACTableID];
			success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.y, previousYDC, header);
			if (!success) {
				return false;
			}
			previousYDC = mcu1.y[0];
			
			if (horizontal == 2) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu2.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = mcu2.y[0];
			}
			if (vertical == 2) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu3.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = mcu3.y[0];
			}
			if (horizontal == 2 && vertical == 2) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu4.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = mcu4.y[0];
			}
		}

		if (header.numComponents > 1) {
			if (header.colorComponents[1].used) {
				//System.out.println("Decoding Cb Component...");
				dcTableCodes = null;
				if (cbDCTableID < DCTableCodes.size()) {
					dcTableCodes = DCTableCodes.get(cbDCTableID);
				}
				acTableCodes = null;
				if (cbACTableID < ACTableCodes.size()) {
					acTableCodes = ACTableCodes.get(cbACTableID);
				}
				dcTable = header.huffmanDCTables[cbDCTableID];
				acTable = header.huffmanACTables[cbACTableID];
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.cb, previousCbDC, header);
				if (!success) {
					return false;
				}
				previousCbDC = mcu1.cb[0];
			}

			if (header.colorComponents[2].used) {
				//System.out.println("Decoding Cr Component...");
				dcTableCodes = null;
				if (crDCTableID < DCTableCodes.size()) {
					dcTableCodes = DCTableCodes.get(crDCTableID);
				}
				acTableCodes = null;
				if (crACTableID < ACTableCodes.size()) {
					acTableCodes = ACTableCodes.get(crACTableID);
				}
				dcTable = header.huffmanDCTables[crDCTableID];
				acTable = header.huffmanACTables[crACTableID];
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.cr, previousCrDC, header);
				if (!success) {
					return false;
				}
				previousCrDC = mcu1.cr[0];
			}
		}

		return true;
	}
	
	public static void beginDecode(Header h) {
		header = h;
		b = new BitReader(header.imageData);
		
		DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		for (int i = 0; i < header.huffmanDCTables.length; ++i) {
			if (header.huffmanDCTables[i] != null) {
				DCTableCodes.add(generateCodes(header.huffmanDCTables[i]));
			}
			else {
				DCTableCodes.add(null);
			}
		}
		for (int i = 0; i < header.huffmanACTables.length; ++i) {
			if (header.huffmanACTables[i] != null) {
				ACTableCodes.add(generateCodes(header.huffmanACTables[i]));
			}
			else {
				ACTableCodes.add(null);
			}
		}

		yDCTableID  = header.colorComponents[0].huffmanDCTableID;
		yACTableID  = header.colorComponents[0].huffmanACTableID;
		if (header.numComponents > 1) {
			cbDCTableID = header.colorComponents[1].huffmanDCTableID;
			cbACTableID = header.colorComponents[1].huffmanACTableID;
			crDCTableID = header.colorComponents[2].huffmanDCTableID;
			crACTableID = header.colorComponents[2].huffmanACTableID;
		}

		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
		
		skips = 0;
	}
	
	public static void restart() {
		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
		b.align();
	}
}
