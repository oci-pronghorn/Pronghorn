package com.ociweb.jpgRaster;

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
		int k = header.startOfSelection;
		
		// get the DC value for this MCU
		if (k == 0) {
			boolean found = false;
			int currentCode = b.nextBit();
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < DCTableCodes.get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(i).get(j)) {
						int length = DCTable.symbols.get(i).get(j);
						short coeff = (short)b.nextBits(length);
						if (coeff < (1 << (length - 1))) {
							coeff -= (1 << length) - 1;
						}
						if (header.frameType.equals("Progressive")) {
							//component[0] |= coeff << header.successiveApproximationLow;
							component[0] = (short) (coeff << header.successiveApproximationLow);
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
			k += 1;
		}
		
		// get the AC values for this MCU
		for (; k <= header.endOfSelection; ++k) {
			boolean found = false;
			int currentCode = b.nextBit();
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < ACTableCodes.get(i).size(); ++j) {
					if (currentCode == ACTableCodes.get(i).get(j)) {
						short decoderValue = ACTable.symbols.get(i).get(j);
						if (decoderValue == 0) {
							for (; k <= header.endOfSelection; ++k) {
								component[JPG.zigZagMap[k]] = 0;
							}
						}
						else {
							short numZeroes = (short)((decoderValue & 0xF0) >> 4);
							short coeffLength = (short)(decoderValue & 0x0F);
							
							for (int l = 0; l < numZeroes; ++l){
								component[JPG.zigZagMap[k]] = 0;
								k += 1;
							}
							if (coeffLength > 11){
								System.out.println("Error - coeffLength > 11");
							}
							
							if (coeffLength != 0) {
								short coeff = (short)b.nextBits(coeffLength);
								
								if (coeff < (1 << (coeffLength - 1))) {
									coeff -= (1 << coeffLength) - 1;
								}
								if (header.frameType.equals("Progressive")) {
									//component[JPG.zigZagMap[k]] |= coeff << header.successiveApproximationLow;
									component[JPG.zigZagMap[k]] = (short) (coeff << header.successiveApproximationLow);
								}
								else {
									component[JPG.zigZagMap[k]] = coeff;
								}
							}
						}
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
				System.err.println("Error - Invalid AC Value");
				return false;
			}
		}
		
		return true;
	}
	
	public static boolean decodeHuffmanData(MCU mcu1, MCU mcu2, MCU mcu3, MCU mcu4) {
		if (!b.hasBits()) return false;
		
		int horizontal = header.colorComponents.get(0).horizontalSamplingFactor;
		int vertical = header.colorComponents.get(0).verticalSamplingFactor;
		
		//System.out.println("Decoding Y Component...");
		ArrayList<ArrayList<Integer>> dcTableCodes = null;
		if (yDCTableID < DCTableCodes.size()) {
			dcTableCodes = DCTableCodes.get(yDCTableID);
		}
		ArrayList<ArrayList<Integer>> acTableCodes = null;
		if (yACTableID < ACTableCodes.size()) {
			acTableCodes = ACTableCodes.get(yACTableID);
		}
		HuffmanTable dcTable = null;
		if (yDCTableID < header.huffmanDCTables.size()) {
			dcTable = header.huffmanDCTables.get(yDCTableID);
		}
		HuffmanTable acTable = null;
		if (yACTableID < header.huffmanACTables.size()) {
			acTable = header.huffmanACTables.get(yACTableID);
		}
		boolean success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.y, previousYDC, header);
		if (!success) {
			return false;
		}
		previousYDC = mcu1.y[0];
		
		if (horizontal == 2) {
			success = decodeMCUComponent(DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
					  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu2.y, previousYDC, header);
			if (!success) {
				return false;
			}
			previousYDC = mcu2.y[0];
		}
		if (vertical == 2) {
			success = decodeMCUComponent(DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
					  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu3.y, previousYDC, header);
			if (!success) {
				return false;
			}
			previousYDC = mcu3.y[0];
		}
		if (horizontal == 2 && vertical == 2) {
			success = decodeMCUComponent(DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
					  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu4.y, previousYDC, header);
			if (!success) {
				return false;
			}
			previousYDC = mcu4.y[0];
		}

		if (header.colorComponents.size() > 1) {
			//System.out.println("Decoding Cb Component...");
			dcTableCodes = null;
			if (cbDCTableID < DCTableCodes.size()) {
				dcTableCodes = DCTableCodes.get(cbDCTableID);
			}
			acTableCodes = null;
			if (cbACTableID < ACTableCodes.size()) {
				acTableCodes = ACTableCodes.get(cbACTableID);
			}
			dcTable = null;
			if (cbDCTableID < header.huffmanDCTables.size()) {
				dcTable = header.huffmanDCTables.get(cbDCTableID);
			}
			acTable = null;
			if (cbACTableID < header.huffmanACTables.size()) {
				acTable = header.huffmanACTables.get(cbACTableID);
			}
			success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.cb, previousCbDC, header);
			if (!success) {
				return false;
			}
			previousCbDC = mcu1.cb[0];

			//System.out.println("Decoding Cr Component...");
			dcTableCodes = null;
			if (crDCTableID < DCTableCodes.size()) {
				dcTableCodes = DCTableCodes.get(crDCTableID);
			}
			acTableCodes = null;
			if (crACTableID < ACTableCodes.size()) {
				acTableCodes = ACTableCodes.get(crACTableID);
			}
			dcTable = null;
			if (crDCTableID < header.huffmanDCTables.size()) {
				dcTable = header.huffmanDCTables.get(crDCTableID);
			}
			acTable = null;
			if (crACTableID < header.huffmanACTables.size()) {
				acTable = header.huffmanACTables.get(crACTableID);
			}
			success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu1.cr, previousCrDC, header);
			if (!success) {
				return false;
			}
			previousCrDC = mcu1.cr[0];
		}

		return true;
	}
	
	public static void beginDecode(Header h) {
		header = h;
		b = new BitReader(header.imageData);
		
		DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
			DCTableCodes.add(generateCodes(header.huffmanDCTables.get(i)));
		}
		for (int i = 0; i < header.huffmanACTables.size(); ++i) {
			ACTableCodes.add(generateCodes(header.huffmanACTables.get(i)));
		}

		yDCTableID  = header.colorComponents.get(0).huffmanDCTableID;
		yACTableID  = header.colorComponents.get(0).huffmanACTableID;
		if (header.colorComponents.size() > 1) {
			cbDCTableID = header.colorComponents.get(1).huffmanDCTableID;
			cbACTableID = header.colorComponents.get(1).huffmanACTableID;
			crDCTableID = header.colorComponents.get(2).huffmanDCTableID;
			crACTableID = header.colorComponents.get(2).huffmanACTableID;
		}

		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
	}
	
	public static void restart() {
		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
		b.align();
	}
}
