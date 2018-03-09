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
						
						component[0] = (short)b.nextBits(length);
						if (component[0] < (1 << (length - 1))) {
							component[0] -= (1 << length) - 1;
						}
						component[0] += previousDC;
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
							for (; k < 64; ++k) {
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
								component[JPG.zigZagMap[k]] = (short)b.nextBits(coeffLength);
								
								if (component[JPG.zigZagMap[k]] < (1 << (coeffLength - 1))) {
									component[JPG.zigZagMap[k]] -= (1 << coeffLength) - 1;
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
		boolean success = decodeMCUComponent(DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
				  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu1.y, previousYDC, header);
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
			
			success = decodeMCUComponent(DCTableCodes.get(cbDCTableID), ACTableCodes.get(cbACTableID),
					header.huffmanDCTables.get(cbDCTableID), header.huffmanACTables.get(cbACTableID), mcu1.cb, previousCbDC, header);
			if (!success) {
				return false;
			}
			previousCbDC = mcu1.cb[0];

			//System.out.println("Decoding Cr Component...");
			success = decodeMCUComponent(DCTableCodes.get(crDCTableID), ACTableCodes.get(crACTableID),
					header.huffmanDCTables.get(crDCTableID), header.huffmanACTables.get(crACTableID), mcu1.cr, previousCrDC, header);
			if (!success) {
				return false;
			}
			previousCrDC = mcu1.cr[0];
		}

		return true;
	}
	
	public static void beginDecode(Header h, MCU mcu) {
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
		
		for (int i = 0; i < 64; ++i) {
			mcu.y[i] = 0;
			mcu.cb[i] = 0;
			mcu.cr[i] = 0;
		}
	}
	
	public static void restart() {
		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
		b.align();
	}
}
