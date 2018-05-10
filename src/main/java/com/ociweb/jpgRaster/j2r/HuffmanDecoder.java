package com.ociweb.jpgRaster.j2r;

import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HuffmanDecoder {

	private static final Logger logger = LoggerFactory.getLogger(HuffmanDecoder.class);
	
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
	
	Header header;
	boolean progressive;
	BitReader b;
	ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes;
	ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes;

	short yDCTableID;
	short yACTableID;
	short cbDCTableID;
	short cbACTableID;
	short crDCTableID;
	short crACTableID;

	short previousYDC;
	short previousCbDC;
	short previousCrDC;
	
	int skips;
	
	public static ArrayList<ArrayList<Integer>> generateCodes(HuffmanTable table){
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
	
	private short getNextSymbol(ArrayList<ArrayList<Integer>> codes,
									   ArrayList<ArrayList<Short>> symbols) {
		int currentCode = b.nextBit();
		for (int i = 0; i < 16; ++i) {
			for (int j = 0; j < codes.get(i).size(); ++j) {
				if (currentCode == codes.get(i).get(j)) {
					return symbols.get(i).get(j);
				}
			}
			currentCode = (currentCode << 1) | b.nextBit();
		}
		return -1;
	}
	
	private boolean decodeMCUComponent(ArrayList<ArrayList<Integer>> DCTableCodes,
											  ArrayList<ArrayList<Integer>> ACTableCodes,
											  HuffmanTable DCTable,
											  HuffmanTable ACTable,
											  short[] component,
											  short previousDC,
											  Header header) {
		if (progressive) {
			int componentsInUse = 0;
			if (header.colorComponents[0].used) ++componentsInUse;
			if (header.numComponents > 1) {
				if (header.colorComponents[1].used) ++componentsInUse;
				if (header.colorComponents[2].used) ++componentsInUse;
			}
			
			if (header.startOfSelection > header.endOfSelection) {
				logger.error("Error - Bad spectral selection (start greater than end)");
			}
			if (header.endOfSelection > 63) {
				logger.error("Error - Bad spectral selection (end greater than 63)");
			}
			if (header.startOfSelection == 0 && header.endOfSelection != 0) {
				logger.error("Error - Bad spectral selection (contains DC and AC)");
			}
			if (header.startOfSelection != 0 && componentsInUse != 1) {
				logger.error("Error - Bad spectral selection (AC scan contains multiple components)");
			}
			if (header.successiveApproximationHigh != 0 &&
				header.successiveApproximationLow != header.successiveApproximationHigh - 1) {
				logger.error("Error - Bad successive approximation");
			}
			
			if (header.startOfSelection == 0 && header.successiveApproximationHigh == 0) {
				// DC first visit
				short length = getNextSymbol(DCTableCodes, DCTable.symbols);
				if (length == -1) {
					logger.error("Error - Invalid DC Value");
					return false;
				}
				short coeff = (short)b.nextBits(length);
				if (coeff == -1) return true;
				if (length != 0 && coeff < (1 << (length - 1))) {
					coeff -= (1 << length) - 1;
				}
				coeff += previousDC;
				component[0] = (short) (coeff << header.successiveApproximationLow);
			}
			else if (header.startOfSelection == 0 && header.successiveApproximationHigh != 0) {
				// DC refinement
				component[0] |= b.nextBit() << header.successiveApproximationLow;
			}
			else if (header.startOfSelection != 0 && header.successiveApproximationHigh == 0) {
				// AC first visit
				if (skips > 0) {
					--skips;
					return true;
				}
				for (int k = header.startOfSelection; k <= header.endOfSelection; ++k) {
					short symbol = getNextSymbol(ACTableCodes, ACTable.symbols);

					short numZeroes = (short)((symbol & 0xF0) >> 4);
					short coeffLength = (short)(symbol & 0x0F);

					if (coeffLength != 0) {
						for (int l = 0; l < numZeroes && k <= 63; ++l, ++k) {
							component[JPG.zigZagMap[k]] = 0;
						}
						if (!b.hasBits()) {
							return true;
						}
						if (coeffLength > 11) {
							System.out.println("Error - coeffLength > 11");
							// try proceeding anyway
						}
					
						if (k == 64) {
							logger.error("Error - Zero run-length exceeded MCU");
							return false;
						}
						
						short coeff = (short)b.nextBits(coeffLength);
						if (coeff == -1) return true;
						if (coeff < (1 << (coeffLength - 1))) {
							coeff -= (1 << coeffLength) - 1;
						}
						component[JPG.zigZagMap[k]] = (short) (coeff << header.successiveApproximationLow);
					}
					else {
						if (numZeroes == 15) {
							for (int l = 0; l < numZeroes && k <= 63; ++l, ++k) {
								component[JPG.zigZagMap[k]] = 0;
							}
							if (k == 64) {
								logger.error("Error - Zero run-length exceeded MCU");
								return false;
							}
						}
						else {
							skips = (1 << numZeroes) - 1;
							skips += b.nextBits(numZeroes);
							break;
						}
					}
				}
			}
			else if (header.startOfSelection != 0 && header.successiveApproximationHigh != 0) {
				// AC refinement
				final short large = (short) (1 << header.successiveApproximationLow);
				final short small = (short) ((-1) << header.successiveApproximationLow);
				int k = header.startOfSelection;
				if (skips == 0) {
					for (; k <= header.endOfSelection; ++k) {
						short symbol = getNextSymbol(ACTableCodes, ACTable.symbols);
						if (symbol == -1) {
							logger.error("Error - Invalid AC Value");
							return false;
						}
						short numZeroes = (short)((symbol & 0xF0) >> 4);
						short coeffLength = (short)(symbol & 0x0F);
						short coeff = 0;
						
						if (coeffLength != 0) {
							if (coeffLength != 1) {
								logger.error("Error - Invalid AC Value");
								return false;
							}
							switch(b.nextBit()) {
							case 1:
								coeff = large;
								break;
							case 0:
								coeff = small;
								break;
							default: // -1, data stream is empty
								return true;
							}
						}
						else {
							if (numZeroes != 15) {
								skips = 1 << numZeroes;
								skips += b.nextBits(numZeroes);
								break;
							}
						}
						
						do {
							if (component[JPG.zigZagMap[k]] != 0) {
								switch(b.nextBit()) {
								case 1:
									if ((component[JPG.zigZagMap[k]] & large) == 0) {
										if (component[JPG.zigZagMap[k]] >= 0) {
											component[JPG.zigZagMap[k]] += large;
										}
										else {
											component[JPG.zigZagMap[k]] += small;
										}
									}
									break;
								case 0:
									// do nothing
									break;
								default: // -1, data stream is empty
									return true;
								}
							}
							else {
								--numZeroes;
								if (numZeroes < 0) {
									break;
								}
							}
							
							++k;
						} while (k <= header.endOfSelection);
						
						if (k < 64) {
							component[JPG.zigZagMap[k]] = coeff;
						}
					}
				}
			}
		}
		else {
			// baseline decoding
			// get the DC value for this MCU
			short length = getNextSymbol(DCTableCodes, DCTable.symbols);
			if (length == -1) {
				logger.error("Error - Invalid DC Value");
				return false;
			}
			short coeff = (short)b.nextBits(length);
			if (coeff == -1) return true;
			if (length != 0 && coeff < (1 << (length - 1))) {
				coeff -= (1 << length) - 1;
			}
			component[0] = coeff;
			component[0] += previousDC;
			//System.out.println("DC Value: " + component[0]);
			
			// get the AC values for this MCU
			for (int k = 1; k <= 63; ++k) {
				short symbol = getNextSymbol(ACTableCodes, ACTable.symbols);
				if (symbol == -1) {
					logger.error("Error - Invalid AC Value");
					return false;
				}
				
				if (symbol == 0) {
					for (; k <= 63; ++k) {
						component[JPG.zigZagMap[k]] = 0;
					}
					return true; // not really necessary
				}
				else {
					short numZeroes = (short)((symbol & 0xF0) >> 4);
					short coeffLength = (short)(symbol & 0x0F);
					coeff = 0;

					for (int l = 0; l < numZeroes && k <= 63; ++l, ++k) {
						component[JPG.zigZagMap[k]] = 0;
					}
					if (!b.hasBits()) {
						return true;
					}
					if (coeffLength > 11) {
						System.out.println("Error - coeffLength > 11");
						// try proceeding anyway
					}
					
					if (coeffLength != 0) {
						if (k == 64) {
							logger.error("Error - Zero run-length exceeded MCU");
							return false;
						}
						
						coeff = (short)b.nextBits(coeffLength);
						if (coeff == -1) return true;
						if (coeff < (1 << (coeffLength - 1))) {
							coeff -= (1 << coeffLength) - 1;
						}
						component[JPG.zigZagMap[k]] = coeff;
					}
				}
			}
		}
		return true;
	}
	
	public boolean decodeHuffmanData(MCU mcu1, MCU mcu2, MCU mcu3, MCU mcu4) {
		if (!b.hasBits()) return true;
		
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
			previousYDC = (short) (mcu1.y[0] >> header.successiveApproximationLow);
			
			if (horizontal == 2 && (header.colorComponents[1].used || header.colorComponents[2].used)) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu2.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = (short) (mcu2.y[0] >> header.successiveApproximationLow);
			}
			if (vertical == 2 && (header.colorComponents[1].used || header.colorComponents[2].used)) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu3.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = (short) (mcu3.y[0] >> header.successiveApproximationLow);
			}
			if (horizontal == 2 && vertical == 2 && (header.colorComponents[1].used || header.colorComponents[2].used)) {
				success = decodeMCUComponent(dcTableCodes, acTableCodes, dcTable, acTable, mcu4.y, previousYDC, header);
				if (!success) {
					return false;
				}
				previousYDC = (short) (mcu4.y[0] >> header.successiveApproximationLow);
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
				previousCbDC = (short) (mcu1.cb[0] >> header.successiveApproximationLow);
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
				previousCrDC = (short) (mcu1.cr[0] >> header.successiveApproximationLow);
			}
		}

		return true;
	}
	
	public void beginDecode(Header h) {
		header = h;
		progressive = header.frameType.equals("Progressive");
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
	
	public void restart() {
		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
		b.align();
	}
}
