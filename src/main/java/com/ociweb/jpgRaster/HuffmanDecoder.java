//for some reason adding to the package is breaking this. 
package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;

import java.io.IOException;
import java.util.ArrayList;

public class HuffmanDecoder {
	private static class BitReader {
		private int nextByte = 0;
		private int nextBit = 0;
		private ArrayList<Short> data;
		
		public BitReader(ArrayList<Short> d) {
			data = d;
		}
		
		public Boolean done() {
			return (nextByte >= data.size());
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
	}
	
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
	
	public static void decodeMCUComponent(BitReader b,
										  ArrayList<ArrayList<Integer>> DCTableCodes,
										  ArrayList<ArrayList<Integer>> ACTableCodes,
										  HuffmanTable DCTable,
										  HuffmanTable ACTable,
										  short[] component,
										  short previousDC) {
		
		//get the DC value for this MCU
		int currentCode = b.nextBit();
		boolean found = false;
		for (int i = 0; i < 16; ++i) {
			for (int j = 0; j < DCTableCodes.get(i).size(); ++j) {
				if (currentCode == DCTableCodes.get(i).get(j)) {
					int length = DCTable.symbols.get(i).get(j);
					component[0] = (short)b.nextBits(length);
					if (component[0] < (1 << (length - 1))) {
						component[0] -= (1 << length) - 1;
					}
					component[0] += previousDC;
					System.out.println("DC Value: " + component[0]);
					found = true;
					break;
				}
			}
			if (found) {
				break;
			}
			currentCode = (currentCode << 1) | b.nextBit();
		}
		
		int[] map = new int[]  { 0,
				1, 8, 16, 15, 2, 3, 10, 17, 
				24, 32, 25, 18, 11, 4, 5,
				12, 19, 26, 33, 40, 48,
				41, 34, 27, 20, 13, 6,
				7, 14, 21, 28, 34, 42, 49, 56,
				57, 50, 43, 36, 29, 22, 15,
				23, 30, 37, 44, 51, 58,
				59, 52, 45, 38, 31, 39,
				46, 53, 60, 61, 54, 47, 55, 62, 63};
		
		//Get the AC values for this MCU
		for (int k = 1; k < 64; ++k) {
			found = false;
			currentCode = b.nextBit();
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < ACTableCodes.get(i).size(); ++j) {
					if (currentCode == ACTableCodes.get(i).get(j)) {
						short decoderValue = ACTable.symbols.get(i).get(j);
						System.out.println("Code -> Value : " + currentCode + " -> " + decoderValue);
						
						if (decoderValue == 0) {
							for (; k < 64; ++k) {
								component[map[k]] = 0;
							}
						}
						else {
							short numZeroes = (short)((decoderValue & 0xF0) >> 4);
							short coefLength = (short)(decoderValue & 0x0F);
							
							System.out.println("k: " + k);
							System.out.println("numZeroes: " + numZeroes);
							System.out.println("coefLength: " + coefLength);
							
							for (int l = 0; l < numZeroes; ++l){
								component[map[k]] = 0;
								++k;
							}
							if (coefLength > 11){
								System.out.println("Error - coeflength > 11");
							}
							
							if (coefLength != 0) {
								component[map[k]] = (short)b.nextBits(coefLength);
								
								
	
								if (component[map[k]] < (1 << (coefLength - 1))) {
									component[map[k]] -= (1 << coefLength) - 1;
								}
								System.out.println("AC Value: " + component[map[k]]);
//								++k; //We never needed to increment k here
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
		}
		
		System.out.print("Component Coefficients:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(component[i * 8 + j] + " ");
			}
		}
		System.out.println();
		
		return;
	}
	
	public static ArrayList<MCU> decodeHuffmanData(Header header) throws IOException {
		ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
			DCTableCodes.add(generateCodes(header.huffmanDCTables.get(i)));
		}
		for (int i = 0; i < header.huffmanACTables.size(); ++i) {
			ACTableCodes.add(generateCodes(header.huffmanACTables.get(i)));
		}
		
		for (int k = 0; k < DCTableCodes.size(); ++k) {
			for (int i = 0; i < DCTableCodes.get(k).size(); ++i) {
				System.out.print((i + 1) + ": ");
				for (int j = 0; j < DCTableCodes.get(k).get(i).size(); ++j) {
					System.out.print(String.format(("%" + (i + 1) + "s"), Integer.toBinaryString(DCTableCodes.get(k).get(i).get(j))).replace(' ', '0') + " ");
				}
				System.out.println();
			}
		}
		for (int k = 0; k < ACTableCodes.size(); ++k) {
			for (int i = 0; i < ACTableCodes.get(k).size(); ++i) {
				System.out.print((i + 1) + ": ");
				for (int j = 0; j < ACTableCodes.get(k).get(i).size(); ++j) {
					System.out.print(String.format(("%" + (i + 1) + "s"), Integer.toBinaryString(ACTableCodes.get(k).get(i).get(j))).replace(' ', '0') + " ");
				}
				System.out.println();
			}
		}
		
		int numMCUs = ((header.width + 7) / 8) * ((header.height + 7) / 8);
		System.out.println("Number of MCUs: " + numMCUs);
		BitReader b = new BitReader(header.imageData);
		ArrayList<MCU> out = new ArrayList<MCU>();

		short yDCTableID  = header.colorComponents.get(0).huffmanDCTableID;
		short yACTableID  = header.colorComponents.get(0).huffmanACTableID;
		short cbDCTableID = header.colorComponents.get(1).huffmanDCTableID;
		short cbACTableID = header.colorComponents.get(1).huffmanACTableID;
		short crDCTableID = header.colorComponents.get(2).huffmanDCTableID;
		short crACTableID = header.colorComponents.get(2).huffmanACTableID;

		short previousYDC = 0;
		short previousCbDC = 0;
		short previousCrDC = 0;
		while (out.size() != numMCUs) { // && !b.done()) {
			MCU mcu = new MCU();
			
			System.out.println("Decoding Y Component...");
			decodeMCUComponent(b, DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
					  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu.y, previousYDC);
			System.out.println("Decoding Cb Component...");
			decodeMCUComponent(b, DCTableCodes.get(cbDCTableID), ACTableCodes.get(cbACTableID),
					  header.huffmanDCTables.get(cbDCTableID), header.huffmanACTables.get(cbACTableID), mcu.cb, previousCbDC);
			System.out.println("Decoding Cr Component...");
			decodeMCUComponent(b, DCTableCodes.get(crDCTableID), ACTableCodes.get(crACTableID),
					  header.huffmanDCTables.get(crDCTableID), header.huffmanACTables.get(crACTableID), mcu.cr, previousCrDC);
			
			previousYDC = mcu.y[0];
			previousCbDC = mcu.cb[0];
			previousCrDC = mcu.cr[0];
			
			out.add(mcu);
		}
		
		return out;
	}
	
	public static void main(String[] args) throws Exception {
		// test BitReader
		/*ArrayList<Short> data = new ArrayList<Short>();
		data.add((short)5);
		data.add((short)10);
		data.add((short)15);
		
		BitReader b = new BitReader(data);
		for (int i = 0; i < 25; ++i) {
			System.out.println(b.nextBit());
		}
		
		System.out.println();*/
		
		// test generateCodes
		/*HuffmanTable table = new HuffmanTable();
		table.tableID = 0;
		table.ACTable = false;
		for (int i = 0; i < 16; ++i) {
			table.symbols.add(new ArrayList<Short>());
		}
		table.symbols.get(1).add((short)0);
		table.symbols.get(2).add((short)1);
		table.symbols.get(2).add((short)2);
		table.symbols.get(2).add((short)3);
		table.symbols.get(2).add((short)4);
		table.symbols.get(2).add((short)5);
		table.symbols.get(3).add((short)6);
		table.symbols.get(4).add((short)7);
		table.symbols.get(5).add((short)8);
		table.symbols.get(6).add((short)9);
		table.symbols.get(7).add((short)10);
		table.symbols.get(8).add((short)11);
		for (int i = 0; i < table.symbols.size(); ++i) {
			System.out.print((i + 1) + ": ");
			for (int j = 0; j < table.symbols.get(i).size(); ++j) {
				System.out.print(table.symbols.get(i).get(j) + " ");
			}
			System.out.println();
		}
		System.out.println();
		ArrayList<ArrayList<Short>> codes = generateCodes(table);
		for (int i = 0; i < codes.size(); ++i) {
			System.out.print((i + 1) + ": ");
			for (int j = 0; j < codes.get(i).size(); ++j) {
				System.out.print(codes.get(i).get(j) + " ");
			}
			System.out.println();
		}*/
		
		// test decodeHuffmanData
		Header header = JPGScanner.ReadJPG("huff_simple0.jpg");
		System.out.println("Decoding Huffman data...");
		ArrayList<MCU> mcus = decodeHuffmanData(header);
		for (int i = 0; i < mcus.size(); ++i) {
			System.out.print("Y: ");
			for (int j = 0; j  < mcus.get(i).y.length; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcus.get(i).y[j] + " ");
			}
			System.out.println();
			System.out.print("Cb: ");
			for (int j = 0; j  < mcus.get(i).cb.length; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcus.get(i).cb[j] + " ");
			}
			System.out.println();
			System.out.print("Cr: ");
			for (int j = 0; j  < mcus.get(i).cr.length; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcus.get(i).cr[j] + " ");
			}
			System.out.println();
		}
	}
}
