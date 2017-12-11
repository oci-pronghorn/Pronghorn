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
	
	public static ArrayList<MCU> decodeHuffmanData(Header header) throws IOException {
		ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
			DCTableCodes.add(generateCodes(header.huffmanDCTables.get(i)));
		}
		for (int i = 0; i < header.huffmanACTables.size(); ++i) {
			ACTableCodes.add(generateCodes(header.huffmanACTables.get(i)));
		}
		
		/*for (int k = 0; k < DCTableCodes.size(); ++k) {
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
		}*/
		
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
		while (out.size() != numMCUs && !b.done()) {
			MCU newMCU = new MCU();
			
			//Y ======================================================================
			
			//get the Y DC value for this MCU
			int currentCode = b.nextBit();
			boolean found = false;
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < DCTableCodes.get(yDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(yDCTableID).get(i).get(j)) {
						int length = header.huffmanDCTables.get(yDCTableID).symbols.get(i).get(j);
						newMCU.y[0] = (short)b.nextBits(length);
						if (newMCU.y[0] < (1 << (length - 1))) {
							newMCU.y[0] -= (1 << length) - 1;
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
			
			//Get the Y AC values for this MCU
			boolean acFinish = false;
			found = false;
			for (int k = 1; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				for (int i = 0; i < 16; ++i) {
					for (int j = 0; j < ACTableCodes.get(yACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(yACTableID).get(i).get(j)) {
							newMCU.y[k] = header.huffmanACTables.get(yACTableID).symbols.get(i).get(j);
							if (header.huffmanACTables.get(yACTableID).symbols.get(i).get(j) == 0) {
								for (; k < 64; ++k) {
									newMCU.y[k] = 0;
								}
								acFinish = true;
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
			
			
			//CB=========================================================================
			
			currentCode = b.nextBit();
			//get the cb DC value for this MCU
			found = false;
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < DCTableCodes.get(cbDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(cbDCTableID).get(i).get(j)) {
						int length = header.huffmanDCTables.get(cbDCTableID).symbols.get(i).get(j);
						newMCU.cb[0] = (short)b.nextBits(length);
						if (newMCU.cb[0] < (1 << (length - 1))) {
							newMCU.cb[0] -= (1 << length) - 1;
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
			
			//Get the cb AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 1; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				for (int i = 0; i < 16; ++i) {
					for (int j = 0; j < ACTableCodes.get(cbACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(cbACTableID).get(i).get(j)) {
							newMCU.cb[k] = header.huffmanACTables.get(cbACTableID).symbols.get(i).get(j);
							if (header.huffmanACTables.get(cbACTableID).symbols.get(i).get(j) == 0) {
								for(; k < 64; ++k) {
									newMCU.cb[k] = 0;
								}
								acFinish = true;
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
			
			
			//CR ===========================================================================
			
			currentCode = b.nextBit();
			//get the cr DC value for this MCU
			found = false;
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < DCTableCodes.get(crDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(crDCTableID).get(i).get(j)) {
						int length = header.huffmanDCTables.get(crDCTableID).symbols.get(i).get(j);
						newMCU.cr[0] = (short)b.nextBits(length);
						if (newMCU.cr[0] < (1 << (length - 1))) {
							newMCU.cr[0] -= (1 << length) - 1;
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
			
			//Get the cr AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 1; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				for (int i = 0; i < 16; ++i) {
					for (int j = 0; j < ACTableCodes.get(crACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(crACTableID).get(i).get(j)) {
							newMCU.cr[k] = header.huffmanACTables.get(crACTableID).symbols.get(i).get(j);
							if (header.huffmanACTables.get(crACTableID).symbols.get(i).get(j) == 0) {
								for (; k < 64; ++k) {
									newMCU.cr[k] = 0;
								}
								acFinish = true;
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
			
			out.add(newMCU);
		} //end while
		
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
