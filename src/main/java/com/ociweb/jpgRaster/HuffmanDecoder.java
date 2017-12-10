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
			int bit = (data.get(nextByte) >> nextBit) & 1;
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
		for (int i = 0; i < header.huffmanTables.size(); ++i) {
			if (header.huffmanTables.get(i).ACTable) {
				ACTableCodes.add(generateCodes(header.huffmanTables.get(i)));
			}
			else {
				DCTableCodes.add(generateCodes(header.huffmanTables.get(i)));
			}
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
		// ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		int readBits = 0;
		while (out.size() != numMCUs && !b.done()) {
			MCU newMCU = new MCU();
			readBits += 1;
			
			//Y ======================================================================
			
			//get the Y DC value for this MCU
			int currentCode = b.nextBit();
			boolean found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j < DCTableCodes.get(header.colorComponents.get(0).huffmanDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(header.colorComponents.get(0).huffmanDCTableID).get(i).get(j)) {
						int length = header.huffmanTables.get(0).symbols.get(i).get(j);
						newMCU.yDc = b.nextBits(length);
						readBits += length;
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
				readBits += 1;
			}
			
			//Get the Y AC values for this MCU
			boolean acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				readBits += 1;
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j < ACTableCodes.get(header.colorComponents.get(0).huffmanACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(header.colorComponents.get(0).huffmanACTableID).get(i).get(j)) {
							newMCU.yAc[k] = header.huffmanTables.get(1).symbols.get(i).get(j);
							if (header.huffmanTables.get(1).symbols.get(i).get(j) == 0) {
								for (; k < 64; ++k) {
									newMCU.yAc[k] = 0;
								}
								acFinish = true;
							}
							found = true;
							break;
						}
						currentCode = (currentCode << 1) & b.nextBit();
						readBits += 1;
					}
				}
			}
			
			
			//CB=========================================================================
			
			currentCode = b.nextBit();
			readBits += 1;
			//get the cb DC value for this MCU
			found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j < DCTableCodes.get(header.colorComponents.get(1).huffmanDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(header.colorComponents.get(1).huffmanDCTableID).get(i).get(j)) {
						int length = header.huffmanTables.get(2).symbols.get(i).get(j);
						newMCU.cbDc = b.nextBits(length);
						readBits += length;
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
				readBits += 1;
			}
			
			//Get the cb AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				readBits += 1;
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j < ACTableCodes.get(header.colorComponents.get(1).huffmanACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(header.colorComponents.get(1).huffmanACTableID).get(i).get(j)) {
							newMCU.cbAc[k] = header.huffmanTables.get(3).symbols.get(i).get(j);
							if (header.huffmanTables.get(3).symbols.get(i).get(j) == 0) {
								for(; k < 64; ++k) {
									newMCU.cbAc[k] = 0;
								}
								acFinish = true;
							}
							found = true;
							break;
						}
						currentCode = (currentCode << 1) & b.nextBit();
						readBits += 1;
					}
				}
			}
			
			
			//CR ===========================================================================
			
			currentCode = b.nextBit();
			readBits += 1;
			//get the cr DC value for this MCU
			found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j < DCTableCodes.get(header.colorComponents.get(2).huffmanDCTableID).get(i).size(); ++j) {
					if (currentCode == DCTableCodes.get(header.colorComponents.get(2).huffmanDCTableID).get(i).get(j)) {
						int length = header.huffmanTables.get(2).symbols.get(i).get(j);
						newMCU.cbDc = b.nextBits(length);
						readBits += length;
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
				readBits += 1;
			}
			
			//Get the cr AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = b.nextBit();
				readBits += 1;
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j < ACTableCodes.get(header.colorComponents.get(2).huffmanACTableID).get(i).size(); ++j) {
						if (currentCode == ACTableCodes.get(header.colorComponents.get(2).huffmanACTableID).get(i).get(j)) {
							newMCU.crAc[k] = header.huffmanTables.get(3).symbols.get(i).get(j);
							if (header.huffmanTables.get(3).symbols.get(i).get(j) == 0) {
								for (; k < 64; ++k) {
									newMCU.crAc[k] = 0;
								}
								acFinish = true;
							}
							found = true;
							break;
						}
						currentCode = (currentCode << 1) & b.nextBit();
						readBits += 1;
					}
				}
			}
			out.add(newMCU);
			System.out.println("Read bits: " + readBits);
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
			System.out.println("yDC: " + mcus.get(i).yDc);
			System.out.println("cbDC: " + mcus.get(i).cbDc);
			System.out.println("crDC: " + mcus.get(i).crDc);
			System.out.print("yAC: ");
			for (int j = 0; j  < mcus.get(i).yAc.length; ++j) {
				System.out.print(mcus.get(i).yAc[j] + " ");
			}
			System.out.println();
			System.out.print("cbAC: ");
			for (int j = 0; j  < mcus.get(i).cbAc.length; ++j) {
				System.out.print(mcus.get(i).cbAc[j] + " ");
			}
			System.out.println();
			System.out.print("crAC: ");
			for (int j = 0; j  < mcus.get(i).crAc.length; ++j) {
				System.out.print(mcus.get(i).crAc[j] + " ");
			}
			System.out.println();
		}
	}
}
