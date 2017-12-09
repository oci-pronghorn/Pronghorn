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
	
	public static ArrayList<MCU> decodeFourTables(Header header) throws IOException {
		ArrayList<ArrayList<Integer>> table1Codes = generateCodes(header.huffmanTables.get(0));
		ArrayList<ArrayList<Integer>> table2Codes = generateCodes(header.huffmanTables.get(1));
		ArrayList<ArrayList<Integer>> table3Codes = generateCodes(header.huffmanTables.get(2));
		ArrayList<ArrayList<Integer>> table4Codes = generateCodes(header.huffmanTables.get(3));

		BitReader b = new BitReader(header.imageData);
		int currentCode = b.nextBit();
		ArrayList<MCU> out = new ArrayList<MCU>();
		// ByteArrayOutputStream out = new ByteArrayOutputStream();

		while (!b.done()) {
			System.out.println("Entering decode...");
			MCU newMCU = new MCU();
			
			//Y ======================================================================
			
			//get the Y DC value for this MCU
			boolean found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j <= table1Codes.get(i).size(); ++j) {
					if (currentCode == table1Codes.get(i).get(j)) {
						int length = header.huffmanTables.get(0).symbols.get(i).get(j);
						newMCU.yDc = (int)b.nextBits(length);
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
			}
			
			//Get the Y AC values for this MCU
			boolean acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = (0 & b.nextBit());
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j <= table2Codes.get(i).size(); ++j) {
						if (currentCode == table2Codes.get(i).get(j)) {
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
					}
				}
			}
			
			
			//CB=========================================================================
			
			currentCode = (0 & b.nextBit());
			//get the cb DC value for this MCU
			found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j <= table3Codes.get(i).size(); ++j) {
					if (currentCode == table3Codes.get(i).get(j)) {
						int length = header.huffmanTables.get(2).symbols.get(i).get(j);
						newMCU.cbDc = (int)b.nextBits(length);
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
			}
			
			//Get the cb AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = (0 & b.nextBit());
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j <= table4Codes.get(i).size(); ++j) {
						if (currentCode == table4Codes.get(i).get(j)) {
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
					}
				}
			}
			
			
			//CR ===========================================================================
			
			currentCode = (0 & b.nextBit());
			//get the cr DC value for this MCU
			found = false;
			for (int i = 0; i < 16 && !found; ++i) {
				for (int j = 0; j <= table3Codes.get(i).size(); ++j) {
					if (currentCode == table3Codes.get(i).get(j)) {
						int length = header.huffmanTables.get(2).symbols.get(i).get(j);
						newMCU.cbDc = (int)b.nextBits(length);
						found = true;
						break;
					}
				}
				currentCode = (currentCode << 1) & b.nextBit();
			}
			
			//Get the cr AC values for this MCU
			acFinish = false;
			found = false;
			for (int k = 0; k < 64 && !acFinish; ++k) {
				currentCode = (0 & b.nextBit());
				for (int i = 0; i < 16 && !found; ++i) {
					for (int j = 0; j <= table4Codes.get(i).size(); ++j) {
						if (currentCode == table4Codes.get(i).get(j)) {
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
					}
				}
			}
			System.out.println("Adding MCU...");
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
		
		// test decodeFourTables
		Header header = JPGScanner.ReadJPG("Simple.jpg");
		ArrayList<MCU> mcus = decodeFourTables(header);
		for (int i = 0; i < mcus.size(); ++i) {
			System.out.println(mcus.get(i));
		}
	}
}
