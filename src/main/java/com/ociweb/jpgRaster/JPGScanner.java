package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.FrameComponent;
import com.ociweb.jpgRaster.JPG.ScanComponent;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.util.ArrayList;

public class JPGScanner {
	public static Header ReadJPG(String filename) throws IOException {
		Header header = new Header();
		DataInputStream f = new DataInputStream(new FileInputStream(filename));
		
		// JPG file must begin with 0xFFD8
		short last = (short)f.readUnsignedByte();
		short current = (short)f.readUnsignedByte();
		if (last != 0xFF || current != 0xD8) {
			header.valid = false;
			f.close();
			return header;
		}
		System.out.println("Start of Image");
		last = (short)f.readUnsignedByte();
		current = (short)f.readUnsignedByte();
		
		while (true) {
			if (last == 0xFF) {
				if      (current == 0xDB) {
					ReadQuantizationTable(f, header);
				}
				else if (current == 0xC0) {
					header.frameType = "Baseline";
					ReadStartOfFrame(f, header);
				}
				else if (current == 0xC1) {
					header.frameType = "Extended Sequential";
					ReadStartOfFrame(f, header);
				}
				else if (current == 0xC2) {
					header.frameType = "Progressive";
					ReadStartOfFrame(f, header);
				}
				else if (current == 0xC3) {
					header.frameType = "Lossless";
					ReadStartOfFrame(f, header);
				}
				else if (current == 0xC4) {
					ReadHuffmanTable(f, header);
				}
				else if (current == 0xDA) {
					ReadStartOfScan(f, header);
					break;
				}
				else if (current == 0xDD) {
					ReadRestartInterval(f, header);
				}
				else if (current >= 0xD0 && current <= 0xD7) {
					ReadRSTN(f, header);
				}
				else if (current >= 0xE0 && current <= 0xEF) {
					ReadAPPN(f, header);
				}
				else if (current == 0xFE) {
					ReadComment(f, header);
				}
				else if (current == 0xFF) {
					// skip
					current = (short)f.readUnsignedByte();
					continue;
				}
				else if (current == 0xF0 ||
						 current == 0xFD ||
						 current == 0xDC ||
						 current == 0xDE ||
						 current == 0xDF) {
					// unsupported segments that can be skipped
					ReadComment(f, header);
				}
				else if (current == 0x01) {
					// unsupported segment with no size
				}
				else if (current == 0xD8) {
					System.err.println("Error - This JPG contains an embedded JPG; This is not supported");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current == 0xD9) {
					System.err.println("Error = EOI detected before SOS");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current == 0xCC) {
					System.err.println("Error - Arithmetic Table mode is not supported");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current >= 0xC0 && current <= 0xCF) {
					System.err.println("Error - This Start of Frame marker is not supported: " + current);
					header.valid = false;
					f.close();
					return header;
				}
				else {
					System.out.println("Error - Unknown Maker: " + current);
					header.valid = false;
					f.close();
					return header;
				}
			}
			else { //if (last != 0xFF) {
				System.err.println("Error - Expected a marker");
				header.valid = false;
				f.close();
				return header;
			}
			
			last = (short)f.readUnsignedByte();
			current = (short)f.readUnsignedByte();
		}
		current = (short)f.readUnsignedByte();
		while (true) {
			last = current;
			current = (short)f.readUnsignedByte();
			if      (last == 0xFF && current == 0xD9) {
				System.out.println("End of Image");
				break;
			}
			else if (last == 0xFF && current == 0x00) {
				header.imageData.add(last);
				// advance by a byte, to drop 0x00
				current = (short)f.readUnsignedByte();
			}
			/*else if (last == 0xFF) {
				System.err.println("Invalid marker during compressed data scan: " + current);
				header.valid = false;
				f.close();
				return header;
			}*/
			else {
				header.imageData.add(last);
			}
		}
		f.close();
		return header;
	}
	
	private static void ReadQuantizationTable(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Quantization Tables");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			short info = (short)f.readUnsignedByte();
			QuantizationTable table = new QuantizationTable();
			table.tableID = (short)(info & 0x0F);
			if ((info & 0xF0) == 0) {
				table.precision = 1;
			}
			else {
				table.precision = 2;
			}
			for (int i = 0; i < 64; ++i) {
				table.table[i] = f.readUnsignedByte();
				if (table.precision == 2) {
					table.table[i] = table.table[i] << 8 + f.readUnsignedByte();
				}
			}
			header.quantizationTables.add(table);
			length -= 64 * table.precision + 1;
		}
		if (length != 0) {
			System.err.println("Error - DQT Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadStartOfFrame(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Frame");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		header.precision = (short)f.readUnsignedByte();
		header.height = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		header.width = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		int numComponents = f.readUnsignedByte();
		for (int i = 0; i < numComponents; ++i) {
			FrameComponent component = new FrameComponent();
			component.componentID = (short)f.readUnsignedByte();
			short samplingFactor = (short)f.readUnsignedByte();
			component.horizontalSamplingFactor = (short)((samplingFactor & 0xF0) >> 4);
			component.verticalSamplingFactor = (short)(samplingFactor & 0x0F);
			component.quantizationTableID = (short)f.readUnsignedByte();
			header.frameComponents.add(component);
		}
		if (length - 8 - (numComponents * 3) != 0) {
			System.err.println("Error - SOF Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadHuffmanTable(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Huffman Tables");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			HuffmanTable table = new HuffmanTable();
			short info = (short)f.readUnsignedByte();
			table.tableID = (short)(info & 0x0F);
			table.ACTable = (info & 0xF0) != 0;
			int allSymbols = 0;
			short[] numSymbols = new short[16];
			for (int i = 0; i < 16; ++i) {
				numSymbols[i] = (short)f.readUnsignedByte();
				allSymbols += numSymbols[i];
			}
			for (int i = 0; i < 16; ++i) {
				table.symbols.add(new ArrayList<Short>());
				for (int j = 0; j < numSymbols[i]; ++j) {
					table.symbols.get(i).add((short)f.readUnsignedByte());
				}
			}
			header.huffmanTables.add(table);
			length -= allSymbols + 17;
		}
		if (length != 0) {
			System.err.println("Error - DHT Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadStartOfScan(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Scan");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		int numComponents = f.readUnsignedByte();
		for (int i = 0; i < numComponents; ++i) {
			ScanComponent component = new ScanComponent();
			component.componentID = (short)f.readUnsignedByte();
			short huffmanTableID = (short)f.readUnsignedByte();
			component.huffmanACTableID = (short)(huffmanTableID & 0x0F);
			component.huffmanDCTableID = (short)((huffmanTableID & 0xF0) >> 4);
			header.scanComponents.add(component);
		}
		header.startOfSelection = (short)f.readUnsignedByte();
		header.endOfSelection = (short)f.readUnsignedByte();
		header.successvieApproximation = (short)f.readUnsignedByte();
		if (length - 6 - (numComponents * 2) != 0) {
			System.err.println("Error - SOS Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadRestartInterval(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Restart Interval");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		//int restartInterval = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		f.readUnsignedByte();
		f.readUnsignedByte();
		if (length - 4 != 0) {
			System.err.println("Error - DRI Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadRSTN(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading RSTN");
		// RSTN has no length
	}
	
	private static void ReadAPPN(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading APPN");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		// all of APPN markers can be ignored
		for (int i = 0; i < length - 2; ++i) {
			f.readUnsignedByte();
		}
	}
	
	private static void ReadComment(DataInputStream f, Header header) throws IOException {
		System.out.println("Reading Comment");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		// all comment markers can be ignored
		for (int i = 0; i < length - 2; ++i) {
			f.readUnsignedByte();
		}
	}
	
	public static void main(String[] args) {
		Header header = null;
		try {
			header = ReadJPG("Simple.jpg");
			if (header != null && header.valid) {
				System.out.println("DQT============");
				for (int i = 0; i < header.quantizationTables.size(); ++i) {
					System.out.println("Table ID: " + header.quantizationTables.get(i).tableID);
					System.out.println("Precision: " + header.quantizationTables.get(i).precision);
					System.out.print("Table Data:");
					for (int j = 0; j < header.quantizationTables.get(i).table.length; ++j) {
						if (j % 8 == 0) {
							System.out.println();
						}
						System.out.print(String.format("%02d ", header.quantizationTables.get(i).table[j]));
					}
					System.out.println();
				}
				System.out.println("SOF============");
				System.out.println("Frame Type: " + header.frameType);
				System.out.println("Precision: " + header.precision);
				System.out.println("Height: " + header.height);
				System.out.println("Width: " + header.width);
				System.out.println("Frame Components:");
				for (int i = 0; i < header.frameComponents.size(); ++i) {
					System.out.println("\tComponent ID: " + header.frameComponents.get(i).componentID);
					System.out.println("\tHorizontal Sampling Factor: " + header.frameComponents.get(i).horizontalSamplingFactor);
					System.out.println("\tVertical Sampling Factor: " + header.frameComponents.get(i).verticalSamplingFactor);
					System.out.println("\tQuantization Table ID: " + header.frameComponents.get(i).quantizationTableID);
				}
				System.out.println("DHT============");
				for (int i = 0; i < header.huffmanTables.size(); ++i) {
					System.out.println("Table ID: " + header.huffmanTables.get(i).tableID);
					System.out.print("Table Type: ");
					if (header.huffmanTables.get(i).ACTable) {
						System.out.println("AC");
					}
					else {
						System.out.println("DC");
					}
					System.out.println("Symbols:");
					for (int j = 0; j < header.huffmanTables.get(i).symbols.size(); ++j) {
						System.out.print((j + 1) + ": ");
						for (int k = 0; k < header.huffmanTables.get(i).symbols.get(j).size(); ++k) {
							System.out.print(header.huffmanTables.get(i).symbols.get(j).get(k));
							if (k < header.huffmanTables.get(i).symbols.get(j).size() - 1) {
								System.out.print(", ");
							}
						}
						System.out.println();
					}
				}
				System.out.println("SOS============");
				System.out.println("Start of Selection: " + header.startOfSelection);
				System.out.println("End of Selection: " + header.endOfSelection);
				System.out.println("Successive Approximation: " + header.successvieApproximation);
				System.out.println("Scan Components:");
				for (int i = 0; i < header.scanComponents.size(); ++i) {
					System.out.println("\tComponent ID: " + header.scanComponents.get(i).componentID);
					System.out.println("\tHuffman AC Table ID: " + header.scanComponents.get(i).huffmanACTableID);
					System.out.println("\tHuffman DC Table ID: " + header.scanComponents.get(i).huffmanDCTableID);
				}
				System.out.println("Length of Image Data: " + header.imageData.size());
			}
			else {
				System.err.println("Error - Not a valid JPG file");
			}
		} catch(FileNotFoundException e) {
			System.err.println("Error - JPG file not found");
		} catch (EOFException e) {
			System.err.println("Error - File ended early");
		} catch(IOException e) {
			System.err.println("Error - Unknown error reading JPG file");
		}
	}
}
