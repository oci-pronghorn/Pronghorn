package com.ociweb.jpgRaster;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class JPGScanner {
	public static class HuffmanTable {
		short tableID;
		Boolean ACTable;
		ArrayList<ArrayList<Integer>> symbols = new ArrayList<ArrayList<Integer>>();
	}
	
	public static class QuantizationTable {
		short tableID;
		short precision;
		ArrayList<Integer> table = new ArrayList<Integer>();
	}
	
	public static class FrameComponent {
		short componentID;
		short horizontalSamplingFactor;
		short verticalSamplingFactor;
		short quantizationTableID;
	}
	
	public static class ScanComponent {
		short componentID;
		short huffmanACTableID;
		short huffmanDCTableID;
	}
	
	public static class Header {
		// APP0
		String identifier;
		short versionMajor;
		short versionMinor;
		short densityUnits;
		int xDensity;
		int yDensity;
		short xThumbnail;
		short yThumbnail;
		short[] thumbnailData;
		
		// DRI
		int restartInterval;
		
		// DHT
		ArrayList<HuffmanTable> huffmanTables = new ArrayList<HuffmanTable>();
		
		// DQT
		ArrayList<QuantizationTable> quantizationTables = new ArrayList<QuantizationTable>();
		
		// SOF
		short precision;
		int height;
		int width;
		ArrayList<FrameComponent> frameComponents = new ArrayList<FrameComponent>();
		
		// SOS
		short startOfSelection;
		short endOfSelection;
		short successvieApproximation;
		ArrayList<ScanComponent> scanComponents = new ArrayList<ScanComponent>();
		ArrayList<Integer> imageData = new ArrayList<Integer>();
		
		Boolean valid = true;
	}
	
	public static Header ReadJPG(String filename) throws IOException {
		Header header = new Header();
		FileInputStream f = new FileInputStream(filename);
        
		short last = (short)f.read();
		short current = (short)f.read();
		if (last != 0xFF || current != 0xD8) {
			header.valid = false;
			f.close();
			return header;
		}
		System.out.println("Start of Image");
        last = (short)f.read();
        current = (short)f.read();
		
        while (true) {            
            if      (last == (short)0xFF && current == (short)0xE0) {
            	ReadAPP0(f, header);
            }
            else if (last == (short)0xFF &&
            		current >= (short)0xE1 && current <= (short)0xEF) {
            	ReadAPPN(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xDB) {
            	ReadQuantizationTable(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xC0) {
            	ReadStartOfFrame_Baseline(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xC2) {
            	ReadStartOfFrame_Progressive(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xC4) {
            	ReadHuffmanTable(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xDA) {
            	ReadStartOfScan(f, header);
            	break;
            }
            else if (last == (short)0xFF && current == (short)0xDD) {
            	ReadRestartInterval(f, header);
            }
            else if (last == (short)0xFF &&
                	current >= (short)0xD0 && current <= (short)0xD7) {
                ReadRSTN(f, header);
            }
            else if (last == (short)0xFF && current == (short)0xFE) {
            	ReadComment(f, header);
            }

            else if (last == (short)0x0FF) {
            	if (current == (short)0xD8) {
            		System.err.println("This JPG contains an embedded JPG. This is not supported");
            		header.valid = false;
            		f.close();
            		return header;
            	}
            	else if (current == (short)0xD9) {
            		System.err.println("Error = EOI detected before SOS");
            		header.valid = false;
            		f.close();
            		return header;
            	}
            	else {
	            	System.out.println("Warning - Unknown Maker: " + current);
	            	ReadAPPN(f, header);
	            	//header.valid = false;
            	}
            }
            else { //if (last != (short)0xFF) {
            	System.err.println("Error - Expected a marker");
            	header.valid = false;
        		f.close();
        		return header;
            }
            
            last = (short)f.read();
            current = (short)f.read();
        }
        current = (short)f.read();
        while (true) {
        	last = current;
        	current = (short)f.read();
        	if      (last == (short)0xFF && current == (short)0xD9) {
            	System.out.println("End of Image");
            	break;
            }
        	else if (last == (short)0xFF && current == (short)0x00) {
        		header.imageData.add((int)last);
        		// advance by a byte, to drop 0x00
        		current = (short)f.read();
        	}
        	/*else if (last == (short)0xFF) {
        		System.err.println("Invalid marker during compressed data scan: " + current);
        		header.valid = false;
        		f.close();
        		return header;
        	}*/
        	else {
        		header.imageData.add((int)last);
        	}
        }
        f.close();
		return header;
	}
	
	public static void ReadAPP0(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading APP0");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.identifier = ReadString(f);
		
		header.versionMajor = (short)f.read();
		header.versionMinor = (short)f.read();
		header.densityUnits = (short)f.read();
		header.xDensity = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		header.yDensity = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		header.xThumbnail = (short)f.read();
		header.yThumbnail = (short)f.read();
		header.thumbnailData = new short[header.xThumbnail * header.yThumbnail * 3];
		for (int i = 0; i < header.thumbnailData.length; ++i) {
			header.thumbnailData[i] = (short)f.read();
		}
		if (length - 11 - (header.identifier.length() + 1) - header.thumbnailData.length != 0) {
			System.err.println("Error - APP0 Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadAPPN(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading APPN");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		// all of APPN markers can be ignored
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadQuantizationTable(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Quantization Tables");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			short info = (short)f.read();
			QuantizationTable table = new QuantizationTable();
			table.tableID = (short)(info & 0x0F);
			if ((info & 0xF0) == 0) {
				table.precision = 1;
			}
			else {
				table.precision = 2;
			}
			for (int i = 0; i < 64 * table.precision; i++) {
				table.table.add(f.read());
			}
			header.quantizationTables.add(table);
			length -= 64 * table.precision + 1;
		}
		if (length != 0) {
			System.err.println("Error - DQT Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadStartOfFrame_Baseline(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Frame (Baseline)");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.precision = (short)f.read();
		header.height = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		header.width = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		int numComponents = f.read();
		for (int i = 0; i < numComponents; i++) {
			FrameComponent component = new FrameComponent();
			component.componentID = (short)f.read();
			short samplingFactor = (short)f.read();
			component.horizontalSamplingFactor = (short)((samplingFactor & 0xF0) >> 4);
			component.verticalSamplingFactor = (short)(samplingFactor & 0x0F);
			component.quantizationTableID = (short)f.read();
			header.frameComponents.add(component);
		}
		if (length - 8 - (numComponents * 3) != 0) {
			System.err.println("Error - SOF0 Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadStartOfFrame_Progressive(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Frame (Progressive)");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.precision = (short)f.read();
		header.height = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		header.width = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		int numComponents = f.read();
		for (int i = 0; i < numComponents; i++) {
			FrameComponent component = new FrameComponent();
			component.componentID = (short)f.read();
			short samplingFactor = (short)f.read();
			component.horizontalSamplingFactor = (short)((samplingFactor & 0xF0) >> 4);
			component.verticalSamplingFactor = (short)(samplingFactor & 0x0F);
			component.quantizationTableID = (short)f.read();
			header.frameComponents.add(component);
		}
		if (length - 8 - (numComponents * 3) != 0) {
			System.err.println("Error - SOF2 Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadHuffmanTable(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Huffman Tables");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			HuffmanTable table = new HuffmanTable();
			short info = (short)f.read();
			table.tableID = (short)(info & 0x0F);
			table.ACTable = (info & 0xF0) != 0;
			int allSymbols = 0;
			short[] numSymbols = new short[16];
			for (int i = 0; i < 16; i++) {
				numSymbols[i] = (short)f.read();
				allSymbols += numSymbols[i];
				table.symbols.add(new ArrayList<Integer>());
			}
			for (int i = 0; i < 16; i++) {
				for (int j = 0; j < numSymbols[i]; j++) {
					table.symbols.get(i).add(f.read());
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
	
	public static void ReadStartOfScan(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Scan");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		int numComponents = (int)f.read();
		for (int i = 0; i < numComponents; i++) {
			ScanComponent component = new ScanComponent();
			component.componentID = (short)f.read();
			short huffmanTableID = (short)f.read();
			component.huffmanACTableID = (short)(huffmanTableID & 0x0F);
			component.huffmanDCTableID = (short)((huffmanTableID & 0xF0) >> 4);
			header.scanComponents.add(component);
		}
		header.startOfSelection = (short)f.read();
		header.endOfSelection = (short)f.read();
		header.successvieApproximation = (short)f.read();
		if (length - 6 - (numComponents * 2) != 0) {
			System.err.println("Error - SOS Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadRestartInterval(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Restart Interval");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.restartInterval = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		if (length - 4 != 0) {
			System.err.println("Error - DRI Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadRSTN(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading RSTN");
		// RSTN has no length
	}
	
	public static void ReadComment(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Comment");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		// all comment markers can be ignored
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	// read ASCII characters from f until a null terminator is reached
	public static String ReadString(FileInputStream f) throws IOException {
		String s = new String("");
		char curChar = (char)f.read();
		while (curChar != '\0') {
			s = s + curChar;
			curChar = (char)f.read();
		}
		return s;
	}
	
	public static void main(String[] args) {
		Header header = null;
		try {
			header = ReadJPG("Simple.jpg");
			if (header != null && header.valid) {
				System.out.println("APP0===========");
				System.out.println("Identifier: " + header.identifier);
				System.out.println("Version: " + header.versionMajor + "." + header.versionMinor);
				System.out.println("Density Units: " + header.densityUnits);
				System.out.println("X Density: " + header.xDensity);
				System.out.println("Y Density: " + header.yDensity);
				System.out.println("X Thumbnail: " + header.xThumbnail);
				System.out.println("Y Thumbnail: " + header.yThumbnail);
				System.out.println("Thumbnail Data:");
				if (header.thumbnailData != null) {
					for (int i = 0; i < header.thumbnailData.length; i++) {
						if (i % 16 == 0) {
							System.out.println();
						}
						System.out.print(header.thumbnailData[i] + " ");
					}
				}
				System.out.println("DQT============");
				for (int i = 0; i < header.quantizationTables.size(); i++) {
					System.out.println("Table ID: " + header.quantizationTables.get(i).tableID);
					System.out.println("Precision: " + header.quantizationTables.get(i).precision);
					System.out.print("Table Data:");
					for (int j = 0; j < header.quantizationTables.get(i).table.size(); j++) {
						if (j % 16 == 0) {
							System.out.println();
						}
						System.out.print(header.quantizationTables.get(i).table.get(j) + " ");
					}
					System.out.println();
				}
				System.out.println("SOF============");
				System.out.println("Precision: " + header.precision);
				System.out.println("Height: " + header.height);
				System.out.println("Width: " + header.width);
				System.out.println("Frame Components:");
				for (int i = 0; i < header.frameComponents.size(); i++) {
					System.out.println("\tComponent ID: " + header.frameComponents.get(i).componentID);
					System.out.println("\tHorizontal Sampling Factor: " + header.frameComponents.get(i).horizontalSamplingFactor);
					System.out.println("\tVertical Sampling Factor: " + header.frameComponents.get(i).verticalSamplingFactor);
					System.out.println("\tQuantization Table ID: " + header.frameComponents.get(i).quantizationTableID);
				}
				System.out.println("DHT============");
				for (int i = 0; i < header.huffmanTables.size(); i++) {
					System.out.println("Table ID: " + header.huffmanTables.get(i).tableID);
					System.out.print("Table Type: ");
					if (header.huffmanTables.get(i).ACTable) {
						System.out.println("AC");
					}
					else {
						System.out.println("DC");
					}
					System.out.println("Symbols:");
					for (int j = 0; j < header.huffmanTables.get(i).symbols.size(); j++) {
						System.out.print((j + 1) + ": ");
						for (int k = 0; k < header.huffmanTables.get(i).symbols.get(j).size(); k++) {
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
				for (int i = 0; i < header.scanComponents.size(); i++) {
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
		} catch(IOException e) {
			System.err.println("Error - Unknown error reading JPG file");
		}
	}
}
