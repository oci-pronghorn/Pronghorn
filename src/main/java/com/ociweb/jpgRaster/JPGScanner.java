package com.ociweb.jpgRaster;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class JPGScanner {
	public static class Header {
		String identifier;
		short versionMajor;
		short versionMinor;
		short densityUnits;
		int xDensity;
		int yDensity;
		short xThumbnail;
		short yThumbnail;
		short[] thumbnailData;
		//short[] huffmanTables;
		//short[] quantizationTables;
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
		
        while (current != -1) {            
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
            /*else if (last == (short)0xFF && current == (short)0xD9) {
            	System.out.println("End of Image");
            	break;
            }*/

            else if (last == (short)0x0FF) {
            	System.out.println("Unknown Maker: " + current);
            	ReadAPPN(f, header);
            	header.valid = false;
            }
            else { //if (last != (short)0xFF) {
            	System.out.println("Warning - Not a marker");
            	header.valid = false;
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
		length -= 11;
		header.identifier = ReadString(f);
		length -= header.identifier.length() + 1;
		
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
		length -= header.thumbnailData.length;
		if (length != 0) {
			System.err.println("Warning - APP0 Invalid");
			header.valid = false;
		}
	}
	
	public static void ReadAPPN(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading APPN");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadQuantizationTable(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Quantization Tables");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadStartOfFrame_Baseline(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Frame (Baseline)");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadStartOfFrame_Progressive(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Frame (Progressive)");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadHuffmanTable(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Huffman Tables");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadStartOfScan(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Start of Scan");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		/*for (int i = 0; i < length - 2; i++) {
			f.read();
		}*/
		int numComponents = (int)f.read();
		for (int i = 0; i < numComponents; i++) {
			f.read();
			f.read();
		}
		f.read();
		f.read();
		f.read();
	}
	
	public static void ReadRestartInterval(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Restart Interval");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadRSTN(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading RSTN");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
	public static void ReadComment(FileInputStream f, Header header) throws IOException {
		System.out.println("Reading Comment");
		int length = ((int)f.read() << 8) + ((int)f.read() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		for (int i = 0; i < length - 2; i++) {
			f.read();
		}
	}
	
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
				System.out.println("Identifier: " + header.identifier);
				System.out.println("Version: " + header.versionMajor + "." + header.versionMinor);
				System.out.println("Density Units: " + header.densityUnits);
				System.out.println("X Density: " + header.xDensity);
				System.out.println("Y Density: " + header.yDensity);
				System.out.println("X Thumbnail: " + header.xThumbnail);
				System.out.println("Y Thumbnail: " + header.yThumbnail);
				System.out.println("Thumbnail Data:");
				for (int i = 0; i < header.thumbnailData.length; i++) {
					if (i % 16 == 0) {
						System.out.println();
					}
					System.out.print(header.thumbnailData[i] + " ");
				}
		        System.out.println("Length of Image Data: " + header.imageData.size());
			}
			else {
				System.out.println("Not a valid JPG file.");
			}
		} catch(FileNotFoundException e) {
			System.err.println("JPG file not found.");
		} catch(IOException e) {
			System.err.println("Error reading JPG file.");
		}
	}
}
