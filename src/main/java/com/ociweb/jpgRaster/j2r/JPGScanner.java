package com.ociweb.jpgRaster.j2r;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPGConstants;
import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class JPGScanner extends PronghornStage {

	private ArrayList<String> inputFiles = new ArrayList<String>();
	private final Pipe<JPGSchema> output;
	boolean verbose;
	public static long timer = 0;
	
	int mcuWidth = 0;
	int mcuHeight = 0;
	int numMCUs = 0;
	int numProcessed = 0;
	int aboutToSend = 0;
	
	Header header;
	MCU mcu1 = new MCU();
	MCU mcu2 = new MCU();
	MCU mcu3 = new MCU();
	MCU mcu4 = new MCU();
	ArrayList<MCU> mcus = null;
	
	public JPGScanner(GraphManager graphManager, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, NONE, output);
		this.output = output;
		this.verbose = verbose;
	}
	
	public Header ReadJPG(String filename, ArrayList<MCU> mcus) throws IOException {
		Header header = new Header();
		header.filename = filename;
		
		FileInputStream f = new FileInputStream(filename);
		FileChannel file = f.getChannel();
		
		int numBytes = (int)(new File(filename)).length();
		ByteBuffer b = ByteBuffer.allocate(numBytes);
		int bytesRead = 0;
		
		while(bytesRead < numBytes) {
			bytesRead += file.read(b);
		}
		
		b.flip();
		
		
		// JPG file must begin with 0xFFD8
		
		short last = (short)(b.get() & 0xFF);
		short current = (short)(b.get() & 0xFF);
		
		if (last != 0xFF || current != JPGConstants.SOI) {
			header.valid = false;
			f.close();
			b.clear();
			return header;
		}
		if (verbose) 
			System.out.println("Start of Image");
		last = (short)(b.get() & 0xFF);
		current = (short)(b.get() & 0xFF);
		
		while (header.valid) {
			if (last != 0xFF) {
				System.err.println("Error - Expected a marker");
				header.valid = false;
				f.close();
				return header;
			}
			switch (current) {
			case JPGConstants.DQT:
				ReadQuantizationTable(b, header);
				break;
			case JPGConstants.SOF0:
				header.frameType = "Baseline";
				ReadStartOfFrame(b, header);
				break;
			case JPGConstants.SOF1:
				header.frameType = "Extended Sequential";
				ReadStartOfFrame(b, header);
				break;
			case JPGConstants.SOF2:
				header.frameType = "Progressive";
				ReadStartOfFrame(b, header);
				break;
			case JPGConstants.SOF3:
				header.frameType = "Lossless";
				ReadStartOfFrame(b, header);
				break;
			case JPGConstants.DHT:
				ReadHuffmanTable(b, header);
				break;
			case JPGConstants.SOS:
				ReadStartOfScan(b, header);
				// break out of while loop
				break;
			case JPGConstants.DRI:
				ReadRestartInterval(b, header);
				break;
			case JPGConstants.APP0:
			case JPGConstants.APP1:
			case JPGConstants.APP2:
			case JPGConstants.APP3:
			case JPGConstants.APP4:
			case JPGConstants.APP5:
			case JPGConstants.APP6:
			case JPGConstants.APP7:
			case JPGConstants.APP8:
			case JPGConstants.APP9:
			case JPGConstants.APP10:
			case JPGConstants.APP11:
			case JPGConstants.APP12:
			case JPGConstants.APP13:
			case JPGConstants.APP14:
			case JPGConstants.APP15:
				ReadAPPN(b, header);
				break;
			case JPGConstants.COM:
				ReadComment(b, header);
				break;
			case 0xFF: // skip
				current = (short)(b.get() & 0xFF);
				break;
			case JPGConstants.JPG0:
			case JPGConstants.JPG1:
			case JPGConstants.JPG2:
			case JPGConstants.JPG3:
			case JPGConstants.JPG4:
			case JPGConstants.JPG5:
			case JPGConstants.JPG6:
			case JPGConstants.JPG7:
			case JPGConstants.JPG8:
			case JPGConstants.JPG9:
			case JPGConstants.JPG10:
			case JPGConstants.JPG11:
			case JPGConstants.JPG12:
			case JPGConstants.JPG13:
			case JPGConstants.DNL:
			case JPGConstants.DHP:
			case JPGConstants.EXP:
				// unsupported segments that can be skipped
				ReadComment(b, header);
				break;
			case JPGConstants.TEM:
				// unsupported segment with no size
				break;
			case JPGConstants.SOI:
				System.err.println("Error - This JPG contains an embedded JPG; THis is not supported");
				header.valid = false;
				f.close();
				return header;
			case JPGConstants.EOI:
				System.err.println("Error - EOI detected before SOS");
				header.valid = false;
				f.close();
				return header;
			case JPGConstants.DAC:
				System.err.println("Error - Arithmetic Table mode is not supported");
				header.valid = false;
				f.close();
				return header;
				// case JPGConstants.SOF4:
			case JPGConstants.SOF5:
			case JPGConstants.SOF6:
			case JPGConstants.SOF7:
				// case JPGConstants.SOF8:
			case JPGConstants.SOF9:
			case JPGConstants.SOF10:
			case JPGConstants.SOF11:
				// case JPGConstants.SOF12:
			case JPGConstants.SOF13:
			case JPGConstants.SOF14:
			case JPGConstants.SOF15:
				System.err.println("Error - This Start of Frame marker is not supported: " + String.format("0x%2x", current));
				header.valid = false;
				f.close();
				b.clear();
				return header;
			case JPGConstants.RST0:
			case JPGConstants.RST1:
			case JPGConstants.RST2:
			case JPGConstants.RST3:
			case JPGConstants.RST4:
			case JPGConstants.RST5:
			case JPGConstants.RST6:
			case JPGConstants.RST7:
				System.err.println("Error - RSTN detected before SOS");
				header.valid = false;
				f.close();
				b.clear();
				return header;
			default:
				System.err.println("Error - Unknown Marker: " + String.format("0x%2x", current));
				header.valid = false;
				f.close();
				b.clear();
				return header;
			}
			if (current == JPGConstants.SOS) {
				// break out of while loop
				break;
			}

			last = (short)(b.get() & 0xFF);
			current = (short)(b.get() & 0xFF);
		}
		if (header.valid) {
			if (header.frameType.equals("Progressive")) {
				int numScans = 0;
				current = (short)(b.get() & 0xFF);
				while (true) {
					last = current;
					current = (short)(b.get() & 0xFF);
					if (last == 0xFF) {
						if      (current == JPGConstants.EOI) {
							if (verbose) 
								System.out.println("End of Image");
							break;
						}
						else if (current == 0x00) {
							header.imageData.add(last);
							// advance by a byte, to drop 0x00
							current = (short)(b.get() & 0xFF);
						}
						else if (current == JPGConstants.DHT) {
							if (header.imageData.size() > 0) {
								if (!decodeScan(header, mcus, numScans)) {
									f.close();
									return header;
								}
								numScans += 1;
							}
							
							ReadHuffmanTable(b, header);
							current = (short)(b.get() & 0xFF);
						}
						else if (current == JPGConstants.SOS) {
							if (header.imageData.size() > 0) {
								if (!decodeScan(header, mcus, numScans)) {
									f.close();
									return header;
								}
								numScans += 1;
							}
							
							ReadStartOfScan(b, header);
							current = (short)(b.get() & 0xFF);
						}
						else if (current >= JPGConstants.RST0 && current <= JPGConstants.RST7) {
							ReadRSTN(b, header);
							current = (short)(b.get() & 0xFF);
						}
						else if (current != 0xFF) {
							System.err.println("Error - Invalid marker during compressed data scan: " + String.format("0x%2x", current));
							header.valid = false;
							f.close();
							return header;
						}
					}
					else {
						header.imageData.add(last);
					}
				}
			}
			else { // if (header.frameType.equals("Baseline")) {
				current = (short)(b.get() & 0xFF);
				while (true) {
					last = current;
					current = (short)(b.get() & 0xFF);
					if (last == 0xFF) {
						if      (current == JPGConstants.EOI) {
							if (verbose) 
								System.out.println("End of Image");
							break;
						}
						else if (current == 0x00) {
							header.imageData.add(last);
							// advance by a byte, to drop 0x00
							current = (short)(b.get() & 0xFF);
						}
						else if (current >= JPGConstants.RST0 && current <= JPGConstants.RST7) {
							ReadRSTN(b, header);
							current = (short)(b.get() & 0xFF);
						}
						else if (current != 0xFF) {
							System.err.println("Error - Invalid marker during compressed data scan: " + String.format("0x%2x", current));
							header.valid = false;
							f.close();
							b.clear();
							return header;
						}
					}
					else {
						header.imageData.add(last);
					}
				}
			}
		}
		f.close();
		
		if (header.numComponents != 1 && header.numComponents != 3) {
			System.err.println("Error - " + header.numComponents + " color components given (1 or 3 required)");
			header.valid = false;
			return header;
		}
		
		if (header.numComponents > 0 &&
			(header.colorComponents[0].horizontalSamplingFactor > 2 ||
			 header.colorComponents[0].verticalSamplingFactor > 2)) {
			System.err.println("Error - Unsupported Sampling Factor");
			header.valid = false;
		}
		if (header.numComponents > 1 &&
			(header.colorComponents[1].horizontalSamplingFactor != 1 ||
			 header.colorComponents[1].verticalSamplingFactor != 1)) {
			System.err.println("Error - Unsupported Sampling Factor");
			header.valid = false;
		}
		if (header.numComponents > 2 &&
			(header.colorComponents[2].horizontalSamplingFactor != 1 ||
			 header.colorComponents[2].verticalSamplingFactor != 1)) {
			System.err.println("Error - Unsupported Sampling Factor");
			header.valid = false;
		}
		
		return header;
	}
	
	// decode a whole scan, progressive images only
	private boolean decodeScan(Header header, ArrayList<MCU> mcus, int numScans) {
		// decode scan so far
		if (verbose) 
			System.out.println("Decoding a scan of size " + header.imageData.size());
		HuffmanDecoder.beginDecode(header);

		MCU mcu1 = null;
		MCU mcu2 = null;
		MCU mcu3 = null;
		MCU mcu4 = null;
		int horizontal = header.colorComponents[0].horizontalSamplingFactor;
		int vertical = header.colorComponents[0].verticalSamplingFactor;
		int numMCUs = ((header.width + 7) / 8) * ((header.height + 7) / 8);
		int numProcessed = 0;
		while (numProcessed < numMCUs) {
			if (mcus.size() < numMCUs) {
				if (horizontal == 1 && vertical == 1) {
					mcu1 = new MCU();
				}
				else if (horizontal == 2 && vertical == 1) {
					mcu1 = new MCU();
					mcu2 = new MCU();
				}
				else if (horizontal == 1 && vertical == 2) {
					mcu1 = new MCU();
					mcu2 = new MCU();
				}
				else if (horizontal == 2 && vertical == 2) {
					mcu1 = new MCU();
					mcu2 = new MCU();
					mcu3 = new MCU();
					mcu4 = new MCU();
				}
			}
			else {
				if (horizontal == 1 && vertical == 1) {
					mcu1 = mcus.get(numProcessed);
				}
				else if (horizontal == 2 && vertical == 1) {
					mcu1 = mcus.get(numProcessed);
					mcu2 = mcus.get(numProcessed + 1);
				}
				else if (horizontal == 1 && vertical == 2) {
					mcu1 = mcus.get(numProcessed);
					mcu2 = mcus.get(numProcessed + 1);
				}
				else if (horizontal == 2 && vertical == 2) {
					mcu1 = mcus.get(numProcessed);
					mcu2 = mcus.get(numProcessed + 1);
					mcu3 = mcus.get(numProcessed + 2);
					mcu4 = mcus.get(numProcessed + 3);
				}
			}
			if (!HuffmanDecoder.decodeHuffmanData(mcu1, mcu2, mcu3, mcu4)) {
				System.err.println("Error during scan " + numScans);
				// add blank mcus on error to avoid out of bounds errors later
				while (mcus.size() < numMCUs + ((header.width + 7) / 8)) {
					mcus.add(new MCU());
				}
				return false;
			}
			if (mcus.size() < numMCUs) {
				if (horizontal == 1 && vertical == 1) {
					mcus.add(mcu1);
					numProcessed += 1;
				}
				else if (horizontal == 2 && vertical == 1) {
					mcus.add(mcu1);
					mcus.add(mcu2);
					numProcessed += 2;
				}
				else if (horizontal == 1 && vertical == 2) {
					mcus.add(mcu1);
					mcus.add(mcu2);
					numProcessed += 2;
				}
				else if (horizontal == 2 && vertical == 2) {
					mcus.add(mcu1);
					mcus.add(mcu2);
					mcus.add(mcu3);
					mcus.add(mcu4);
					numProcessed += 4;
				}
			}
			else {
				if (horizontal == 1 && vertical == 1) {
					mcus.set(numProcessed, mcu1);
					numProcessed += 1;
				}
				else if (horizontal == 2 && vertical == 1) {
					mcus.set(numProcessed, mcu1);
					mcus.set(numProcessed + 1, mcu2);
					numProcessed += 2;
				}
				else if (horizontal == 1 && vertical == 2) {
					mcus.set(numProcessed, mcu1);
					mcus.set(numProcessed + 1, mcu2);
					numProcessed += 2;
				}
				else if (horizontal == 2 && vertical == 2) {
					mcus.set(numProcessed, mcu1);
					mcus.set(numProcessed + 1, mcu2);
					mcus.set(numProcessed + 2, mcu3);
					mcus.set(numProcessed + 3, mcu4);
					numProcessed += 4;
				}
			}
		}
		header.imageData.clear();
		return true;
	}
	
	private void ReadQuantizationTable(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading Quantization Tables");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			short info = (short)(b.get() & 0xFF);
			QuantizationTable table = new QuantizationTable();
			table.tableID = (short)(info & 0x0F);
			
			if (table.tableID > 3) {
				System.err.println("Error - Invalid Quantization table ID: " + table.tableID);
				header.valid = false;
				return;
			}
			
			if ((info & 0xF0) == 0) {
				table.precision = 1;
			}
			else {
				table.precision = 2;
			}
			if (table.precision == 2) {
				for (int i = 0; i < 64; ++i) {
					table.table[i] = (b.get() & 0xFF) << 8 + (b.get() & 0xFF);
				}
			}
			else {
				for (int i = 0; i < 64; ++i) {
					table.table[i] = (b.get() & 0xFF);
				}
			}
			header.quantizationTables[table.tableID] = table;
			length -= 64 * table.precision + 1;
		}
		if (length != 0) {
			System.err.println("Error - DQT Invalid");
			header.valid = false;
		}
	}
	
	private void ReadStartOfFrame(ByteBuffer b, Header header) throws IOException {
		if (header.numComponents != 0) {
			System.err.println("Error - Multiple SOFs detected");
			header.valid = false;
			return;
		}
		if (verbose) 
			System.out.println("Reading Start of Frame");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.precision = (short)(b.get() & 0xFF);
		
		if (header.precision != 8) {
			System.err.println("Error - Invalid precision: " + header.precision);
			header.valid = false;
			return;
		}
		
		header.height = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		header.width = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		
		if (header.height == 0 || header.width == 0) {
			System.err.println("Error - Invalid dimensions");
			header.valid = false;
			return;
		}
		
		header.numComponents = (short)(b.get() & 0xFF);
		if (header.numComponents == 4) {
			System.err.println("Error - CMYK color mode not supported");
			header.valid = false;
			return;
		}
		for (int i = 0; i < header.numComponents; ++i) {
			ColorComponent component = new ColorComponent();
			component.componentID = (short)(b.get() & 0xFF);
			short samplingFactor = (short)(b.get() & 0xFF);
			component.horizontalSamplingFactor = (short)((samplingFactor & 0xF0) >> 4);
			component.verticalSamplingFactor = (short)(samplingFactor & 0x0F);
			component.quantizationTableID = (short)(b.get() & 0xFF);
			
			if (component.componentID == 0) {
				header.zeroBased = true;
			}
			if (header.zeroBased) {
				component.componentID += 1;
			}
			
			if (component.componentID == 4 || component.componentID == 5) {
				System.err.println("Error - YIQ color mode not supported");
				header.valid = false;
				return;
			}
			if (header.colorComponents[component.componentID - 1] != null) {
				System.err.println("Error - Duplicate color component ID");
				header.valid = false;
				return;
			}
			if (component.quantizationTableID > 3) {
				System.err.println("Error - Invalid Quantization table ID in frame components");
				header.valid = false;
				return;
			}
			
			header.colorComponents[component.componentID - 1] = component;
		}
		if (length - 8 - (header.numComponents * 3) != 0) {
			System.err.println("Error - SOF Invalid");
			header.valid = false;
		}
	}
	
	private void ReadHuffmanTable(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading Huffman Tables");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		length -= 2;
		while (length > 0) {
			HuffmanTable table = new HuffmanTable();
			short info = (short)(b.get() & 0xFF);
			table.tableID = (short)(info & 0x0F);
			boolean ACTable = (info & 0xF0) != 0;
			/*if (ACTable) {
				System.out.println("AC Table " + table.tableID);
			}
			else {
				System.out.println("DC Table " + table.tableID);
			}*/
			
			if (table.tableID > 3) {
				System.err.println("Error - Invalid Huffman table ID: " + table.tableID);
				header.valid = false;
				return;
			}
			
			int allSymbols = 0;
			short[] numSymbols = new short[16];
			for (int i = 0; i < 16; ++i) {
				numSymbols[i] = (short)(b.get() & 0xFF);
				allSymbols += numSymbols[i];
			}
			
			if (allSymbols > 256) {
				System.err.println("Error - Invalid Huffman table");
				header.valid = false;
				return;
			}
			
			for (int i = 0; i < 16; ++i) {
				table.symbols.add(new ArrayList<Short>());
				for (int j = 0; j < numSymbols[i]; ++j) {
					table.symbols.get(i).add((short)(b.get() & 0xFF));
				}
			}
			if (ACTable) {
				header.huffmanACTables[table.tableID] = table;
			}
			else {
				header.huffmanDCTables[table.tableID] = table;
			}
			length -= allSymbols + 17;
		}
		if (length != 0) {
			System.err.println("Error - DHT Invalid");
			header.valid = false;
		}
	}
	
	private void ReadStartOfScan(ByteBuffer b, Header header) throws IOException {
		if (header.numComponents == 0) {
			System.err.println("Error - SOS detected before SOF");
			header.valid = false;
			return;
		}
		if (verbose) 
			System.out.println("Reading Start of Scan");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		
		for (int i = 0; i < header.numComponents; ++i) {
			header.colorComponents[i].used = false;
		}
		
		int numComponents = (b.get() & 0xFF);
		for (int i = 0; i < numComponents; ++i) {
			short componentID = (short)(b.get() & 0xFF);
			short huffmanTableID = (short)(b.get() & 0xFF);
			short huffmanACTableID = (short)(huffmanTableID & 0x0F);
			short huffmanDCTableID = (short)((huffmanTableID & 0xF0) >> 4);
			//System.out.println("Component " + componentID);
			
			if (header.zeroBased) {
				componentID += 1;
			}
			
			if (huffmanACTableID > 3 || huffmanDCTableID > 3) {
				System.err.println("Error - Invalid Huffman table ID in scan components");
				header.valid = false;
				return;
			}

			if (componentID > header.numComponents) {
				System.err.println("Error - Invalid Color Component ID: " + componentID);
				header.valid = false;
				return;
			}
			header.colorComponents[componentID - 1].huffmanACTableID = huffmanACTableID;
			header.colorComponents[componentID - 1].huffmanDCTableID = huffmanDCTableID;
			header.colorComponents[componentID - 1].used = true;
		}
		header.startOfSelection = (short)(b.get() & 0xFF);
		header.endOfSelection = (short)(b.get() & 0xFF);
		short successiveApproximation = (short)(b.get() & 0xFF);
		header.successiveApproximationLow = (short)(successiveApproximation & 0x0F);
		header.successiveApproximationHigh = (short)((successiveApproximation & 0xF0) >> 4);
		//System.out.println("Ss " + header.startOfSelection + ", Se " + header.endOfSelection + ", Ah " + header.successiveApproximationHigh + ", Al " + header.successiveApproximationLow);
		
		if (header.frameType.equals("Baseline") &&
			(header.startOfSelection != 0 ||
			header.endOfSelection != 63 ||
			header.successiveApproximationHigh != 0 ||
			header.successiveApproximationLow != 0)) {
			System.err.println("Error - Spectral selection or approximation is incompatible with Baseline");
			header.valid = false;
			return;
		}
		
		if (length - 6 - (numComponents * 2) != 0) {
			System.err.println("Error - SOS Invalid");
			header.valid = false;
		}
	}
	
	private void ReadRestartInterval(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading Restart Interval");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		header.restartInterval = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		if (length - 4 != 0) {
			System.err.println("Error - DRI Invalid");
			header.valid = false;
		}
	}
	
	private void ReadRSTN(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading RSTN");
		// RSTN has no length
	}
	
	private void ReadAPPN(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading APPN");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		// all of APPN markers can be ignored
		for (int i = 0; i < length - 2; ++i) {
			b.get();
		}
	}
	
	private void ReadComment(ByteBuffer b, Header header) throws IOException {
		if (verbose) 
			System.out.println("Reading Comment");
		int length = ((b.get() & 0xFF) << 8) + (b.get() & 0xFF);
		//System.out.println("Length: " + (length + 2));
		// all comment markers can be ignored
		for (int i = 0; i < length - 2; ++i) {
			b.get();
		}
	}
	
	public void queueFile(String inFile) {
		inputFiles.add(inFile);
	}
	
	public void sendMCU(MCU emcu) {
		if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_4)) {
			DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.y[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
			
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.cb[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CB_204);
			
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.cr[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CR_304);
			
			
			PipeWriter.publishWrites(output);
			
			numProcessed += 1;
			if (header.restartInterval > 0 && numProcessed % header.restartInterval == 0) {
				HuffmanDecoder.restart();
			}
		}
		else {
			System.err.println("JPG Scanner requesting shutdown");
			requestShutdown();
		}
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeWriter.hasRoomForWrite(output) && numProcessed < numMCUs) {
			int horizontal = header.colorComponents[0].horizontalSamplingFactor;
			int vertical = header.colorComponents[0].verticalSamplingFactor;
			if (aboutToSend != 0) {
				if (aboutToSend == 2) {
					sendMCU(mcu2);
					if (horizontal == 2 && vertical == 2) {
						aboutToSend = 4;
					}
					else {
						aboutToSend = 0;
					}
				}
				else if (aboutToSend == 3) {
					sendMCU(mcu3);
					if (horizontal == 2 && vertical == 2) {
						aboutToSend = 2;
					}
					else {
						aboutToSend = 0;
					}
				}
				else { // if (aboutToSend == 4) {
					sendMCU(mcu4);
					aboutToSend = 0;
				}
			}
			else {
				if (header.frameType.equals("Progressive")) {
					if (horizontal == 1 && vertical == 1) {
						mcu1 = mcus.get(numProcessed);
					}
					else if (horizontal == 2 && vertical == 1) {
						mcu1 = mcus.get(numProcessed);
						mcu2 = mcus.get(numProcessed + 1);
					}
					else if (horizontal == 1 && vertical == 2) {
						mcu1 = mcus.get(numProcessed);
						mcu2 = mcus.get(numProcessed + 1);
					}
					else if (horizontal == 2 && vertical == 2) {
						mcu1 = mcus.get(numProcessed);
						mcu2 = mcus.get(numProcessed + 1);
						mcu3 = mcus.get(numProcessed + 2);
						mcu4 = mcus.get(numProcessed + 3);
					}
				}
				else {
					HuffmanDecoder.decodeHuffmanData(mcu1, mcu2, mcu3, mcu4);
				}
				// write mcu to pipe
				if (horizontal == 1 && vertical == 1) {
					sendMCU(mcu1);
					aboutToSend = 0;
				}
				else if (horizontal == 2 && vertical == 1) {
					sendMCU(mcu1);
					aboutToSend = 2;
					if (PipeWriter.hasRoomForWrite(output)) {
						sendMCU(mcu2);
						aboutToSend = 0;
					}
				}
				else if (horizontal == 1 && vertical == 2) {
					sendMCU(mcu1);
					aboutToSend = 3;
					if (PipeWriter.hasRoomForWrite(output)) {
						sendMCU(mcu3);
						aboutToSend = 0;
					}
				}
				else if (horizontal == 2 && vertical == 2) {
					sendMCU(mcu1);
					aboutToSend = 3;
					if (PipeWriter.hasRoomForWrite(output)) {
						sendMCU(mcu3);
						aboutToSend = 2;
						if (PipeWriter.hasRoomForWrite(output)) {
							sendMCU(mcu2);
							aboutToSend = 4;
							if (PipeWriter.hasRoomForWrite(output)) {
								sendMCU(mcu4);
								aboutToSend = 0;
							}
						}
					}
				}
			}
		}
		if (PipeWriter.hasRoomForFragmentOfSize(output, 400) && !inputFiles.isEmpty()) {
			String file = inputFiles.get(0);
			inputFiles.remove(0);
			System.out.println(file);
			try {
				mcus = new ArrayList<MCU>();
				header = ReadJPG(file, mcus);
				if (header == null || !header.valid) {
					System.err.println("Error - JPG file '" + file + "' invalid");
					if (inputFiles.size() > 0) {
						return;
					}
					else if (verbose) 
						System.out.println("All input files read.");
					header = new Header();
					header.width = 0;
					header.height = 0;
					header.valid = false;
				}
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					// write header to pipe
					if (verbose) 
						System.out.println("JPG Scanner writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, file);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, (inputFiles.size() == 0 ? 1 : 0));
					PipeWriter.publishWrites(output);
					if (!header.valid) {
						return;
					}
				}
				else {
					System.err.println("JPG Scanner requesting shutdown");
					requestShutdown();
				}
				// write color component data to pipe
				for (int i = 0; i < header.numComponents; ++i) {
					if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
						if (verbose) 
							System.out.println("JPG Scanner writing color component to pipe...");
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, header.colorComponents[i].componentID);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, header.colorComponents[i].horizontalSamplingFactor);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, header.colorComponents[i].verticalSamplingFactor);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, header.colorComponents[i].quantizationTableID);
						PipeWriter.publishWrites(output);
					}
					else {
						System.err.println("JPG Scanner requesting shutdown");
						requestShutdown();
					}
				}
				// write quantization tables to pipe
				for (int i = 0; i < header.quantizationTables.length; ++i) {
					if (header.quantizationTables[i] != null) {
						if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3)) {
							if (verbose) 
								System.out.println("JPG Scanner writing quantization table to pipe...");
	
							DataOutputBlobWriter<JPGSchema> quantizationTableWriter = PipeWriter.outputStream(output);
							DataOutputBlobWriter.openField(quantizationTableWriter);
							quantizationTableWriter.writeInt(header.quantizationTables[i].tableID);
							DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLEID_103);
							
							DataOutputBlobWriter.openField(quantizationTableWriter);
							quantizationTableWriter.writeInt(header.quantizationTables[i].precision);
							DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_PRECISION_203);
							
							DataOutputBlobWriter.openField(quantizationTableWriter);
							for (int j = 0; j < 64; ++j) {
								quantizationTableWriter.writeInt(header.quantizationTables[i].table[j]);
							}
							DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLE_303);
							
							PipeWriter.publishWrites(output);
						}
						else {
							System.err.println("JPG Scanner requesting shutdown");
							requestShutdown();
						}
					}
				}
				if (header.frameType.equals("Baseline")) {
					HuffmanDecoder.beginDecode(header);
				}
				mcuWidth = (header.width + 7) / 8;
				mcuHeight = (header.height + 7) / 8;
				if (header.colorComponents[0].horizontalSamplingFactor == 2 &&
					((header.width - 1) / 8 + 1) % 2 == 1) {
					mcuWidth += 1;
				}
				if (header.colorComponents[0].verticalSamplingFactor == 2 &&
					((header.height - 1) / 8 + 1) % 2 == 1) {
					mcuHeight += 1;
				}
				numMCUs = mcuWidth * mcuHeight;
				numProcessed = 0;
			}
			catch (IOException e) {
				System.err.println("Error - Unknown error reading file '" + file + "'");
			}
			if (inputFiles.isEmpty()) {
				if (verbose) 
					System.out.println("All input files read.");
			}
		}
		timer += (System.nanoTime() - s);
	}
	
	/*public static void main(String[] args) {
		Header header = null;
		try {
			ArrayList<MCU> mcus = new ArrayList<MCU>();
			header = ReadJPG("test_jpgs/earth_progressive.jpg", mcus);
			if (header != null && header.valid) {
				System.out.println("DQT============");
				for (int i = 0; i < header.quantizationTables.length; ++i) {
					if (header.quantizationTables[i] != null) {
						System.out.println("Table ID: " + header.quantizationTables[i].tableID);
						System.out.println("Precision: " + header.quantizationTables[i].precision);
						System.out.print("Table Data:");
						for (int j = 0; j < header.quantizationTables[i].table.length; ++j) {
							if (j % 8 == 0) {
								System.out.println();
							}
							System.out.print(String.format("%02d ", header.quantizationTables[i].table[j]));
						}
						System.out.println();
					}
				}
				System.out.println("SOF============");
				System.out.println("Frame Type: " + header.frameType);
				System.out.println("Precision: " + header.precision);
				System.out.println("Height: " + header.height);
				System.out.println("Width: " + header.width);
				System.out.println("DHT============");
				System.out.println("DC Tables:");
				for (int i = 0; i < header.huffmanDCTables.length; ++i) {
					if (header.huffmanDCTables[i] != null) {
						System.out.println("Table ID: " + header.huffmanDCTables[i].tableID);
						System.out.println("Symbols:");
						for (int j = 0; j < header.huffmanDCTables[i].symbols.size(); ++j) {
							System.out.print((j + 1) + ": ");
							for (int k = 0; k < header.huffmanDCTables[i].symbols.get(j).size(); ++k) {
								System.out.print(header.huffmanDCTables[i].symbols.get(j).get(k));
								if (k < header.huffmanDCTables[i].symbols.get(j).size() - 1) {
									System.out.print(", ");
								}
							}
							System.out.println();
						}
					}
				}
				System.out.println("AC Tables:");
				for (int i = 0; i < header.huffmanACTables.length; ++i) {
					if (header.huffmanACTables[i] != null) {
						System.out.println("Table ID: " + header.huffmanACTables[i].tableID);
						System.out.println("Symbols:");
						for (int j = 0; j < header.huffmanACTables[i].symbols.size(); ++j) {
							System.out.print((j + 1) + ": ");
							for (int k = 0; k < header.huffmanACTables[i].symbols.get(j).size(); ++k) {
								System.out.print(header.huffmanACTables[i].symbols.get(j).get(k));
								if (k < header.huffmanACTables[i].symbols.get(j).size() - 1) {
									System.out.print(", ");
								}
							}
							System.out.println();
						}
					}
				}
				System.out.println("SOS============");
				System.out.println("Start of Selection: " + header.startOfSelection);
				System.out.println("End of Selection: " + header.endOfSelection);
				System.out.println("Successive Approximation High: " + header.successiveApproximationHigh);
				System.out.println("Successive Approximation Low: " + header.successiveApproximationLow);
				System.out.println("Restart Interval: " + header.restartInterval);
				System.out.println("Color Components:");
				for (int i = 0; i < header.numComponents; ++i) {
					System.out.println("\tComponent ID: " + header.colorComponents[i].componentID);
					System.out.println("\tHorizontal Sampling Factor: " + header.colorComponents[i].horizontalSamplingFactor);
					System.out.println("\tVertical Sampling Factor: " + header.colorComponents[i].verticalSamplingFactor);
					System.out.println("\tQuantization Table ID: " + header.colorComponents[i].quantizationTableID);
					System.out.println("\tHuffman AC Table ID: " + header.colorComponents[i].huffmanACTableID);
					System.out.println("\tHuffman DC Table ID: " + header.colorComponents[i].huffmanDCTableID);
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
	}*/
}
