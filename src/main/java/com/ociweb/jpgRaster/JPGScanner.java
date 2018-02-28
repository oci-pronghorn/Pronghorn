package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.Header;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.util.ArrayList;

public class JPGScanner extends PronghornStage {

	private ArrayList<String> inputFiles = new ArrayList<String>();
	private final Pipe<JPGSchema> output;
	
	int numMCUs = 0;
	int numProcessed = 0;
	
	MCU mcu = new MCU();
	
	protected JPGScanner(GraphManager graphManager, Pipe<JPGSchema> output) {
		super(graphManager, NONE, output);
		this.output = output;
	}
	
	public static Header ReadJPG(String filename) throws IOException {
		Header header = new Header();
		DataInputStream f = new DataInputStream(new FileInputStream(filename));
		
		// JPG file must begin with 0xFFD8
		short last = (short)f.readUnsignedByte();
		short current = (short)f.readUnsignedByte();
		if (last != 0xFF || current != JPGConstants.SOI) {
			header.valid = false;
			f.close();
			return header;
		}
		System.out.println("Start of Image");
		last = (short)f.readUnsignedByte();
		current = (short)f.readUnsignedByte();
		
		while (true) {
			if (header.valid == false) {
				break;
			}
			
			if (last == 0xFF) {
				if      (current == JPGConstants.DQT) {
					ReadQuantizationTable(f, header);
				}
				else if (current == JPGConstants.SOF0) {
					header.frameType = "Baseline";
					ReadStartOfFrame(f, header);
				}
				// only Baseline is supported for now
				/*else if (current == JPGConstants.SOF1) {
					header.frameType = "Extended Sequential";
					ReadStartOfFrame(f, header);
				}
				else if (current == JPGConstants.SOF2) {
					header.frameType = "Progressive";
					ReadStartOfFrame(f, header);
				}
				else if (current == JPGConstants.SOF3) {
					header.frameType = "Lossless";
					ReadStartOfFrame(f, header);
				}*/
				else if (current == JPGConstants.DHT) {
					ReadHuffmanTable(f, header);
				}
				else if (current == JPGConstants.SOS) {
					ReadStartOfScan(f, header);
					break;
				}
				else if (current == JPGConstants.DRI) {
					ReadRestartInterval(f, header);
				}
				else if (current >= JPGConstants.RST0 && current <= JPGConstants.RST7) {
					ReadRSTN(f, header);
				}
				else if (current >= JPGConstants.APP0 && current <= JPGConstants.APP15) {
					ReadAPPN(f, header);
				}
				else if (current == JPGConstants.COM) {
					ReadComment(f, header);
				}
				else if (current == 0xFF) {
					// skip
					current = (short)f.readUnsignedByte();
					continue;
				}
				else if (current == JPGConstants.JPG0 ||
						 current == JPGConstants.JPG13 ||
						 current == JPGConstants.DNL ||
						 current == JPGConstants.DHP ||
						 current == JPGConstants.EXP) {
					// unsupported segments that can be skipped
					ReadComment(f, header);
				}
				else if (current == JPGConstants.TEM) {
					// unsupported segment with no size
				}
				else if (current == JPGConstants.SOI) {
					System.err.println("Error - This JPG contains an embedded JPG; This is not supported");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current == JPGConstants.EOI) {
					System.err.println("Error - EOI detected before SOS");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current == JPGConstants.DAC) {
					System.err.println("Error - Arithmetic Table mode is not supported");
					header.valid = false;
					f.close();
					return header;
				}
				else if (current >= JPGConstants.SOF0 && current <= JPGConstants.SOF15) {
					System.err.println("Error - This Start of Frame marker is not supported: " + String.format("0x%2x", current));
					header.valid = false;
					f.close();
					return header;
				}
				else {
					System.err.println("Error - Unknown Marker: " + String.format("0x%2x", current));
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
		if (header.valid) {
			current = (short)f.readUnsignedByte();
			while (true) {
				last = current;
				current = (short)f.readUnsignedByte();
				if      (last == 0xFF && current == JPGConstants.EOI) {
					System.out.println("End of Image");
					break;
				}
				else if (last == 0xFF && current == 0x00) {
					header.imageData.add(last);
					// advance by a byte, to drop 0x00
					current = (short)f.readUnsignedByte();
				}
				// this doesn't seem to be true for SOF2
				else if (last == 0xFF) {
					System.err.println("Invalid marker during compressed data scan: " + String.format("0x%2x", current));
					header.valid = false;
					f.close();
					return header;
				}
				else {
					header.imageData.add(last);
				}
			}
		}
		f.close();
		
		for (int i = 0; i < header.colorComponents.size(); ++i) {
			if (header.colorComponents.get(i).quantizationTableID >= header.quantizationTables.size()) {
				System.err.println("Error - Color component " + i + " using invalid quantization table ID " + header.colorComponents.get(i).quantizationTableID);
				header.valid = false;
			}
			if (header.colorComponents.get(i).huffmanDCTableID >= header.huffmanDCTables.size()) {
				System.err.println("Error - Color component " + i + " using invalid DC table ID " + header.colorComponents.get(i).huffmanDCTableID);
				header.valid = false;
			}
			if (header.colorComponents.get(i).huffmanACTableID >= header.huffmanACTables.size()) {
				System.err.println("Error - Color component " + i + " using invalid AC table ID " + header.colorComponents.get(i).huffmanACTableID);
				header.valid = false;
			}
		}
		if (header.colorComponents.size() != 1 && header.colorComponents.size() != 3) {
			System.err.println("Error - " + header.colorComponents.size() + " color components given (1 or 3 required)");
			header.valid = false;
		}
		
		if (header.huffmanDCTables.size() == 2) {
			if (header.huffmanDCTables.get(0).tableID == header.huffmanDCTables.get(1).tableID) {
				System.err.println("Error - Huffman DC tables given same ID");
				header.valid = false;
			}
			else if (header.huffmanDCTables.get(0).tableID > header.huffmanDCTables.get(1).tableID) {
				HuffmanTable temp = header.huffmanDCTables.get(0);
				header.huffmanDCTables.remove(0);
				header.huffmanDCTables.add(temp);
			}
		}
		if (header.huffmanACTables.size() == 2) {
			if (header.huffmanACTables.get(0).tableID == header.huffmanACTables.get(1).tableID) {
				System.err.println("Error - Huffman AC tables given same ID");
				header.valid = false;
			}
			else if (header.huffmanACTables.get(0).tableID > header.huffmanACTables.get(1).tableID) {
				HuffmanTable temp = header.huffmanACTables.get(0);
				header.huffmanACTables.remove(0);
				header.huffmanACTables.add(temp);
			}
		}
		
		for (int i = 0; i < header.colorComponents.size(); ++i) {
			if (header.colorComponents.get(i).horizontalSamplingFactor != 1 ||
				header.colorComponents.get(i).verticalSamplingFactor != 1) {
				System.err.println("Error - Sampling Factors not yet supported");
				header.valid = false;
				break;
			}
		}
		
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
			
			if (table.tableID > 1) {
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
					table.table[i] = f.readUnsignedByte() << 8 + f.readUnsignedByte();
				}
			}
			else {
				for (int i = 0; i < 64; ++i) {
					table.table[i] = f.readUnsignedByte();
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
		if (header.colorComponents.size() != 0) {
			System.err.println("Error - Multiple SOFs detected");
			header.valid = false;
			return;
		}
		System.out.println("Reading Start of Frame");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		header.precision = (short)f.readUnsignedByte();
		
		if (header.precision != 8) {
			System.err.println("Error - Invalid precision: " + header.precision);
			header.valid = false;
			return;
		}
		
		header.height = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		header.width = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		
		if (header.height == 0 || header.width == 0) {
			System.err.println("Error - Invalid dimensions");
			header.valid = false;
			return;
		}
		
		int numComponents = f.readUnsignedByte();
		if (numComponents == 4) {
			System.err.println("Error - CMYK color mode not supported");
			header.valid = false;
			return;
		}
		for (int i = 0; i < numComponents; ++i) {
			ColorComponent component = new ColorComponent();
			component.componentID = (short)f.readUnsignedByte();
			short samplingFactor = (short)f.readUnsignedByte();
			component.horizontalSamplingFactor = (short)((samplingFactor & 0xF0) >> 4);
			component.verticalSamplingFactor = (short)(samplingFactor & 0x0F);
			component.quantizationTableID = (short)f.readUnsignedByte();
			
			if (component.componentID == 4 || component.componentID == 5) {
				System.err.println("Error - YIQ color mode not supported");
				header.valid = false;
				return;
			}
			for (int j = 0; j < header.colorComponents.size(); ++j) {
				if (header.colorComponents.get(j).componentID == component.componentID) {
					System.err.println("Error - Duplicate color component ID");
					header.valid = false;
					return;
				}
			}
			
			header.colorComponents.add(component);
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
			boolean ACTable = (info & 0xF0) != 0;
			
			if (table.tableID > 1) {
				System.err.println("Error - Invalid Huffman table ID: " + table.tableID);
				header.valid = false;
				return;
			}
			
			int allSymbols = 0;
			short[] numSymbols = new short[16];
			for (int i = 0; i < 16; ++i) {
				numSymbols[i] = (short)f.readUnsignedByte();
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
					table.symbols.get(i).add((short)f.readUnsignedByte());
				}
			}
			if (ACTable) {
				header.huffmanACTables.add(table);
			}
			else {
				header.huffmanDCTables.add(table);
			}
			length -= allSymbols + 17;
		}
		if (length != 0) {
			System.err.println("Error - DHT Invalid");
			header.valid = false;
		}
	}
	
	private static void ReadStartOfScan(DataInputStream f, Header header) throws IOException {
		if (header.colorComponents.size() == 0) {
			System.err.println("Error - SOS detected before SOF");
			header.valid = false;
			return;
		}
		System.out.println("Reading Start of Scan");
		int length = (f.readUnsignedByte() << 8) + f.readUnsignedByte();
		//System.out.println("Length: " + (length + 2));
		int numComponents = f.readUnsignedByte();
		if (numComponents != header.colorComponents.size()) {
			System.err.println("Error - Frame/Scan Color Component mismatch");
			header.valid = false;
			return;
		}
		for (int i = 0; i < numComponents; ++i) {
			short componentID = (short)f.readUnsignedByte();
			short huffmanTableID = (short)f.readUnsignedByte();
			short huffmanACTableID = (short)(huffmanTableID & 0x0F);
			short huffmanDCTableID = (short)((huffmanTableID & 0xF0) >> 4);
			
			if (huffmanACTableID > 1 || huffmanDCTableID > 1) {
				System.err.println("Error - Invalid Huffman table ID in scan components");
				header.valid = false;
				return;
			}
			
			boolean found = false;
			for (int j = 0; j < header.colorComponents.size(); ++j) {
				if (componentID == header.colorComponents.get(j).componentID) {
					header.colorComponents.get(j).huffmanACTableID = huffmanACTableID;
					header.colorComponents.get(j).huffmanDCTableID = huffmanDCTableID;
					found = true;
					break;
				}
			}
			if (!found) {
				System.err.println("Error - Invalid Color Component ID: " + componentID);
				header.valid = false;
				return;
			}
		}
		header.startOfSelection = (short)f.readUnsignedByte();
		header.endOfSelection = (short)f.readUnsignedByte();
		header.successiveApproximation = (short)f.readUnsignedByte();
		
		if (header.startOfSelection != 0 ||
			header.endOfSelection != 63 ||
			header.successiveApproximation != 0) {
			System.err.println("Error - Non-standard selection and successive approximation not yet supported");
			header.valid = false;
			return;
		}
		
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
	
	public void queueFile(String inFile) {
		inputFiles.add(inFile);
	}

	@Override
	public void run() {
		while (PipeWriter.hasRoomForWrite(output) && numProcessed < numMCUs) {
			if (HuffmanDecoder.decodeHuffmanData(mcu)) {
				// write mcu to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_6)) {
					DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.y[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cb[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cr[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306);
					PipeWriter.publishWrites(output);
					
					numProcessed += 1;
				}
				else {
					System.err.println("Requesting shutdown");
					requestShutdown();
				}
			}
			else {
				break; // should never happen
			}
		}
		if (PipeWriter.hasRoomForWrite(output) && !inputFiles.isEmpty()) {
			String file = inputFiles.get(0);
			inputFiles.remove(0);
			try {
				Header header = ReadJPG(file + ".jpg");
				if (header == null || !header.valid) {
					System.err.println("Error - JPG file " + file + " invalid");
					return;
				}
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					// write header to pipe
					System.out.println("Scanner writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, file);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, header.frameType);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501, header.precision);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601, header.startOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701, header.endOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801, header.successiveApproximation);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Requesting shutdown");
					requestShutdown();
				}
				// write color component data to pipe
				for (int i = 0; i < header.colorComponents.size(); ++i) {
					System.out.println("Attempting to write color component to pipe...");
					if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
						System.out.println("Scanner writing color component to pipe...");
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, header.colorComponents.get(i).componentID);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, header.colorComponents.get(i).horizontalSamplingFactor);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, header.colorComponents.get(i).verticalSamplingFactor);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, header.colorComponents.get(i).quantizationTableID);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502, header.colorComponents.get(i).huffmanACTableID);
						PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602, header.colorComponents.get(i).huffmanDCTableID);
						PipeWriter.publishWrites(output);
					}
					else {
						System.err.println("Requesting shutdown");
						requestShutdown();
					}
				}
				// write quantization tables to pipe
				for (int i = 0; i < header.quantizationTables.size(); ++i) {
					System.out.println("Attempting to write quantization table to pipe...");
					if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5)) {
						System.out.println("Scanner writing quantization table to pipe...");

						DataOutputBlobWriter<JPGSchema> quantizationTableWriter = PipeWriter.outputStream(output);
						DataOutputBlobWriter.openField(quantizationTableWriter);
						quantizationTableWriter.writeInt(header.quantizationTables.get(i).tableID);
						DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLEID_105);
						
						DataOutputBlobWriter.openField(quantizationTableWriter);
						quantizationTableWriter.writeInt(header.quantizationTables.get(i).precision);
						DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_PRECISION_205);
						
						DataOutputBlobWriter.openField(quantizationTableWriter);
						for (int j = 0; j < 64; ++j) {
							quantizationTableWriter.writeInt(header.quantizationTables.get(i).table[j]);
						}
						DataOutputBlobWriter.closeHighLevelField(quantizationTableWriter, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLE_305);
						
						PipeWriter.publishWrites(output);
					}
					else {
						System.err.println("Requesting shutdown");
						requestShutdown();
					}
				}
				HuffmanDecoder.beginDecode(header);
				numMCUs = ((header.width + 7) / 8) * ((header.height + 7) / 8);
				numProcessed = 0;
			}
			catch (IOException e) {
				System.err.println("Error - Unknown error reading " + file);
			}
		}
	}
	
	/*public static void main(String[] args) {
		Header header = null;
		try {
			header = ReadJPG("test_jpgs/huff_simple0.jpg");
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
				System.out.println("DHT============");
				System.out.println("DC Tables:");
				for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
					System.out.println("Table ID: " + header.huffmanDCTables.get(i).tableID);
					System.out.println("Symbols:");
					for (int j = 0; j < header.huffmanDCTables.get(i).symbols.size(); ++j) {
						System.out.print((j + 1) + ": ");
						for (int k = 0; k < header.huffmanDCTables.get(i).symbols.get(j).size(); ++k) {
							System.out.print(header.huffmanDCTables.get(i).symbols.get(j).get(k));
							if (k < header.huffmanDCTables.get(i).symbols.get(j).size() - 1) {
								System.out.print(", ");
							}
						}
						System.out.println();
					}
				}
				System.out.println("AC Tables:");
				for (int i = 0; i < header.huffmanACTables.size(); ++i) {
					System.out.println("Table ID: " + header.huffmanACTables.get(i).tableID);
					System.out.println("Symbols:");
					for (int j = 0; j < header.huffmanACTables.get(i).symbols.size(); ++j) {
						System.out.print((j + 1) + ": ");
						for (int k = 0; k < header.huffmanACTables.get(i).symbols.get(j).size(); ++k) {
							System.out.print(header.huffmanACTables.get(i).symbols.get(j).get(k));
							if (k < header.huffmanACTables.get(i).symbols.get(j).size() - 1) {
								System.out.print(", ");
							}
						}
						System.out.println();
					}
				}
				System.out.println("SOS============");
				System.out.println("Start of Selection: " + header.startOfSelection);
				System.out.println("End of Selection: " + header.endOfSelection);
				System.out.println("Successive Approximation: " + header.successiveApproximation);
				System.out.println("Color Components:");
				for (int i = 0; i < header.colorComponents.size(); ++i) {
					System.out.println("\tComponent ID: " + header.colorComponents.get(i).componentID);
					System.out.println("\tHorizontal Sampling Factor: " + header.colorComponents.get(i).horizontalSamplingFactor);
					System.out.println("\tVertical Sampling Factor: " + header.colorComponents.get(i).verticalSamplingFactor);
					System.out.println("\tQuantization Table ID: " + header.colorComponents.get(i).quantizationTableID);
					System.out.println("\tHuffman AC Table ID: " + header.colorComponents.get(i).huffmanACTableID);
					System.out.println("\tHuffman DC Table ID: " + header.colorComponents.get(i).huffmanDCTableID);
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
