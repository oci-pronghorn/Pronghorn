package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;

import java.util.ArrayList;

public class HuffmanDecoder {

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
	}
	
	static Header header;
	static BitReader b;
	static ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes;
	static ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes;

	static short yDCTableID;
	static short yACTableID;
	static short cbDCTableID;
	static short cbACTableID;
	static short crDCTableID;
	static short crACTableID;

	static short previousYDC;
	static short previousCbDC;
	static short previousCrDC;
	
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
	
	private static Boolean decodeMCUComponent(ArrayList<ArrayList<Integer>> DCTableCodes,
											  ArrayList<ArrayList<Integer>> ACTableCodes,
											  HuffmanTable DCTable,
											  HuffmanTable ACTable,
											  short[] component,
											  short previousDC) {
		
		// get the DC value for this MCU
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
					//System.out.println("DC Value: " + component[0]);
					found = true;
					break;
				}
			}
			if (found) {
				break;
			}
			currentCode = (currentCode << 1) | b.nextBit();
		}
		if (!found ) {
			System.err.println("Error - Invalid DC Value");
			return false;
		}
		
		// get the AC values for this MCU
		for (int k = 1; k < 64; ++k) {
			found = false;
			currentCode = b.nextBit();
			for (int i = 0; i < 16; ++i) {
				for (int j = 0; j < ACTableCodes.get(i).size(); ++j) {
					if (currentCode == ACTableCodes.get(i).get(j)) {
						short decoderValue = ACTable.symbols.get(i).get(j);
						//System.out.println("Code -> Value : " + currentCode + " -> " + decoderValue);
						
						if (decoderValue == 0) {
							for (; k < 64; ++k) {
								component[JPG.zigZagMap[k]] = 0;
							}
						}
						else {
							short numZeroes = (short)((decoderValue & 0xF0) >> 4);
							short coeffLength = (short)(decoderValue & 0x0F);
							
							//System.out.println("k: " + k);
							//System.out.println("numZeroes: " + numZeroes);
							//System.out.println("coeffLength: " + coefLength);
							
							for (int l = 0; l < numZeroes; ++l){
								component[JPG.zigZagMap[k]] = 0;
								++k;
							}
							if (coeffLength > 11){
								System.out.println("Error - coeflength > 11");
							}
							
							if (coeffLength != 0) {
								component[JPG.zigZagMap[k]] = (short)b.nextBits(coeffLength);
								
								if (component[JPG.zigZagMap[k]] < (1 << (coeffLength - 1))) {
									component[JPG.zigZagMap[k]] -= (1 << coeffLength) - 1;
								}
								//System.out.println("AC Value: " + component[map[k]]);
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
			if (!found ) {
				System.err.println("Error - Invalid AC Value");
				return false;
			}
		}
		
		/*System.out.print("Component Coefficients:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(component[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		return true;
	}
	
	public static MCU decodeHuffmanData() {
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
		if (!b.hasBits()) return null;
		
		MCU mcu = new MCU();
		
		//System.out.println("Decoding Y Component...");
		Boolean success = decodeMCUComponent(DCTableCodes.get(yDCTableID), ACTableCodes.get(yACTableID),
				  header.huffmanDCTables.get(yDCTableID), header.huffmanACTables.get(yACTableID), mcu.y, previousYDC);
		if (!success) {
			return null;
		}
		
		//System.out.println("Decoding Cb Component...");
		success = decodeMCUComponent(DCTableCodes.get(cbDCTableID), ACTableCodes.get(cbACTableID),
				  header.huffmanDCTables.get(cbDCTableID), header.huffmanACTables.get(cbACTableID), mcu.cb, previousCbDC);
		if (!success) {
			return null;
		}
		
		//System.out.println("Decoding Cr Component...");
		success = decodeMCUComponent(DCTableCodes.get(crDCTableID), ACTableCodes.get(crACTableID),
				  header.huffmanDCTables.get(crDCTableID), header.huffmanACTables.get(crACTableID), mcu.cr, previousCrDC);
		if (!success) {
			return null;
		}
		
		previousYDC = mcu.y[0];
		previousCbDC = mcu.cb[0];
		previousCrDC = mcu.cr[0];
		
		return mcu;
	}
	
	public static void beginDecode(Header h) {
		header = h;
		b = new BitReader(header.imageData);
		
		DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
		for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
			DCTableCodes.add(generateCodes(header.huffmanDCTables.get(i)));
		}
		for (int i = 0; i < header.huffmanACTables.size(); ++i) {
			ACTableCodes.add(generateCodes(header.huffmanACTables.get(i)));
		}

		yDCTableID  = header.colorComponents.get(0).huffmanDCTableID;
		yACTableID  = header.colorComponents.get(0).huffmanACTableID;
		cbDCTableID = header.colorComponents.get(1).huffmanDCTableID;
		cbACTableID = header.colorComponents.get(1).huffmanACTableID;
		crDCTableID = header.colorComponents.get(2).huffmanDCTableID;
		crACTableID = header.colorComponents.get(2).huffmanACTableID;

		previousYDC = 0;
		previousCbDC = 0;
		previousCrDC = 0;
	}

	/*@Override
	public void run() {
		if (b != null && b.hasBits()) {
			if (!decodeHuffmanData()) {
				System.err.println("Huffman requesting shutdown");
				requestShutdown();
			}
		}
		else if (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				String filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				header.frameType = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, new StringBuilder()).toString();
				header.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501);
				header.startOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601);
				header.endOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701);
				header.successiveApproximation = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					System.out.println("Writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, filename);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, header.frameType);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501, header.precision);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601, header.startOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701, header.endOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801, header.successiveApproximation);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Huffman requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				component.huffmanACTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502);
				component.huffmanDCTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602);
				header.colorComponents.add(component);
				PipeReader.releaseReadLock(input);
				
				// write color component data to pipe
				System.out.println("Attempting to write color component to pipe...");
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					System.out.println("Writing color component to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502, component.huffmanACTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602, component.huffmanDCTableID);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Huffman requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_COMPRESSEDDATAMESSAGE_3) {
				int size = PipeReader.readInt(input, JPGSchema.MSG_COMPRESSEDDATAMESSAGE_3_FIELD_LENGTH_103);
				ByteBuffer buffer = ByteBuffer.allocate(size * 2);
				PipeReader.readBytes(input, JPGSchema.MSG_COMPRESSEDDATAMESSAGE_3_FIELD_DATA_203, buffer);
				PipeReader.releaseReadLock(input);
				buffer.position(0);
				for (int i = 0; i < size; ++i) {
					header.imageData.add(buffer.getShort());
				}
				b = new BitReader(header.imageData);
				
				ArrayList<ArrayList<ArrayList<Integer>>> DCTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
				ArrayList<ArrayList<ArrayList<Integer>>> ACTableCodes = new ArrayList<ArrayList<ArrayList<Integer>>>(2);
				for (int i = 0; i < header.huffmanDCTables.size(); ++i) {
					DCTableCodes.add(generateCodes(header.huffmanDCTables.get(i)));
				}
				for (int i = 0; i < header.huffmanACTables.size(); ++i) {
					ACTableCodes.add(generateCodes(header.huffmanACTables.get(i)));
				}

				yDCTableID  = header.colorComponents.get(0).huffmanDCTableID;
				yACTableID  = header.colorComponents.get(0).huffmanACTableID;
				cbDCTableID = header.colorComponents.get(1).huffmanDCTableID;
				cbACTableID = header.colorComponents.get(1).huffmanACTableID;
				crDCTableID = header.colorComponents.get(2).huffmanDCTableID;
				crACTableID = header.colorComponents.get(2).huffmanACTableID;

				previousYDC = 0;
				previousCbDC = 0;
				previousCrDC = 0;
				
				if (!decodeHuffmanData()) {
					System.err.println("Huffman requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_HUFFMANTABLEMESSAGE_4) {
				HuffmanTable table = new HuffmanTable();
				table.tableID = (short) PipeReader.readInt(input, JPGSchema.MSG_HUFFMANTABLEMESSAGE_4_FIELD_TABLEID_104);
				ByteBuffer lengthsBuffer = ByteBuffer.allocate(16 * 2);
				PipeReader.readBytes(input, JPGSchema.MSG_HUFFMANTABLEMESSAGE_4_FIELD_LENGTHS_204, lengthsBuffer);
				short[] sizes = new short[16];
				int size = 0;
				lengthsBuffer.position(0);
				for (int i = 0; i < 16; ++i) {
					sizes[i]= lengthsBuffer.getShort();
					size += sizes[i];
				}
				ByteBuffer buffer = ByteBuffer.allocate(size * 2);
				PipeReader.readBytes(input, JPGSchema.MSG_HUFFMANTABLEMESSAGE_4_FIELD_TABLE_304, buffer);
				PipeReader.releaseReadLock(input);
				buffer.position(0);
				for (int i = 0; i < 16; ++i) {
					table.symbols.add(new ArrayList<Short>());
					for (int j = 0; j < sizes[i]; ++j) {
						table.symbols.get(i).add(buffer.getShort());
					}
				}
				if (table.tableID > 1) {
					table.tableID -= 2;
					header.huffmanACTables.add(table);
				}
				else {
					header.huffmanDCTables.add(table);
				}
			}
			else if (msgIdx == JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5) {
				// read quantization table from pipe
				QuantizationTable table = new QuantizationTable();
				table.tableID = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLEID_105);
				table.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_PRECISION_205);
				ByteBuffer buffer = ByteBuffer.allocate(64 * 4);
				PipeReader.readBytes(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLE_305, buffer);
				PipeReader.releaseReadLock(input);
				buffer.position(0);
				for (int i = 0; i < 64; ++i) {
					table.table[i] = buffer.getInt();
				}
				header.quantizationTables.add(table);
				
				// write quantization table to pipe
				System.out.println("Attempting to write quantization table to pipe...");
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5)) {
					System.out.println("Writing quantization table to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLEID_105, table.tableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_PRECISION_205, table.precision);
					buffer.position(0);
					PipeWriter.writeBytes(output, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLE_305, buffer);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Huffman requesting shutdown");
					requestShutdown();
				}
			}
			else {
				System.err.println("Huffman requesting shutdown");
				requestShutdown();
			}
		}
	}/*
	
	/*public static void main(String[] args) throws Exception {
		// test BitReader
		ArrayList<Short> data = new ArrayList<Short>();
		data.add((short)5);
		data.add((short)10);
		data.add((short)15);
		
		BitReader b = new BitReader(data);
		for (int i = 0; i < 25; ++i) {
			System.out.println(b.nextBit());
		}
		
		System.out.println();
		
		// test generateCodes
		HuffmanTable table = new HuffmanTable();
		table.tableID = 0;
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
		ArrayList<ArrayList<Integer>> codes = generateCodes(table);
		for (int i = 0; i < codes.size(); ++i) {
			System.out.print((i + 1) + ": ");
			for (int j = 0; j < codes.get(i).size(); ++j) {
				System.out.print(codes.get(i).get(j) + " ");
			}
			System.out.println();
		}
		
		// test decodeHuffmanData
		Header header = JPGScanner.ReadJPG("test_jpgs/huff_simple0.jpg");
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
	}*/
}
