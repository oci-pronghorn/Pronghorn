package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.jpgRaster.JPGConstants;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class JPGDumper {

	public static void Dump(ArrayList<Byte> data, Header header) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(header.filename);
		FileChannel file = fileStream.getChannel();
		int size = 277; // 2 + 18 + 69 + 69 + 19 + 21 + 21 + 21 + 21 + 14 + 2
		size += JPG.hDCTable0Symbols.length;
		size += JPG.hDCTable1Symbols.length;
		size += JPG.hACTable0Symbols.length;
		size += JPG.hACTable1Symbols.length;
		size += data.size();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		
		// start of image
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.SOI);
		
		// write jfif app0 marker
		WriteAPP0(buffer);
		
		// write quantization tables
		WriteQuantizationTable(buffer, JPG.qTable0);
		WriteQuantizationTable(buffer, JPG.qTable1);
		
		// write start of frame
		WriteStartOfFrame(buffer, header);
		
		// write huffman tables
		WriteHuffmanTable(buffer, JPG.hDCTable0Lengths, JPG.hDCTable0Symbols, 0, 0);
		WriteHuffmanTable(buffer, JPG.hDCTable1Lengths, JPG.hDCTable1Symbols, 0, 1);
		WriteHuffmanTable(buffer, JPG.hACTable0Lengths, JPG.hACTable0Symbols, 1, 0);
		WriteHuffmanTable(buffer, JPG.hACTable1Lengths, JPG.hACTable1Symbols, 1, 1);
		
		// write start of scan
		WriteStartOfScan(buffer);

		// write huffman coded data
		for (int i = 0; i < data.size(); ++i) {
			buffer.put((byte)data.get(i));
		}
		
		// end of image
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.EOI);
		
		buffer.flip();
		while(buffer.hasRemaining()) {
			file.write(buffer);
		}
		file.close();
		fileStream.close();
	}
	
	private static void WriteAPP0(ByteBuffer buffer) {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.APP0);
		buffer.putShort((short)16);
		buffer.put((byte)'J');
		buffer.put((byte)'F');
		buffer.put((byte)'I');
		buffer.put((byte)'F');
		buffer.put((byte)0);
		buffer.put((byte)1);
		buffer.put((byte)2);
		buffer.put((byte)0);
		buffer.putShort((short)100);
		buffer.putShort((short)100);
		buffer.put((byte)0);
		buffer.put((byte)0);
	}
	
	private static void WriteQuantizationTable(ByteBuffer buffer, QuantizationTable qtable) {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.DQT);
		buffer.putShort((short)67);
		buffer.put((byte)(qtable.tableID));
		for (int i = 0; i < 64; ++i) {
			buffer.put((byte)qtable.table[i]);
		}
	}
	
	private static void WriteStartOfFrame(ByteBuffer buffer, Header header) {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.SOF0);
		buffer.putShort((short)17);
		buffer.put((byte)8);
		buffer.putShort((short)header.height);
		buffer.putShort((short)header.width);
		buffer.put((byte)3);
		for (int i = 1; i <= 3; ++i) {
			buffer.put((byte)i);
			buffer.put((byte)0x11);
			buffer.put((byte)( i == 1 ? 0 : 1));
		}
	}

	private static void WriteHuffmanTable(ByteBuffer buffer, short[] lengths, short[] symbols, int acdc, int id) {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.DHT);
		buffer.putShort((short)(19 + symbols.length));
		buffer.put((byte)(acdc << 4 | id));
		for (int i = 0; i < lengths.length; ++i) {
			buffer.put((byte)lengths[i]);
		}
		for (int i = 0; i < symbols.length; ++i) {
			buffer.put((byte)symbols[i]);
		}
	}
	
	private static void WriteStartOfScan(ByteBuffer buffer) {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.SOS);
		buffer.putShort((short)12);
		buffer.put((byte)3);
		for (int i = 1; i <= 3; ++i) {
			buffer.put((byte)i);
			buffer.put((byte)( i == 1 ? 0x00 : 0x11));
		}
		buffer.put((byte)0);
		buffer.put((byte)63);
		buffer.put((byte)0);
	}
	
	public static void main(String[] args) throws IOException {
		ArrayList<Byte> data = new ArrayList<Byte>();
		data.add((byte)0b11111011);
		Header header = new Header();
		header.height = 8;
		header.width = 8;
		header.filename = "simple_test.jpg";
		Dump(data, header);
	}
}
