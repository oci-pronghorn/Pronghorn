package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPGConstants;
import com.ociweb.jpgRaster.JPG;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class JPGDumper {
	boolean verbose;
	
	Header header;
	MCU mcu = new MCU();

	public static void Dump(ArrayList<Byte> data, String filename) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(filename);
		FileChannel file = fileStream.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(2);
		
		putHeader(buffer);
		
		buffer.flip();
		while(buffer.hasRemaining()) {
			file.write(buffer);
		}
		file.close();
		fileStream.close();
	}
	
	private static void putHeader(ByteBuffer buffer) throws IOException {
		buffer.put((byte)0xFF);
		buffer.put((byte)JPGConstants.SOI);
		
		//... Add the rest of the header in here
	}
	
	private static void putInt(ByteBuffer buffer, int v) throws IOException {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
		buffer.put((byte)((v >> 16) & 0xFF));
		buffer.put((byte)((v >> 24) & 0xFF));
	}
	
	private static void putShort(ByteBuffer buffer, int v) throws IOException {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
	}
}
