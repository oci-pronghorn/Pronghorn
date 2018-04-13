package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPGSchema;
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

public class BMPScanner extends PronghornStage {

	private ArrayList<String> inputFiles = new ArrayList<String>();
	private final Pipe<JPGSchema> output;
	boolean verbose;
	public static long timer = 0;
	
	int mcuWidth = 0;
	int mcuHeight = 0;
	int numMCUs = 0;
	int numProcessed = 0;
	int paddingSize = 0;
	
	Header header;
	MCU mcu = new MCU();
	
	short[][] pixels;
	
	public BMPScanner(GraphManager graphManager, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, NONE, output);
		this.output = output;
		this.verbose = verbose;
	}
	
	public Header ReadBMP(String filename) throws IOException {
		header = new Header();
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
		
		if ((b.get() & 0xFF) != 'B' || (b.get() & 0xFF) != 'M') {
			System.err.println("Error - not a BMP file");
			header.valid = false;
			f.close();b.clear();
			return header;
		}
		
		int offset, dibSize, planes, depth, compr = 0;
		
		// file size
		b.get();
		b.get();
		b.get();
		b.get();
		// nothing
		b.get();
		b.get();
		b.get();
		b.get();
		offset  = (b.get() & 0xFF); // hopefully 26 or 54 (or more)
		offset |= (b.get() & 0xFF) << 8;
		offset |= (b.get() & 0xFF) << 16;
		offset |= (b.get() & 0xFF) << 24;
		
		dibSize  = (b.get() & 0xFF); // hopefully 12 or 40
		dibSize |= (b.get() & 0xFF) << 8;
		dibSize |= (b.get() & 0xFF) << 16;
		dibSize |= (b.get() & 0xFF) << 24;
		if (dibSize == 12) {
			header.width   = (b.get() & 0xFF);
			header.width  |= (b.get() & 0xFF) << 8;
			header.height  = (b.get() & 0xFF);
			header.height |= (b.get() & 0xFF) << 8;
			planes         = (b.get() & 0xFF);
			planes        |= (b.get() & 0xFF) << 8;
			depth          = (b.get() & 0xFF);
			depth         |= (b.get() & 0xFF) << 8;
			
			offset -= 26;
		}
		else if (dibSize == 40) {
			header.width   = (b.get() & 0xFF);
			header.width  |= (b.get() & 0xFF) << 8;
			header.width  |= (b.get() & 0xFF) << 16;
			header.width  |= (b.get() & 0xFF) << 24;
			header.height  = (b.get() & 0xFF);
			header.height |= (b.get() & 0xFF) << 8;
			header.height |= (b.get() & 0xFF) << 16;
			header.height |= (b.get() & 0xFF) << 24;
			planes         = (b.get() & 0xFF);
			planes        |= (b.get() & 0xFF) << 8;
			depth          = (b.get() & 0xFF);
			depth         |= (b.get() & 0xFF) << 8;
			compr          = (b.get() & 0xFF);
			compr         |= (b.get() & 0xFF) << 8;
			compr         |= (b.get() & 0xFF) << 16;
			compr         |= (b.get() & 0xFF) << 24;
			
			// image size
			b.get();
			b.get();
			b.get();
			b.get();
			// horizontal resolution
			b.get();
			b.get();
			b.get();
			b.get();
			// vertical resolution
			b.get();
			b.get();
			b.get();
			b.get();
			// color palette size
			b.get();
			b.get();
			b.get();
			b.get();
			// important colors
			b.get();
			b.get();
			b.get();
			b.get();
			
			offset -= 54;
		}
		else {
			System.err.println("Error - DIB Header not supported");
			header.valid = false;
			f.close();b.clear();
			return null;
		}
		
		if (planes != 1) {
			System.err.println("Error - Number of color planes must be 1");
			header.valid = false;
			f.close();b.clear();
			return null;
		}
		if (depth != 24) {
			System.err.println("Error - Only 24bpp color depth supported");
			header.valid = false;
			f.close();b.clear();
			return null;
		}
		if (compr != 0) {
			System.err.println("Error - BMP compression not supported");
			f.close();b.clear();
			return null;
		}
		if (offset < 0) {
			System.err.println("Error - Invalid offset");
			f.close();b.clear();
			return null;
		}
		
		while (offset > 0) {
			b.get();
			--offset;
		}
		
		// begin reading pixels...
		mcuWidth = (header.width + 7) / 8;
		mcuHeight = (header.height + 7) / 8;
		numMCUs = mcuWidth * mcuHeight;
		numProcessed = 0;
		paddingSize = (4 - (header.width * 3) % 4) % 4;
		pixels = new short[mcuHeight * 8][mcuWidth * 8 * 3];

		int trailingRows = 8 - (mcuHeight * 8 - header.height);
		
		readMCURow(b, trailingRows, (mcuHeight - 1) * 8);
		
		for (int i = mcuHeight - 2; i >= 0; --i) {
			readMCURow(b, 8, i * 8);
		}
		
		f.close();b.clear();
		return header;
	}
	
	private void readMCURow(ByteBuffer b, int numRows, int startRow) throws IOException {
		for (int i = numRows - 1; i >= 0; --i) {
			for (int j = 0; j < header.width; ++j) {
				pixels[i + startRow][j * 3 + 2] = (short) (b.get() & 0xFF);
				pixels[i + startRow][j * 3 + 1] = (short) (b.get() & 0xFF);
				pixels[i + startRow][j * 3 + 0] = (short) (b.get() & 0xFF);
			}
			for (int j = 0; j < paddingSize; j++) {
				b.get();
			}
		}
	}
	
	private void fillMCU(int index) {
		int row = numProcessed / mcuWidth * 8;
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				mcu.y [i * 8 + j] = pixels[i + row][index * 24 + j * 3 + 0];
				mcu.cb[i * 8 + j] = pixels[i + row][index * 24 + j * 3 + 1];
				mcu.cr[i * 8 + j] = pixels[i + row][index * 24 + j * 3 + 2];
			}
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
		}
		else {
			System.err.println("BMP Scanner requesting shutdown");
			requestShutdown();
		}
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeWriter.hasRoomForWrite(output) && numProcessed < numMCUs) {
			fillMCU(numProcessed % mcuWidth);
			//JPG.printMCU(mcu);
			sendMCU(mcu);
		}
		if (PipeWriter.hasRoomForWrite(output) && !inputFiles.isEmpty()) {
			String file = inputFiles.get(0);
			inputFiles.remove(0);
			System.out.println(file);
			try {
				header = ReadBMP(file);
				if (header == null || !header.valid) {
					System.err.println("Error - BMP file '" + file + "' invalid");
					if (inputFiles.size() > 0) {
						return;
					}
					header = new Header();
					header.width = 0;
					header.height = 0;
					header.valid = false;
				}
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					// write header to pipe
					if (verbose) 
						System.out.println("BMP Scanner writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, file);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, (inputFiles.size() == 0 ? 1 : 0));
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("BMP Scanner requesting shutdown");
					requestShutdown();
				}
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
}
