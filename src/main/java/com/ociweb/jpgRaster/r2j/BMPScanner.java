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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class BMPScanner extends PronghornStage {

	private ArrayList<String> inputFiles = new ArrayList<String>();
	private final Pipe<JPGSchema> output;
	boolean verbose;
	
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
		DataInputStream f = new DataInputStream(new FileInputStream(filename));
		
		if (f.readUnsignedByte() != 'B' || f.readUnsignedByte() != 'M') {
			System.err.println("Error - not a BMP file");
			header.valid = false;
			f.close();
			return header;
		}
		
		int offset, dibSize, planes, depth, compr = 0;
		
		// file size
		f.readUnsignedByte();
		f.readUnsignedByte();
		f.readUnsignedByte();
		f.readUnsignedByte();
		// nothing
		f.readUnsignedByte();
		f.readUnsignedByte();
		f.readUnsignedByte();
		f.readUnsignedByte();
		offset  = f.readUnsignedByte(); // hopefully 26 or 54 (or more)
		offset |= f.readUnsignedByte() << 8;
		offset |= f.readUnsignedByte() << 16;
		offset |= f.readUnsignedByte() << 24;
		
		dibSize  = f.readUnsignedByte(); // hopefully 12 or 40
		dibSize |= f.readUnsignedByte() << 8;
		dibSize |= f.readUnsignedByte() << 16;
		dibSize |= f.readUnsignedByte() << 24;
		if (dibSize == 12) {
			header.width   = f.readUnsignedByte();
			header.width  |= f.readUnsignedByte() << 8;
			header.height  = f.readUnsignedByte();
			header.height |= f.readUnsignedByte() << 8;
			planes         = f.readUnsignedByte();
			planes        |= f.readUnsignedByte() << 8;
			depth          = f.readUnsignedByte();
			depth         |= f.readUnsignedByte() << 8;
			
			offset -= 26;
		}
		else if (dibSize == 40) {
			header.width   = f.readUnsignedByte();
			header.width  |= f.readUnsignedByte() << 8;
			header.width  |= f.readUnsignedByte() << 16;
			header.width  |= f.readUnsignedByte() << 24;
			header.height  = f.readUnsignedByte();
			header.height |= f.readUnsignedByte() << 8;
			header.height |= f.readUnsignedByte() << 16;
			header.height |= f.readUnsignedByte() << 24;
			planes         = f.readUnsignedByte();
			planes        |= f.readUnsignedByte() << 8;
			depth          = f.readUnsignedByte();
			depth         |= f.readUnsignedByte() << 8;
			compr          = f.readUnsignedByte();
			compr         |= f.readUnsignedByte() << 8;
			compr         |= f.readUnsignedByte() << 16;
			compr         |= f.readUnsignedByte() << 24;
			
			// image size
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			// horizontal resolution
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			// vertical resolution
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			// color palette size
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			// important colors
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			f.readUnsignedByte();
			
			offset -= 54;
		}
		else {
			System.err.println("Error - DIB Header not supported");
			header.valid = false;
			f.close();
			return null;
		}
		
		if (planes != 1) {
			System.err.println("Error - Number of color planes must be 1");
			header.valid = false;
			f.close();
			return null;
		}
		if (depth != 24) {
			System.err.println("Error - Only 24bpp color depth supported");
			header.valid = false;
			f.close();
			return null;
		}
		if (compr != 0) {
			System.err.println("Error - BMP compression not supported");
			f.close();
			return null;
		}
		if (offset < 0) {
			System.err.println("Error - Invalid offset");
			f.close();
			return null;
		}
		
		while (offset > 0) {
			f.readUnsignedByte();
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
		
		readMCURow(f, trailingRows, (mcuHeight - 1) * 8);
		
		for (int i = mcuHeight - 2; i >= 0; --i) {
			readMCURow(f, 8, i * 8);
		}
		
		f.close();
		return header;
	}
	
	private void readMCURow(DataInputStream f, int numRows, int startRow) throws IOException {
		for (int i = numRows - 1; i >= 0; --i) {
			for (int j = 0; j < header.width; ++j) {
				pixels[i + startRow][j * 3 + 2] = (short) f.readUnsignedByte();
				pixels[i + startRow][j * 3 + 1] = (short) f.readUnsignedByte();
				pixels[i + startRow][j * 3 + 0] = (short) f.readUnsignedByte();
			}
			for (int j = 0; j < paddingSize; j++) {
				f.readUnsignedByte();
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
					return;
				}
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					// write header to pipe
					if (verbose) 
						System.out.println("BMP Scanner writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, file);
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
	}
}
