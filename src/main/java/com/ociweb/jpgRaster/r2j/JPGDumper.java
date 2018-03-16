package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPGConstants;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class JPGDumper extends PronghornStage {

	private final Pipe<JPGSchema> input;
	boolean verbose;
	
	Header header;
	MCU mcu = new MCU();
	
	ArrayList<Byte> scanData;
	int count;
	int mcuHeight;
	int mcuWidth;
	int numMCUs;
	
	public JPGDumper(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
	}

	public static void Dump(ArrayList<Byte> data, String filename) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(filename);
		FileChannel file = fileStream.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(2);
		
		buffer.put((byte) 0xFF);
		buffer.put((byte) JPGConstants.SOI);
		
		buffer.flip();
		while(buffer.hasRemaining()) {
			file.write(buffer);
		}
		file.close();
		fileStream.close();
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

	@Override
	public void run() {
		
		while (PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				PipeReader.releaseReadLock(input);

				scanData = new ArrayList<Byte>();
				count = 0;
				mcuHeight = (header.height + 7) / 8;
				mcuWidth = (header.width + 7) / 8;
				numMCUs = mcuHeight * mcuWidth;
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_4) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cb[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cr[i] = mcuReader.readShort();
				}
				PipeReader.releaseReadLock(input);

				count += 1;
				
				if (count >= numMCUs) {
					try {
						int extension = header.filename.lastIndexOf('.');
						if (extension == -1) {
							header.filename += ".bmp";
						}
						else {
							header.filename = header.filename.substring(0, extension) + ".bmp";
						}
						if (verbose) 
							System.out.println("Writing to '" + header.filename + "'...");
						Dump(scanData, header.filename);
						if (verbose) 
							System.out.println("Done.");
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				System.err.println("BMPDumper requesting shutdown");
				requestShutdown();
			}
		}
	
	}
}
