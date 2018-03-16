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
	
	Header header;
	MCU mcu = new MCU();
	
	public BMPScanner(GraphManager graphManager, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, NONE, output);
		this.output = output;
		this.verbose = verbose;
	}
	
	public Header ReadBMP(String filename) throws IOException {
		Header header = new Header();
		header.filename = filename;
		DataInputStream f = new DataInputStream(new FileInputStream(filename));
		
		f.close();
		return header;
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
			// get next mcu
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
				
				mcuWidth = (header.width + 7) / 8;
				mcuHeight = (header.height + 7) / 8;
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
	}
}
