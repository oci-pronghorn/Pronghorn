package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class Quantizer extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	boolean verbose;
	public static long timer = 0;
	int quality;
	
	Header header;
	MCU mcu = new MCU();
	
	public Quantizer(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose, int quality) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.quality = quality;
	}
	
	private static void quantizeMCU(short[] MCU, QuantizationTable table) {
		for (int i = 0; i < MCU.length; ++i) {
			MCU[JPG.zigZagMap[i]] = (short)(MCU[JPG.zigZagMap[i]] / table.table[i]);
		}
	}
	
	public static void quantize(MCU mcu, int quality) {
		if (quality == 50) {
			quantizeMCU(mcu.y, JPG.qTable0_50);
			quantizeMCU(mcu.cb, JPG.qTable1_50);
			quantizeMCU(mcu.cr, JPG.qTable1_50);
		}
		else if (quality == 75) {
			quantizeMCU(mcu.y, JPG.qTable0_75);
			quantizeMCU(mcu.cb, JPG.qTable1_75);
			quantizeMCU(mcu.cr, JPG.qTable1_75);
		}
		else {
			quantizeMCU(mcu.y, JPG.qTable0_100);
			quantizeMCU(mcu.cb, JPG.qTable1_100);
			quantizeMCU(mcu.cr, JPG.qTable1_100);
		}
		return;
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				int last = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					if (verbose) 
						System.out.println("Quantizer writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, last);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Quantizer requesting shutdown");
					requestShutdown();
				}
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
				
				quantize(mcu, quality);
				//JPG.printMCU(mcu);

				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_4)) {
					DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.y[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cb[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CB_204);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cr[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CR_304);
					
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else {
				System.err.println("Quantizer requesting shutdown");
				requestShutdown();
			}
		}
		timer += (System.nanoTime() - s);
	}
}
