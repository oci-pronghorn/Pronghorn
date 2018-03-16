package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RGBToYCbCr extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	boolean verbose;
	
	Header header;
	MCU mcu = new MCU();
	static byte[] rgb = new byte[3];
	
	public RGBToYCbCr(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}

	private static byte[] convertToRGB(short Y, short Cb, short Cr) {
		short r, g, b;
		r = (short)((double)Y + 1.402 * ((double)Cr) + 128);
		g = (short)(((double)(Y) - (0.114 * (Y + 1.772 * (double)Cb)) - 0.299 * (Y + 1.402 * ((double)Cr))) / 0.587 + 128);
		b = (short)((double)Y + 1.772 * ((double)Cb) + 128);
		if (r < 0)   r = 0;
		if (r > 255) r = 255;
		if (g < 0)   g = 0;
		if (g > 255) g = 255;
		if (b < 0)   b = 0;
		if (b > 255) b = 255;
		rgb[0] = (byte)r;
		rgb[1] = (byte)g;
		rgb[2] = (byte)b;
		//System.out.println("(" + Y + ", " + Cb + ", " + Cr + ") -> (" + rgb[0] + ", " + rgb[1] + ", " + rgb[2] + ")");
		return rgb;
	}
	
	public static void convertYCbCrToRGB(MCU mcu) {
		for (int i = 0; i < 64; ++i) {
			byte[] rgb = convertToRGB(mcu.y[i], mcu.cb[i], mcu.cr[i]);
			mcu.y[i] = rgb[0];
			mcu.cb[i] = rgb[1];
			mcu.cr[i] = rgb[2];
		}
		return;
	}

	@Override
	public void run() {
		while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					if (verbose) 
						System.out.println("RGBToYCbCr writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("RGBToYCbCr requesting shutdown");
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

				convertYCbCrToRGB(mcu);
				
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
					System.err.println("RGBToYCbCr requesting shutdown");
					requestShutdown();
				}
			}
			else {
				System.err.println("RGBToYCbCr requesting shutdown");
				requestShutdown();
			}
		}
	}
}
