package com.ociweb.jpgRaster.j2r;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class YCbCrToRGB extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	boolean verbose;
	
	int mcuWidth = 0;
	int mcuHeight = 0;
	int numProcessed = 0;
	int aboutToSend = 0;
	
	Header header;
	MCU mcu1 = new MCU();
	MCU mcu2 = new MCU();
	MCU mcu3 = new MCU();
	MCU mcu4 = new MCU();
	static short[] tempCB = new short[64];
	static short[] tempCR = new short[64];
	int count = 0;
	static byte[] rgb = new byte[3];
	
	public YCbCrToRGB(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}

	private static void convertToRGB(short Y, short Cb, short Cr) {
		short r, g, b;
		r = (short)(Y + 1.402 * Cr + 128);
		g = (short)((Y - (0.114 * (Y + 1.772 * Cb)) - 0.299 * (Y + 1.402 * Cr)) / 0.587 + 128);
		b = (short)(Y + 1.772 * Cb + 128);
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
	}
	
	public static void convertYCbCrToRGB(MCU mcu) {
		for (int i = 0; i < 64; ++i) {
			convertToRGB(mcu.y[i], mcu.cb[i], mcu.cr[i]);
			mcu.y[i] = rgb[0];
			mcu.cb[i] = rgb[1];
			mcu.cr[i] = rgb[2];
		}
		return;
	}
	
	public static void expandColumns(MCU mcu, MCU mcu2) {
		for (int i = 0; i < 64; ++i) {
			tempCB[i] = mcu.cb[i];
			tempCR[i] = mcu.cr[i];
		}
		
		for (int i = 0; i < 8; ++i) {
			for (int j = 3; j >= 0; --j) {
				mcu.cb [(j * 2    ) + i * 8] = tempCB[j + i * 8];
				mcu.cb [(j * 2 + 1) + i * 8] = tempCB[j + i * 8];
				
				mcu.cr [(j * 2    ) + i * 8] = tempCR[j + i * 8];
				mcu.cr [(j * 2 + 1) + i * 8] = tempCR[j + i * 8];
				
				
				mcu2.cb[(j * 2    ) + i * 8] = tempCB[(j + 4) + i * 8];
				mcu2.cb[(j * 2 + 1) + i * 8] = tempCB[(j + 4) + i * 8];
				
				mcu2.cr[(j * 2    ) + i * 8] = tempCR[(j + 4) + i * 8];
				mcu2.cr[(j * 2 + 1) + i * 8] = tempCR[(j + 4) + i * 8];
			}
		}
	}
	
	public static void expandRows(MCU mcu, MCU mcu2) {
		for (int i = 0; i < 64; ++i) {
			tempCB[i] = mcu.cb[i];
			tempCR[i] = mcu.cr[i];
		}
		
		for (int i = 3; i >= 0; --i) {
			for (int j = 0; j < 8; ++j) {
				mcu.cb [j + (i * 2    ) * 8] = tempCB[j + i * 8];
				mcu.cb [j + (i * 2 + 1) * 8] = tempCB[j + i * 8];

				mcu.cr [j + (i * 2    ) * 8] = tempCR[j + i * 8];
				mcu.cr [j + (i * 2 + 1) * 8] = tempCR[j + i * 8];
				

				mcu2.cb[j + (i * 2    ) * 8] = tempCB[j + (i + 4) * 8];
				mcu2.cb[j + (i * 2 + 1) * 8] = tempCB[j + (i + 4) * 8];

				mcu2.cr[j + (i * 2    ) * 8] = tempCR[j + (i + 4) * 8];
				mcu2.cr[j + (i * 2 + 1) * 8] = tempCR[j + (i + 4) * 8];
			}
		}
	}
	
	public static void expandColumnsAndRows(MCU mcu1, MCU mcu2, MCU mcu3, MCU mcu4) {
		for (int i = 0; i < 64; ++i) {
			tempCB[i] = mcu1.cb[i];
			tempCR[i] = mcu1.cr[i];
		}
		
		for (int i = 3; i >= 0; --i) {
			for (int j = 3; j >= 0; --j) {
				mcu1.cb[(j * 2    ) + (i * 2    ) * 8] = tempCB[(j    ) + (i    ) * 8];
				mcu1.cb[(j * 2 + 1) + (i * 2    ) * 8] = tempCB[(j    ) + (i    ) * 8];
				mcu1.cb[(j * 2    ) + (i * 2 + 1) * 8] = tempCB[(j    ) + (i    ) * 8];
				mcu1.cb[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCB[(j    ) + (i    ) * 8];
				
				mcu1.cr[(j * 2    ) + (i * 2    ) * 8] = tempCR[(j    ) + (i    ) * 8];
				mcu1.cr[(j * 2 + 1) + (i * 2    ) * 8] = tempCR[(j    ) + (i    ) * 8];
				mcu1.cr[(j * 2    ) + (i * 2 + 1) * 8] = tempCR[(j    ) + (i    ) * 8];
				mcu1.cr[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCR[(j    ) + (i    ) * 8];
				
				
				mcu2.cb[(j * 2    ) + (i * 2    ) * 8] = tempCB[(j    ) + (i + 4) * 8];
				mcu2.cb[(j * 2 + 1) + (i * 2    ) * 8] = tempCB[(j    ) + (i + 4) * 8];
				mcu2.cb[(j * 2    ) + (i * 2 + 1) * 8] = tempCB[(j    ) + (i + 4) * 8];
				mcu2.cb[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCB[(j    ) + (i + 4) * 8];
				
				mcu2.cr[(j * 2    ) + (i * 2    ) * 8] = tempCR[(j    ) + (i + 4) * 8];
				mcu2.cr[(j * 2 + 1) + (i * 2    ) * 8] = tempCR[(j    ) + (i + 4) * 8];
				mcu2.cr[(j * 2    ) + (i * 2 + 1) * 8] = tempCR[(j    ) + (i + 4) * 8];
				mcu2.cr[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCR[(j    ) + (i + 4) * 8];
				
				
				mcu3.cb[(j * 2    ) + (i * 2    ) * 8] = tempCB[(j + 4) + (i    ) * 8];
				mcu3.cb[(j * 2 + 1) + (i * 2    ) * 8] = tempCB[(j + 4) + (i    ) * 8];
				mcu3.cb[(j * 2    ) + (i * 2 + 1) * 8] = tempCB[(j + 4) + (i    ) * 8];
				mcu3.cb[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCB[(j + 4) + (i    ) * 8];
				
				mcu3.cr[(j * 2    ) + (i * 2    ) * 8] = tempCR[(j + 4) + (i    ) * 8];
				mcu3.cr[(j * 2 + 1) + (i * 2    ) * 8] = tempCR[(j + 4) + (i    ) * 8];
				mcu3.cr[(j * 2    ) + (i * 2 + 1) * 8] = tempCR[(j + 4) + (i    ) * 8];
				mcu3.cr[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCR[(j + 4) + (i    ) * 8];
				
				
				mcu4.cb[(j * 2    ) + (i * 2    ) * 8] = tempCB[(j + 4) + (i + 4) * 8];
				mcu4.cb[(j * 2 + 1) + (i * 2    ) * 8] = tempCB[(j + 4) + (i + 4) * 8];
				mcu4.cb[(j * 2    ) + (i * 2 + 1) * 8] = tempCB[(j + 4) + (i + 4) * 8];
				mcu4.cb[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCB[(j + 4) + (i + 4) * 8];
				
				mcu4.cr[(j * 2    ) + (i * 2    ) * 8] = tempCR[(j + 4) + (i + 4) * 8];
				mcu4.cr[(j * 2 + 1) + (i * 2    ) * 8] = tempCR[(j + 4) + (i + 4) * 8];
				mcu4.cr[(j * 2    ) + (i * 2 + 1) * 8] = tempCR[(j + 4) + (i + 4) * 8];
				mcu4.cr[(j * 2 + 1) + (i * 2 + 1) * 8] = tempCR[(j + 4) + (i + 4) * 8];
			}
		}
	}
	
	public void sendMCU(MCU emcu) {
		if (header.colorComponents[0].horizontalSamplingFactor == 2 &&
			header.width % 8 == 0 && (header.width / 8) % 2 == 1 &&
			numProcessed % mcuWidth == mcuWidth - 1) {
			numProcessed += 1;
			return;
		}
		if (header.colorComponents[0].verticalSamplingFactor == 2 &&
			header.height % 8 == 0 && (header.height / 8) % 2 == 1 &&
			numProcessed >= mcuWidth * (mcuHeight - 2) + 1 &&
			numProcessed % 2 == 1) {
			numProcessed += 1;
			return;
		}
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
			System.err.println("YCbCrToRGB requesting shutdown");
			requestShutdown();
		}
	}

	@Override
	public void run() {
		if (PipeWriter.hasRoomForWrite(output) && aboutToSend != 0) {
			int horizontal = header.colorComponents[0].horizontalSamplingFactor;
			int vertical = header.colorComponents[0].verticalSamplingFactor;
			if (aboutToSend != 0) {
				if (aboutToSend == 2) {
					sendMCU(mcu2);
					if (horizontal == 2 && vertical == 2) {
						aboutToSend = 3;
					}
					else {
						aboutToSend = 0;
					}
				}
				else if (aboutToSend == 3) {
					sendMCU(mcu3);
					if (horizontal == 2 && vertical == 2) {
						aboutToSend = 4;
					}
					else {
						aboutToSend = 0;
					}
				}
				else { // if (aboutToSend == 4) {
					sendMCU(mcu4);
					aboutToSend = 0;
				}
			}
		}
		
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
						System.out.println("YCbCrToRGB writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, last);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("YCbCrToRGB requesting shutdown");
					requestShutdown();
				}
				
				mcuWidth = (header.width + 7) / 8;
				mcuHeight = (header.height + 7) / 8;
				numProcessed = 0;
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				header.colorComponents[component.componentID - 1] = component;
				header.numComponents += 1;
				PipeReader.releaseReadLock(input);
				
				// write color component data to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					if (verbose) 
						System.out.println("YCbCrToRGB writing color component to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("YCbCrToRGB requesting shutdown");
					requestShutdown();
				}
				
				if (component.componentID == 1 && component.horizontalSamplingFactor == 2 &&
					((header.width - 1) / 8 + 1) % 2 == 1) {
					mcuWidth += 1;
				}
				if (component.componentID == 1 && component.verticalSamplingFactor == 2 &&
					((header.height - 1) / 8 + 1) % 2 == 1) {
					mcuHeight += 1;
				}
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_4) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
				if (count == 0) {
					for (int i = 0; i < 64; ++i) {
						mcu1.y[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu1.cb[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu1.cr[i] = mcuReader.readShort();
					}
					
					count = 1;
					if (header.colorComponents[0].horizontalSamplingFactor == 1 &&
						header.colorComponents[0].verticalSamplingFactor == 2) {
						count = 5;
					}
				}
				else if (count == 1){
					for (int i = 0; i < 64; ++i) {
						mcu2.y[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu2.cb[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu2.cr[i] = mcuReader.readShort();
					}
					
					count = 2;
					if (header.colorComponents[0].verticalSamplingFactor == 2) {
						count = 5;
					}
				}
				else if (count == 5){
					for (int i = 0; i < 64; ++i) {
						mcu3.y[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu3.cb[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu3.cr[i] = mcuReader.readShort();
					}
					
					count = 2;
					if (header.colorComponents[0].horizontalSamplingFactor == 2) {
						count = 3;
					}
				}
				else if (count == 3){
					for (int i = 0; i < 64; ++i) {
						mcu4.y[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu4.cb[i] = mcuReader.readShort();
					}
					
					for (int i = 0; i < 64; ++i) {
						mcu4.cr[i] = mcuReader.readShort();
					}
					
					count = 4;
				}
				PipeReader.releaseReadLock(input);

				if (count == header.colorComponents[0].horizontalSamplingFactor * header.colorComponents[0].verticalSamplingFactor) {
					if (header.colorComponents[0].horizontalSamplingFactor == 2 &&
						header.colorComponents[0].verticalSamplingFactor == 2) {
						expandColumnsAndRows(mcu1, mcu2, mcu3, mcu4);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu2);
						convertYCbCrToRGB(mcu3);
						convertYCbCrToRGB(mcu4);
						sendMCU(mcu1);
						aboutToSend = 2;
						if (PipeWriter.hasRoomForWrite(output)) {
							sendMCU(mcu2);
							aboutToSend = 3;
							if (PipeWriter.hasRoomForWrite(output)) {
								sendMCU(mcu3);
								aboutToSend = 4;
								if (PipeWriter.hasRoomForWrite(output)) {
									sendMCU(mcu4);
									aboutToSend = 0;
								}
							}
						}
					}
					else if (header.colorComponents[0].horizontalSamplingFactor == 2) {
						expandColumns(mcu1, mcu2);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu2);
						sendMCU(mcu1);
						aboutToSend = 2;
						if (PipeWriter.hasRoomForWrite(output)) {
							sendMCU(mcu2);
							aboutToSend = 0;
						}
					}
					else if (header.colorComponents[0].verticalSamplingFactor == 2) {
						expandRows(mcu1, mcu3);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu3);
						sendMCU(mcu1);
						aboutToSend = 3;
						if (PipeWriter.hasRoomForWrite(output)) {
							sendMCU(mcu3);
							aboutToSend = 0;
						}
					}
					else {
						convertYCbCrToRGB(mcu1);
						sendMCU(mcu1);
						aboutToSend = 0;
					}
					
					count = 0;
				}
			}
			else {
				System.err.println("YCbCrToRGB requesting shutdown");
				requestShutdown();
			}
		}
	}
}
