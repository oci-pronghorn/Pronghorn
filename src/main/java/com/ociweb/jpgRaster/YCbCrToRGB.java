package com.ociweb.jpgRaster;

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
	
	int mcuWidth = 0;
	int mcuHeight = 0;
	int numProcessed = 0;
	
	Header header;
	MCU mcu1 = new MCU();
	MCU mcu2 = new MCU();
	MCU mcu3 = new MCU();
	MCU mcu4 = new MCU();
	static short[] tempCB = new short[64];
	static short[] tempCR = new short[64];
	int count = 0;
	static byte[] rgb = new byte[3];
	
	protected YCbCrToRGB(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
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
		if (header.colorComponents.get(0).horizontalSamplingFactor == 2 &&
			header.width % 8 == 0 && (header.width / 8) % 2 == 1 &&
			numProcessed % mcuWidth == mcuWidth - 1) {
			numProcessed += 1;
			return;
		}
		if (header.colorComponents.get(0).verticalSamplingFactor == 2 &&
			header.height % 8 == 0 && (header.height / 8) % 2 == 1 &&
			numProcessed >= mcuWidth * (mcuHeight - 2) + 1 &&
			numProcessed % 2 == 1) {
			numProcessed += 1;
			return;
		}
		if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_6)) {
			DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.y[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106);
			
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.cb[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206);
			
			DataOutputBlobWriter.openField(mcuWriter);
			for (int i = 0; i < 64; ++i) {
				mcuWriter.writeShort(emcu.cr[i]);
			}
			DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306);
			
			PipeWriter.publishWrites(output);

			numProcessed += 1;
		}
		else {
			System.err.println("Requesting shutdown");
			requestShutdown();
		}
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
				String filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				header.frameType = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, new StringBuilder()).toString();
				header.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501);
				header.startOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601);
				header.endOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701);
				header.successiveApproximation = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					System.out.println("YCbCrToRGB writing header to pipe...");
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
				component.huffmanACTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502);
				component.huffmanDCTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602);
				header.colorComponents.add(component);
				PipeReader.releaseReadLock(input);
				
				// write color component data to pipe
				System.out.println("Attempting to write color component to pipe...");
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					System.out.println("Inverse DCT writing color component to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502, component.huffmanACTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602, component.huffmanDCTableID);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Inverse DCT requesting shutdown");
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
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_6) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106);
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
					if (header.colorComponents.get(0).horizontalSamplingFactor == 1 &&
						header.colorComponents.get(0).verticalSamplingFactor == 2) {
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
					if (header.colorComponents.get(0).verticalSamplingFactor == 2) {
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
					if (header.colorComponents.get(0).horizontalSamplingFactor == 2) {
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

				if (count == header.colorComponents.get(0).horizontalSamplingFactor * header.colorComponents.get(0).verticalSamplingFactor) {
					if (header.colorComponents.get(0).horizontalSamplingFactor == 2 &&
						header.colorComponents.get(0).verticalSamplingFactor == 2) {
						expandColumnsAndRows(mcu1, mcu2, mcu3, mcu4);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu2);
						convertYCbCrToRGB(mcu3);
						convertYCbCrToRGB(mcu4);
						sendMCU(mcu1);
						sendMCU(mcu2);
						sendMCU(mcu3);
						sendMCU(mcu4);
					}
					else if (header.colorComponents.get(0).horizontalSamplingFactor == 2) {
						expandColumns(mcu1, mcu2);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu2);
						sendMCU(mcu1);
						sendMCU(mcu2);
					}
					else if (header.colorComponents.get(0).verticalSamplingFactor == 2) {
						expandRows(mcu1, mcu3);
						convertYCbCrToRGB(mcu1);
						convertYCbCrToRGB(mcu3);
						sendMCU(mcu1);
						sendMCU(mcu3);
					}
					else {
						convertYCbCrToRGB(mcu1);
						sendMCU(mcu1);
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
	
	/*public static void main(String[] args) {
		ArrayList<MCU> testArray =  new ArrayList<MCU>(1);
		MCU mcu = new MCU();
		mcu.y[0]  = -63;
		mcu.cb[0] =  -1;
		mcu.cr[0] =   6;
		mcu.y[1]  = -74;
		mcu.cb[1] = -12;
		mcu.cr[1] =  11;
		mcu.y[2]  = 106;
		mcu.cb[2] =   6;
		mcu.cr[2] = -16;
		testArray.add(mcu);
		byte[][] converted = convertYCbCrToRGB(testArray, 1, 3);
		for (int i = 0; i < converted.length ; ++i) {
			for (int j = 0; j < converted[0].length; j += 3)
			System.out.println(converted[i][j + 0] + ", " + converted[i][j + 1] + ", " + converted[i][j + 2]);
		}
	}*/
}
