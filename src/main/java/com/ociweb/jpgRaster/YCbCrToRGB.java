package com.ociweb.jpgRaster;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class YCbCrToRGB extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	
	
	protected YCbCrToRGB(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}

	private static byte[] convertToRGB(short Y, short Cb, short Cr) {
		byte[] rgb = new byte[3];
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
				Header header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				String filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				header.frameType = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, new StringBuilder()).toString();
				header.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501);
				header.startOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601);
				header.endOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701);
				header.successiveApproximation = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					System.out.println("Writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, filename);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, header.frameType);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501, header.precision);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601, header.startOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701, header.endOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801, header.successiveApproximation);
				}
				else {
					requestShutdown();
				}
				PipeWriter.publishWrites(output);
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
				
				// write color component data to pipe
				System.out.println("Attempting to write color component to pipe...");
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					System.out.println("Writing color component to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502, component.huffmanACTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602, component.huffmanDCTableID);
				}
				else {
					requestShutdown();
				}
				PipeWriter.publishWrites(output);
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_6) {
				MCU mcu = new MCU();
				ByteBuffer yBuffer = ByteBuffer.allocate(64 * 2);
				ByteBuffer cbBuffer = ByteBuffer.allocate(64 * 2);
				ByteBuffer crBuffer = ByteBuffer.allocate(64 * 2);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106, yBuffer);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206, cbBuffer);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306, crBuffer);
				convertYCbCrToRGB(mcu);
				
				ByteBuffer yBuffer2 = ByteBuffer.allocate(64 * 2);
				ByteBuffer cbBuffer2 = ByteBuffer.allocate(64 * 2);
				ByteBuffer crBuffer2 = ByteBuffer.allocate(64 * 2);
				for (int i = 0; i < 64; ++i) {
					yBuffer2.putShort(mcu.y[i]);
					cbBuffer2.putShort(mcu.cb[i]);
					crBuffer2.putShort(mcu.cr[i]);
				}
				yBuffer2.position(0);
				cbBuffer2.position(0);
				crBuffer2.position(0);
				PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106, yBuffer2);
				PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206, cbBuffer2);
				PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306, crBuffer2);
				PipeWriter.publishWrites(output);
			}
			else {
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
