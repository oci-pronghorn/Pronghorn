package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.byteMCU;
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
	
	public static byte[][] convertYCbCrToRGB(ArrayList<MCU> mcus, int height, int width) {
		int mcuHeight = (height + 7) / 8;
		int mcuWidth = (width + 7) / 8;
		int unusedRows = (mcuHeight * 8) - height;
		int unusedColumns = (mcuWidth * 8) - width;
		//ArrayList<RGB> rgb = new ArrayList<RGB>(mcuHeight * mcuWidth);
		byte[][] pixels = new byte[height][width * 3];
		byte[] pixel;
		for (int i = 0; i < mcuHeight; ++i) {         // mcu height
			for (int y = 0; y < 8; ++y) {             // pixel height
				for (int j = 0; j < mcuWidth; ++j) {  // mcu width
					for (int x = 0; x < 8; ++x) {     // pixel width
						if (i == mcuHeight - 1 && y >= (8 - unusedRows)) {
							break;
						}
						if (j == mcuWidth - 1 && x >= (8 - unusedColumns)) {
							break;
						}
						pixel = convertToRGB(mcus.get(i * mcuWidth + j).y[y * 8 + x],
											 mcus.get(i * mcuWidth + j).cb[y * 8 + x],
											 mcus.get(i * mcuWidth + j).cr[y * 8 + x]);
						pixels[i * 8 + y][(j * 8 + x) * 3 + 0] = pixel[0];
						pixels[i * 8 + y][(j * 8 + x) * 3 + 1] = pixel[1];
						pixels[i * 8 + y][(j * 8 + x) * 3 + 2] = pixel[2];
					}
				}
			}
		}
		return pixels;
	}

	@Override
	public void run() {
		/*while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == InverseDCTSchema.MSG_HEADER) {
				int height = PipeReader.readInt(input, InverseDCTSchema.FIELD_HEIGHT);
				int width = PipeReader.readInt(input, InverseDCTSchema.FIELD_WIDTH);
				Appendable filename = PipeReader.readASCII(input, InverseDCTSchema.FIELD_FILENAME, null);
				
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADER)) {
					PipeWriter.writeInt(output, JPGSchema.FIELD_HEIGHT, height);
					PipeWriter.writeInt(output, JPGSchema.FIELD_WIDTH, width);
					PipeWriter.writeASCII(output, JPGSchema.FIELD_FILENAME, filename.toString());
				}
			}
			else if (msgIdx == InverseDCTSchema.MSG_MCU) {
				byteMCU bmcu = new byteMCU();
				PipeReader.readBytes(input, InverseDCTSchema.FIELD_Y, bmcu.y, 0);
				PipeReader.readBytes(input, InverseDCTSchema.FIELD_CB, bmcu.cb, 0);
				PipeReader.readBytes(input, InverseDCTSchema.FIELD_CR, bmcu.cr, 0);
				
				MCU mcu = new MCU();
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = bmcu.y[i];
					mcu.cb[i] = bmcu.cb[i];
					mcu.cr[i] = bmcu.cr[i];
				}
				convertYCbCrToRGB(mcu);
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCU)) {
					PipeWriter.writeInt(output, JPGSchema.FIELD_HEIGHT, height);
					PipeWriter.writeInt(output, JPGSchema.FIELD_WIDTH, width);
					PipeWriter.writeASCII(output, JPGSchema.FIELD_FILENAME, filename.toString());
				}
			}
			else {
				requestShutdown();
			}
		}*/
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
