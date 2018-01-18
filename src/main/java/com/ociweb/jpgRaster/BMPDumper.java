package com.ociweb.jpgRaster;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BMPDumper extends PronghornStage {

	private final Pipe<JPGSchema> input;
	
	static int width;
	static int height;
	static Appendable filename;
	
	static int[][] pixels;
	static int count;
	
	protected BMPDumper(GraphManager graphManager, Pipe<JPGSchema> input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	public static void Dump() throws IOException {
		int paddingSize = (4 - (width * 3) % 4) % 4;
		int size = 14 + 12 + pixels.length * pixels[0].length + height * paddingSize;
		
		DataOutputStream file = new DataOutputStream(new FileOutputStream(filename.toString()));
		file.writeByte('B');
		file.writeByte('M');
		writeInt(file, size);
		writeInt(file, 0);
		writeInt(file, 0x1A);
		writeInt(file, 12);
		writeShort(file, width);
		writeShort(file, height);
		writeShort(file, 1);
		writeShort(file, 24);
		for (int i = height - 1; i >= 0; --i) {
			for (int j = 0; j < width * 3 - 2; j += 3) {
				file.writeByte(pixels[i][j + 2]);
				file.writeByte(pixels[i][j + 1]);
				file.writeByte(pixels[i][j + 0]);
			}
			for (int j = 0; j < paddingSize; j++) {
				file.writeByte(0);
			}
		}
		file.close();
	}
	
	private static void writeInt(DataOutputStream stream, int v) throws IOException {
		stream.writeByte((v & 0x000000FF));
		stream.writeByte((v & 0x0000FF00) >>  8);
		stream.writeByte((v & 0x00FF0000) >> 16);
		stream.writeByte((v & 0xFF000000) >> 24);
	}
	
	private static void writeShort(DataOutputStream stream, int v) throws IOException {
		stream.writeByte((v & 0x00FF));
		stream.writeByte((v & 0xFF00) >>  8);
	}

	@Override
	public void run() {
		
		/*while (PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADER) {
				height = PipeReader.readInt(input, JPGSchema.FIELD_HEIGHT);
				width = PipeReader.readInt(input, JPGSchema.FIELD_WIDTH);
				filename = PipeReader.readASCII(input, JPGSchema.FIELD_FILENAME, null);
				
				pixels = new int[height][width * 3];
				count = 0;
			} else if (msgIdx == JPGSchema.MSG_PIXEL) {
				int red = PipeReader.readInt(input, JPGSchema.FIELD_RED);
				int green = PipeReader.readInt(input, JPGSchema.FIELD_GREEN);
				int blue = PipeReader.readInt(input, JPGSchema.FIELD_BLUE);
				
				pixels[count / width][(count % width) * 3 + 0] = red;
				pixels[count / width][(count % width) * 3 + 1] = green;
				pixels[count / width][(count % width) * 3 + 2] = blue;
				
				count += 1;
			}
			else {
				requestShutdown();
			}
			
			
			if (count >= (width * height)) {
				try {
					Dump();
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}*/
	
	}
	
	/*public static void main(String[] args) {
		
		byte[][] rgb = new byte[8][8 * 3];
		// red
		rgb[0][0 * 3 + 0] = (byte)255;
		rgb[0][0 * 3 + 1] = 0;
		rgb[0][0 * 3 + 2] = 0;
		// green
		rgb[0][1 * 3 + 0] = 0;
		rgb[0][1 * 3 + 1] = (byte)255;
		rgb[0][1 * 3 + 2] = 0;
		// blue
		rgb[0][2 * 3 + 0] = 0;
		rgb[0][2 * 3 + 1] = 0;
		rgb[0][2 * 3 + 2] = (byte)255;
		// cyan
		rgb[1][0 * 3 + 0] = 0;
		rgb[1][0 * 3 + 1] = (byte)255;
		rgb[1][0 * 3 + 2] = (byte)255;
		// magenta
		rgb[1][1 * 3 + 0] = (byte)255;
		rgb[1][1 * 3 + 1] = 0;
		rgb[1][1 * 3 + 2] = (byte)255;
		// yellow
		rgb[1][2 * 3 + 0] = (byte)255;
		rgb[1][2 * 3 + 1] = (byte)255;
		rgb[1][2 * 3 + 2] = 0;
		// black
		rgb[2][0 * 3 + 0] = 0;
		rgb[2][0 * 3 + 1] = 0;
		rgb[2][0 * 3 + 2] = 0;
		// gray
		rgb[2][1 * 3 + 0] = (byte)128;
		rgb[2][1 * 3 + 1] = (byte)128;
		rgb[2][1 * 3 + 2] = (byte)128;
		// white
		rgb[2][2 * 3 + 0] = (byte)255;
		rgb[2][2 * 3 + 1] = (byte)255;
		rgb[2][2 * 3 + 2] = (byte)255;
		try {
			Dump(rgb, 3, 3, "bmp_test.bmp");
		} catch (IOException e) {
			System.out.println("Error - Unknown error creating BMP file");
		}
		
	}*/
}
