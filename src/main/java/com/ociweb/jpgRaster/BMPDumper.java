package com.ociweb.jpgRaster;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BMPDumper extends PronghornStage {

	private final Pipe<YCbCrToRGBSchema> input;

	private final FieldReferenceOffsetManager FROM;
	
	private final int MSG_HEADER;
	private final int FIELD_HEIGHT;
	private final int FIELD_WIDTH;
	private final int FIELD_FILENAME;
	
	private final int MSG_PIXEL;
	private final int FIELD_RED;
	private final int FIELD_GREEN;
	private final int FIELD_BLUE;
	
	int width;
	int height;
	Appendable filename;
	
	int[][] pixels;
	
	protected BMPDumper(GraphManager graphManager, Pipe<YCbCrToRGBSchema> input) {
		super(graphManager, input, NONE);
		this.input = input;
		
		FROM = Pipe.from(input);
		
		MSG_HEADER = lookupTemplateLocator("HeaderMessage", FROM);
		FIELD_HEIGHT = lookupFieldLocator("height", MSG_HEADER, FROM);
		FIELD_WIDTH = lookupFieldLocator("width", MSG_HEADER, FROM);
		FIELD_FILENAME = lookupFieldLocator("filename", MSG_HEADER, FROM);
		
		MSG_PIXEL = lookupTemplateLocator("PixelMessage", FROM);
		FIELD_RED = lookupFieldLocator("red", MSG_PIXEL, FROM);
		FIELD_GREEN = lookupFieldLocator("green", MSG_PIXEL, FROM);
		FIELD_BLUE = lookupFieldLocator("blue", MSG_PIXEL, FROM);
		
	}

	public static void Dump(int[][] rgb, int height, int width, String filename) throws IOException {
		int paddingSize = (4 - (width * 3) % 4) % 4;
		int size = 14 + 12 + rgb.length * rgb[0].length + height * paddingSize;
		
		DataOutputStream file = new DataOutputStream(new FileOutputStream(filename));
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
				file.writeByte(rgb[i][j + 2]);
				file.writeByte(rgb[i][j + 1]);
				file.writeByte(rgb[i][j + 0]);
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

	@Override
	public void run() {
		
		int count = 0;
		
		while(PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if(msgIdx == MSG_HEADER){
				height = PipeReader.readInt(input, FIELD_HEIGHT);
				width = PipeReader.readInt(input,  FIELD_WIDTH);
				filename = PipeReader.readASCII(input,  FIELD_FILENAME, null);
				
				pixels = new int[height][width * 3];
			} else if(msgIdx == -1) {
				//die
			} else {
				int red = PipeReader.readInt(input, FIELD_RED);
				int green = PipeReader.readInt(input,  FIELD_GREEN);
				int blue = PipeReader.readInt(input,  FIELD_BLUE);
				
				pixels[count / width][(count % width) * 3] = red;
				pixels[count / width][(count % width) * 3 + 1] = green;
				pixels[count / width][(count % width) * 3 + 2] = blue;
				
				count += 1;
			}
			
			
			if(count >= (width * height)){
				try {
					Dump(pixels, height, width, filename.toString());
				} catch (IOException e) {
					e.printStackTrace();
//					throw new RuntimeException;
				}
			}
		}
	
	}
}
