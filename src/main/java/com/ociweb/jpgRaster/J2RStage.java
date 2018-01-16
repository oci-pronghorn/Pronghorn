package com.ociweb.jpgRaster;


import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;

import java.io.IOException;
import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.MainArgs;

public class J2RStage extends PronghornStage {

	private final Pipe<YCbCrToRGBSchema> output;
	
	private final FieldReferenceOffsetManager FROM;
	
	private final int MSG_HEADER;
	private final int FIELD_HEIGHT;
	private final int FIELD_WIDTH;
	private final int FIELD_FILENAME;
	
	private final int MSG_PIXEL;
	private final int FIELD_RED;
	private final int FIELD_GREEN;
	private final int FIELD_BLUE;
	
	protected J2RStage(GraphManager graphManager, Pipe<YCbCrToRGBSchema> output) {
		super(graphManager, NONE, output);
		
		this.output = output;
		
		FROM = Pipe.from(output);
		
		MSG_HEADER = lookupTemplateLocator("HeaderMessage", FROM);
		FIELD_FILENAME = lookupFieldLocator("filename", MSG_HEADER, FROM);
		FIELD_HEIGHT = lookupFieldLocator("height", MSG_HEADER, FROM);
		FIELD_WIDTH = lookupFieldLocator("width", MSG_HEADER, FROM);
		
		
		MSG_PIXEL = lookupTemplateLocator("PixelMessage", FROM);
		FIELD_RED = lookupFieldLocator("red", MSG_PIXEL, FROM);
		FIELD_GREEN = lookupFieldLocator("green", MSG_PIXEL, FROM);
		FIELD_BLUE = lookupFieldLocator("blue", MSG_PIXEL, FROM);
		
	}

	@Override
	public void run() {
		
//		String defaultFiles = "test_jpgs/car test_jpgs/cat test_jpgs/dice test_jpgs/earth test_jpgs/nathan test_jpgs/pyramids test_jpgs/robot test_jpgs/squirrel test_jpgs/static test_jpgs/turtle";
		String defaultFiles = "test_jpgs/car";
		//String inputFilePaths = MainArgs.getOptArg("fileName", "-f", null, defaultFiles);
		String inputFilePaths = defaultFiles;
		String[] inputFiles = inputFilePaths.split(" ");

		//GraphManager gm = new GraphManager();
		
		//populateGraph(gm, inputFilePath);
		
		//gm.enableTelemetry(8089);
		
		//StageScheduler.defaultScheduler(gm).startup();
		
		for (int i = 0; i < inputFiles.length; ++i) {
			String file = inputFiles[i];
			if (file.length() > 4 && file.substring(file.length() - 4).equals(".jpg")) {
				file = file.substring(0, file.length() - 4);
			}
			try {
				System.out.println("Reading '" + file + "' JPG file...");
				Header header = JPGScanner.ReadJPG(file + ".jpg");
				if (header.valid) {
					System.out.println("Performing Huffman Decoding...");
					ArrayList<MCU> mcus = HuffmanDecoder.decodeHuffmanData(header);
					if (mcus != null) {
						System.out.println("Performing Inverse Quantization...");
						InverseQuantizer.dequantize(mcus, header);
						System.out.println("Performing Inverse DCT...");
						InverseDCT.inverseDCT(mcus);
						System.out.println("Performing YCbCr to RGB Conversion...");
						
						if(PipeWriter.tryWriteFragment(output, MSG_HEADER)){
							PipeWriter.writeInt(output, FIELD_HEIGHT, header.height);
							PipeWriter.writeInt(output, FIELD_WIDTH, header.width);
							PipeWriter.writeASCII(output, FIELD_FILENAME, file);
							
							YCbCrToRGB.convertYCbCrToRGB(mcus, header.height, header.width, output,
									MSG_PIXEL, FIELD_RED, FIELD_GREEN, FIELD_BLUE);
							System.out.println("Writing BMP file...");
							
	//						BMPDumper.Dump(rgb, header.height, header.width, file + ".bmp");
							System.out.println("Done.");
						}
					}
				}
			} catch (IOException e) {
				System.err.println("Error - Unknown error");
			}
		}
		
	}

}
