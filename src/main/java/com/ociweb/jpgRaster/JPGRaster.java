package com.ociweb.jpgRaster;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.util.MainArgs;

public class JPGRaster {

	public static void main(String[] args) {
		//String defaultFiles = "test_jpgs/car test_jpgs/cat test_jpgs/dice test_jpgs/earth test_jpgs/nathan test_jpgs/pyramids test_jpgs/robot test_jpgs/squirrel test_jpgs/static test_jpgs/turtle";
		String defaultFiles = "test_jpgs/huff_simple0";
		String inputFilePaths = MainArgs.getOptArg("fileName", "-f", args, defaultFiles);
		String[] inputFiles = inputFilePaths.split(" ");
		
		/*for (int i = 0; i < inputFiles.length; ++i) {
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
						byte[][] rgb = YCbCrToRGB.convertYCbCrToRGB(mcus, header.height, header.width);
						System.out.println("Writing BMP file...");
						BMPDumper.Dump(rgb, header.height, header.width, file + ".bmp");
						System.out.println("Done.");
					}
				}
			} catch (IOException e) {
				System.err.println("Error - Unknown error");
			}
		}*/
		
		GraphManager gm = new GraphManager();
		
		populateGraph(gm, inputFiles);
				
		gm.enableTelemetry(8089);
				
		StageScheduler.defaultScheduler(gm).startup();
	}


	private static void populateGraph(GraphManager gm, String[] inputFiles) {
				
		/*Pipe<RawDataSchema> pipe1  = RawDataSchema.instance.newPipe(10, 10_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1A = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size
		Pipe<RawDataSchema> pipe1B = RawDataSchema.instance.newPipe(20, 20_000); // 10 chunks each 10K in  size
		
		
		new FileBlobReadStage(gm, pipe1, inputFilePath); // This stage reads a file
		
		
		// This stage replicates the data to two pipes, great for debugging while passing on the real data.
		new ReplicatorStage<>(gm, pipe1, pipe1A, pipe1B); 
		
		new ConsoleJSONDumpStage<>(gm, pipe1A); // see all the data at the console.
		//new ConsoleSummaryStage<>(gm, pipe1A); // dumps just a count of messages periodically
		
		new PipeCleanerStage<>(gm, pipe1B); // dumps all data which came in 
		
		//new FileBlobWriteStage(gm, pipe1B, false, ".\targetFile.dat"); // write byte data to disk
		*/
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(10, 100_000);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(10, 100_000);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(10, 100_000);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(10, 100_000);
		Pipe<JPGSchema> pipe5 = JPGSchema.instance.newPipe(10, 100_000);
		
		JPGScanner scanner = new JPGScanner(gm, pipe1);
		new HuffmanDecoder(gm, pipe1, pipe2);
		new InverseQuantizer(gm, pipe2, pipe3);
		new InverseDCT(gm, pipe3, pipe4);
		new YCbCrToRGB(gm, pipe4, pipe5);
		new BMPDumper(gm, pipe5);
		
		//new ConsoleJSONDumpStage<JPGSchema>(gm, pipe5);
		
		for (int i = 0; i < inputFiles.length; ++i) {
			String file = inputFiles[i];
			if (file.length() > 4 && file.substring(file.length() - 4).equals(".jpg")) {
				file = file.substring(0, file.length() - 4);
			}
			scanner.queueFile(file);
		}
	}
	

}
