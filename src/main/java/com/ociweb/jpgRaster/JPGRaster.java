package com.ociweb.jpgRaster;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.util.MainArgs;

public class JPGRaster {

	public static void main(String[] args) {
//		String defaultFiles = "test_jpgs/huff_simple0.jpg test_jpgs/robot.jpg test_jpgs/cat.jpg test_jpgs/car.jpg test_jpgs/squirrel.jpg test_jpgs/nathan.jpg test_jpgs/earth.jpg test_jpgs/dice.jpg test_jpgs/pyramids.jpg test_jpgs/static.jpg test_jpgs/turtle.jpg";
		
		
		String defaultFiles = "";
		String inputFilePaths = MainArgs.getOptArg("fileName", "-f", args, defaultFiles);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		for (String file : inputFilePaths.split(" ")) {
			if (!file.equals("")) {
				inputFiles.add(file);
			}
		}
		
		String defaultDirectory = "test_jpgs/";
		String inputDirectory = MainArgs.getOptArg("directory", "-d", args, defaultDirectory);
		
		File[] files = new File(inputDirectory).listFiles();
		if (files != null) {
			for (File file : files) {
			    if (file.isFile()) {
			        inputFiles.add(inputDirectory + file.getName());
			    }
			}
		}
		//Collections.shuffle(inputFiles);
		
		GraphManager gm = new GraphManager();
		
		populateGraph(gm, inputFiles);
		
		gm.enableTelemetry(8089);
		
		StageScheduler.defaultScheduler(gm).startup();
	}


	private static void populateGraph(GraphManager gm, ArrayList<String> inputFiles) {
		
		// such a large pipe helps with 2:1:1 images. a better fix is needed
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		JPGScanner scanner = new JPGScanner(gm, pipe1);
		new InverseQuantizer(gm, pipe1, pipe2);
		new InverseDCT(gm, pipe2, pipe3);
		new YCbCrToRGB(gm, pipe3, pipe4);
		new BMPDumper(gm, pipe4);
		
		for (int i = 0; i < inputFiles.size(); ++i) {
			String file = inputFiles.get(i);
			scanner.queueFile(file);
		}
	}

}
