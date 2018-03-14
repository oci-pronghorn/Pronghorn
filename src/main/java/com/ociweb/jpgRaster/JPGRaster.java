package com.ociweb.jpgRaster;

import java.io.File;
import java.util.ArrayList;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;

public class JPGRaster {

	public static void main(String[] args) {
//		String defaultFiles = "test_jpgs/huff_simple0.jpg test_jpgs/robot.jpg test_jpgs/cat.jpg test_jpgs/car.jpg test_jpgs/squirrel.jpg test_jpgs/nathan.jpg test_jpgs/earth.jpg test_jpgs/dice.jpg test_jpgs/pyramids.jpg test_jpgs/static.jpg test_jpgs/turtle.jpg";
		
		boolean verbose = hasArg("--verbose", "-v", args);
		
		String defaultFiles = "";
		String inputFilePaths = getOptNArg("--file", "-f", args, defaultFiles);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		for (String file : inputFilePaths.split(" ")) {
			if (!file.equals("")) {
				inputFiles.add(file);
			}
		}
		
		String defaultDirectory = "";
		String inputDirectory = getOptArg("--directory", "-d", args, defaultDirectory);
		if (!inputDirectory.equals("") && !inputDirectory.endsWith("/")) {
			inputDirectory += "/";
		}
		
		File[] files = new File(inputDirectory).listFiles();
		if (files != null) {
			for (File file : files) {
			    if (file.isFile()) {
			        inputFiles.add(inputDirectory + file.getName());
			    }
			}
		}
		
		if (inputFiles.size() == 0) {
			System.out.println("Usage: j2r [ -f file1 [ file2 ... ] | -d directory ] [ -v ] [ -p port ]");
			return;
		}
		
		GraphManager gm = new GraphManager();
		
		populateGraph(gm, inputFiles, verbose);
		
		String defaultPort = "";
		String portString = getOptArg("--port", "-p", args, defaultPort);
		if (portString != "") {
			int port = 0;
			try {
				port = Integer.parseInt(portString);
			}
			catch (Exception e) {}
			if (port != 0) {
				gm.enableTelemetry(Integer.parseInt(portString));
			}
		}
		
		StageScheduler.defaultScheduler(gm).startup();
	}


	private static void populateGraph(GraphManager gm, ArrayList<String> inputFiles, boolean verbose) {
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		JPGScanner scanner = new JPGScanner(gm, pipe1, verbose);
		new InverseQuantizer(gm, pipe1, pipe2, verbose);
		new InverseDCT(gm, pipe2, pipe3, verbose);
		new YCbCrToRGB(gm, pipe3, pipe4, verbose);
		new BMPDumper(gm, pipe4, verbose);
		
		for (int i = 0; i < inputFiles.size(); ++i) {
			String file = inputFiles.get(i);
			scanner.queueFile(file);
		}
	}
	
	public static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
        
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    return defaultValue;
                }
                return token.trim();
            }
            prev = token;
        }
        return defaultValue;
    }
	
	public static String getOptNArg(String longName, String shortName, String[] args, String defaultValue) {
        
		String tokens = "";
        for (int i = 0; i < args.length; ++i) {
        	String token = args[i];
            if (longName.equals(token) || shortName.equals(token)) {
            	for (int j = i + 1; j < args.length; ++j) {
            		token = args[j];
	            	if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
	                    return tokens;
	                }
	            	tokens += " " + token.trim();
            	}
                return tokens;
            }
        }
        return defaultValue;
    }
    

    public static boolean hasArg(String longName, String shortName, String[] args) {
        for(String token : args) {
            if(longName.equals(token) || shortName.equals(token)) {
                return true;
            }
        }
        return false;
    }

}
