package com.ociweb.jpgRaster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;

import com.ociweb.jpgRaster.j2r.BMPDumperStage;
import com.ociweb.jpgRaster.j2r.InverseDCTStage;
import com.ociweb.jpgRaster.j2r.InverseQuantizerStage;
import com.ociweb.jpgRaster.j2r.JPGScannerStage;
import com.ociweb.jpgRaster.j2r.YCbCrToRGBStage;
import com.ociweb.jpgRaster.r2j.BMPScannerStage;
import com.ociweb.jpgRaster.r2j.ForwardDCTStage;
import com.ociweb.jpgRaster.r2j.HuffmanEncoderStage;
import com.ociweb.jpgRaster.r2j.QuantizerStage;
import com.ociweb.jpgRaster.r2j.RGBToYCbCrStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;

public class JPGRaster {

	public static void main(String[] args) throws IOException {
		
		boolean verbose = hasArg("--verbose", "-v", args);
		boolean time = hasArg("--time", "-t", args);
		boolean encode = hasArg("--encode", "-e", args);

		String defaultFiles = "";
		ArrayList<String> inputFilePaths = getOptNArg("--file", "-f", args, defaultFiles);

		ArrayList<String> inputFiles = new ArrayList<String>();
		for (String file : inputFilePaths) {
			if (file.startsWith("./")) {
				file = file.substring(2);
				while (file.startsWith("/")) {
					file = file.substring(1);
				}
			}
			String userdir = System.getProperty("user.dir");
			if (!file.startsWith("/")) {
				file = userdir + "/" + file;
			}
			String dir = file;
			while (dir.contains("*") || dir.contains("?") ||
					dir.contains("{") || dir.contains("[")) {
				dir = dir.substring(0, dir.lastIndexOf("/"));
			}
			String glob = "glob:" + file;
			FileMatcher filematcher = new FileMatcher(glob);
			Files.walkFileTree(Paths.get(dir), filematcher);
			for (String inputFile : filematcher.files) {
				if (!inputFile.equals("")) {
					if (inputFile.startsWith(userdir)) {
						inputFiles.add(inputFile.substring(userdir.length() + 1));
					} else {
						inputFiles.add(inputFile);
					}

				}
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

		if (inputFiles.size() == 0 || hasArg("--help", "-h", args)) {
			System.out.println("Usage: j2r [ -e [ -q 50 | 75 | 100 ] ] [ -f file1 [ file2 ... ] | -d directory ] [ -v ] [ -t ] [ -p port ]");
			return;
		}
		
		GraphManager gm = new GraphManager();
		
		if (encode) {
			String defaultQuality = "";
			String qualityString = getOptArg("--quality", "-q", args, defaultQuality);
			int quality = 75;
			try {
				quality = Integer.parseInt(qualityString);
				if (quality != 50 && quality != 75 && quality != 100) {
					quality = 75;
				}
			}
			catch (Exception e) {}
			populateEncoderGraph(gm, inputFiles, verbose, time, quality);
		}
		else {
			populateDecoderGraph(gm, inputFiles, verbose, time);
		}
		
		String defaultPort = "";
		String portString = getOptArg("--port", "-p", args, defaultPort);
		if (portString != "") {
			int port = 0;
			try {
				port = Integer.parseInt(portString);
			}
			catch (Exception e) {}
			if (port != 0) {
				gm.enableTelemetry(port);
			}
		}
		
		StageScheduler.defaultScheduler(gm).startup();
	}


	private static void populateDecoderGraph(GraphManager gm, ArrayList<String> inputFiles, boolean verbose, boolean time) {
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		new JPGScannerStage(gm, pipe1, verbose, inputFiles);
		new InverseQuantizerStage(gm, pipe1, pipe2, verbose);
		new InverseDCTStage(gm, pipe2, pipe3, verbose);
		new YCbCrToRGBStage(gm, pipe3, pipe4, verbose);
		new BMPDumperStage(gm, pipe4, verbose, time);

	}

	private static void populateEncoderGraph(GraphManager gm, ArrayList<String> inputFiles, boolean verbose, boolean time, int quality) {
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		new BMPScannerStage(gm, pipe1, verbose, inputFiles);
		new RGBToYCbCrStage(gm, pipe1, pipe2, verbose);
		new ForwardDCTStage(gm, pipe2, pipe3, verbose);
		new QuantizerStage(gm, pipe3, pipe4, verbose, quality);
		new HuffmanEncoderStage(gm, pipe4, verbose, time, quality);
	
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

	public static ArrayList<String> getOptNArg(String longName, String shortName, String[] args, String defaultValue) {
		ArrayList<String> tokens = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
        	String token = args[i];
            if (longName.equals(token) || shortName.equals(token)) {
            	for (int j = i + 1; j < args.length; ++j) {
            		token = args[j];
	            	if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
	                    return tokens;
	                }
	            	tokens.add(token.trim());
            	}
                return tokens;
            }
        }
        tokens.clear();
        tokens.add(defaultValue);
        return tokens;
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
