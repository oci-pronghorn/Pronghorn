package com.ociweb.jpgRaster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.HashSet;

import com.ociweb.jpgRaster.j2r.BMPDumper;
import com.ociweb.jpgRaster.j2r.InverseDCT;
import com.ociweb.jpgRaster.j2r.InverseQuantizer;
import com.ociweb.jpgRaster.j2r.JPGScanner;
import com.ociweb.jpgRaster.j2r.YCbCrToRGB;
import com.ociweb.jpgRaster.r2j.BMPScanner;
import com.ociweb.jpgRaster.r2j.ForwardDCT;
import com.ociweb.jpgRaster.r2j.HuffmanEncoder;
import com.ociweb.jpgRaster.r2j.Quantizer;
import com.ociweb.jpgRaster.r2j.RGBToYCbCr;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;

public class JPGRaster {

	public static void main(String[] args) throws IOException {
		
		boolean verbose = hasArg("--verbose", "-v", args);
		boolean time = hasArg("--time", "-t", args);
		boolean encode = hasArg("--encode", "-e", args);
		
		String defaultFiles = "";
		String inputFilePaths = getOptNArg("--file", "-f", args, defaultFiles);

		HashSet<String> inputFiles = new HashSet<String>();
		for (String file : inputFilePaths.split(" ")) {
			if (file.startsWith("./")) {
				file = file.substring(2);
			}
			String dir = System.getProperty("user.dir");
			if (file.startsWith("/")) {
				dir = file.substring(0, file.lastIndexOf("/"));
			}
			else {
				dir += "/" + file.substring(0, file.lastIndexOf("/"));
			}
			String glob = "glob:**/" + file;
			FileMatcher filematcher = new FileMatcher(glob);
			Files.walkFileTree(Paths.get(dir), filematcher);
			for (String inputFile : filematcher.files) {
				if (!inputFile.equals("")) {
					inputFiles.add(inputFile);
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


	private static void populateDecoderGraph(GraphManager gm, HashSet<String> inputFiles, boolean verbose, boolean time) {
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		JPGScanner scanner = new JPGScanner(gm, pipe1, verbose);
		new InverseQuantizer(gm, pipe1, pipe2, verbose);
		new InverseDCT(gm, pipe2, pipe3, verbose);
		new YCbCrToRGB(gm, pipe3, pipe4, verbose);
		new BMPDumper(gm, pipe4, verbose, time);
		
		for (String file : inputFiles) {
			scanner.queueFile(file);
		}
	}

	private static void populateEncoderGraph(GraphManager gm, HashSet<String> inputFiles, boolean verbose, boolean time, int quality) {
		
		Pipe<JPGSchema> pipe1 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe2 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe3 = JPGSchema.instance.newPipe(500, 200);
		Pipe<JPGSchema> pipe4 = JPGSchema.instance.newPipe(500, 200);
		
		BMPScanner scanner = new BMPScanner(gm, pipe1, verbose);
		new RGBToYCbCr(gm, pipe1, pipe2, verbose);
		new ForwardDCT(gm, pipe2, pipe3, verbose);
		new Quantizer(gm, pipe3, pipe4, verbose, quality);
		new HuffmanEncoder(gm, pipe4, verbose, time, quality);
		
		for (String file : inputFiles) {
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
