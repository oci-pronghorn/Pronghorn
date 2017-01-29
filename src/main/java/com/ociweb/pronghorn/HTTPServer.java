package com.ociweb.pronghorn;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ModuleConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class HTTPServer {

	private static final Logger logger = LoggerFactory.getLogger(HTTPServer.class);
	
	
	public static void startupHTTPServer(boolean large, ModuleConfig config, String bindHost, int port, boolean isTLS) {
				
		boolean debug = false;
		
		if (!isTLS) {
			logger.warn("TLS has been progamatically switched off");
		}
		
		GraphManager gm = new GraphManager();
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1_250);
						
		///////////////
	    //BUILD THE SERVER
	    ////////////////		
		final ServerCoordinator serverCoord = NetGraphBuilder.httpServerSetup(isTLS, bindHost, port, gm, large, config);
					
		if (debug) {
			////////////////
			///FOR DEBUG GENERATE A PICTURE OF THE SERVER
			////////////////	
			final MonitorConsoleStage attach =  MonitorConsoleStage.attach(gm);
		}
		
		////////////////
		//CREATE A SCHEDULER TO RUN THE SERVER
		////////////////
		final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
				
		//////////////////
		//UPON CTL-C SHUTDOWN OF SERVER DO A CLEAN SHUTDOWN
		//////////////////
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	        public void run() {
	        		//soft shutdown
	        	    serverCoord.shutdown();
	                try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						
					}
	                //harder shutdown
	        		scheduler.shutdown();
	        		//hard shutdown
	                scheduler.awaitTermination(1, TimeUnit.SECONDS);
	
	        }
	    });
		
	    ///////////////// 
	    //START RUNNING THE SERVER
	    /////////////////        
	    scheduler.startup();
	}


	public static String buildStaticFileFolderPath(String testFile) {
		URL dir = ClassLoader.getSystemResource(testFile);
		String root = "";	//file:/home/nate/Pronghorn/target/test-classes/OCILogo.png
						
		try {
		
			String uri = dir.toURI().toString();
			uri = uri.replace("jar:","");
			uri = uri.replace("file:","");
			
			root = uri.substring(0, uri.lastIndexOf('/'));
			
		} catch (URISyntaxException e) {						
			e.printStackTrace();
		}
		return root;
	}


	public static String reportChoice(final String longName, final String shortName, final String value) {
	    System.out.print(longName);
	    System.out.print(" ");
	    System.out.print(shortName);
	    System.out.print(" ");
	    System.out.println(value);
	    return value;
	}


	public static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
	    
	    String prev = null;
	    for (String token : args) {
	        if (longName.equals(prev) || shortName.equals(prev)) {
	            if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
	                return defaultValue;
	            }
	            return reportChoice(longName, shortName, token.trim());
	        }
	        prev = token;
	    }
	    return reportChoice(longName, shortName, defaultValue);
	}


}
