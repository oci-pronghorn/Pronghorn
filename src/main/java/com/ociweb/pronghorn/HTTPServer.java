package com.ociweb.pronghorn;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.FixedThreadsScheduler;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.util.MainArgs;

public class HTTPServer {

	private static final Logger logger = LoggerFactory.getLogger(HTTPServer.class);
	
	
	public static void startupHTTPServer(boolean large, ModuleConfig config, String bindHost, int port, boolean isTLS) {
				
		boolean debug = false;
		
		if (!isTLS) {
			logger.warn("TLS has been progamatically switched off");
		}
		// 78,300
		
		GraphManager gm = new GraphManager();
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, large ? 20_000 : 2_000_000 );//pi needs larger values...
						
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
	//	final StageScheduler scheduler = new FixedThreadsScheduler(gm, Runtime.getRuntime().availableProcessors(), false);
				
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


	public static String buildStaticFileFolderPath(String testFile, boolean fullPath) {
		URL dir = ClassLoader.getSystemResource(testFile);
		String root = "";	//file:/home/nate/Pronghorn/target/test-classes/OCILogo.png
						
		try {
		
			String uri = dir.toURI().toString();
			uri = uri.replace("jar:","");
			uri = uri.replace("file:","");
			
			root = fullPath ? uri.toString() : uri.substring(0, uri.lastIndexOf('/'));
			
		} catch (URISyntaxException e) {						
			e.printStackTrace();
		}
		return root;
	}

	public static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
	    return MainArgs.getOptArg(longName, shortName, args, defaultValue);
	}


}
