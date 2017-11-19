package com.ociweb.pronghorn;

import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.config.*;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.util.MainArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class HTTPServer {

	private static final Logger logger = LoggerFactory.getLogger(HTTPServer.class);
	
	
	public static void startupHTTPServer(GraphManager gm, int processors, ModuleConfig config, String bindHost, int port, TLSCertificates tlsCertificates) {
				
		
		
		boolean debug = false;
		
		if (tlsCertificates == null) {
			logger.warn("TLS has been progamatically switched off");
		}

		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, processors>=4 ? 20_000 : 2_000_000 );//pi needs larger values...
						
		///////////////
	    //BUILD THE SERVER
	    ////////////////		
		final ServerCoordinator serverCoord = NetGraphBuilder.httpServerSetup(tlsCertificates, bindHost, port, gm, processors, config);
					
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
						Thread.sleep(300);
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


	public static ModuleConfig simpleFileServerConfig(final int fileOutgoing, final int fileChunkSize,
			final String resourcesRoot, final String resourcesDefault, final File pathRoot) {
		//using the basic no-fills API
		final int finalModuleCount = 1;
		final int fileServerIndex = 0;
		
		ModuleConfig config = new ModuleConfig() {
			
		    final PipeConfig<ServerResponseSchema> fileServerOutgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, fileOutgoing, fileChunkSize);//from modules  to  supervisor
	
			@Override
			public int moduleCount() {
				return finalModuleCount;
			}        
		 	
			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager, RouterStageConfig routerConfig,
					Pipe<HTTPRequestSchema>[] inputPipes) {
				
				Pipe<ServerResponseSchema>[] staticFileOutputs = null;
				if (fileServerIndex == a) {
					
					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputPipes.length;
					
					staticFileOutputs = new Pipe[instances];
					
					int i = instances;
					while (--i>=0) {
						staticFileOutputs[i] = new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig); //TODO: old code which will be removed.
						if (null != pathRoot) {
							//file based site
							FileReadModuleStage.newInstance(graphManager, inputPipes[i], staticFileOutputs[i], (HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTP1xRouterStageConfig)routerConfig).httpSpec, pathRoot);	
						} else {
							//jar resources based site
							FileReadModuleStage.newInstance(graphManager, inputPipes[i], staticFileOutputs[i], ((HTTP1xRouterStageConfig)routerConfig).httpSpec, resourcesRoot, resourcesDefault);	
						}
					}
					
				}
				
				routerConfig.registerRoute(
	                    (CharSequence) ((fileServerIndex == a) ? "/${path}" : null)
	                    ); //NOTE: we did not request any headers here
	
				if (fileServerIndex == a) {
					return staticFileOutputs;
				} else {
					return null;
				}				
			}  
			
		 };
		return config;
	}


}
