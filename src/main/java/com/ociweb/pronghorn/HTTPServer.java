package com.ociweb.pronghorn;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.network.ModuleConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class HTTPServer {

    private static final int groups = 3;
    private static final int apps = 1; 
      
    public HTTPServer() {     
  
    }  
    
    public static void main(String[] args) {
        
    	GraphManager gm = new GraphManager();
    	GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000);
          	
        
        ModuleConfig config = new ModuleConfig() {

            
            PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 2048, 1<<15);//from module to  supervisor
            Pipe<ServerResponseSchema> output = new Pipe<ServerResponseSchema>(outgoingDataConfig);
            
			@Override
			public long addModule(int a, GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs,
					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> spec) {
				
				
				FileReadModuleStage.newInstance(graphManager, inputs, output, spec, new File("/home/nate/elmForm"));				
				//return needed headers
				return 0;
			}

			@Override
			public CharSequence getPathRoute(int a) {
				return "/%b";
			}

			@Override
			public Pipe<ServerResponseSchema>[] outputPipes(int a) {
				return new Pipe[]{output};
			}

			@Override
			public int moduleCount() {
				return 1;
			}
        	
        	
        };
        
        int requestUnwrapUnits = 1;
        int responseWrapUnits = 1;
        int outputPipes = 2;
        int socketWriters = 1;
        int serverInputBlobs = 1<<11;

		int serverBlobToEncrypt = 1<<14;
		int serverBlobToWrite = 1<<16;
		 int routerCount = 4;	
		 
		String bindHost = "127.0.0.1";
		ServerCoordinator coordinator = new ServerCoordinator(groups, bindHost, 8443, 15, 2, routerCount);//32K simulanious connections on server. 
		int serverInputMsg = 64;
		int serverMsgToEncrypt = 2048;
		int serverMsgToWrite = 1024;
		gm = NetGraphBuilder.buildHTTPServerGraph(true, gm, groups, 2, config, coordinator, requestUnwrapUnits,
				responseWrapUnits, outputPipes, socketWriters, 
				serverInputMsg, serverInputBlobs, serverMsgToEncrypt, serverBlobToEncrypt, serverMsgToWrite, serverBlobToWrite, routerCount); 
		//gm = NetGraphBuilder.buildHTTPServerGraph(gm, groups, apps);
        
        
        GraphManager.exportGraphDotFile(gm, "RealHTTPServer");
        
       
        MonitorConsoleStage.attach(gm);
        
        
        
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        scheduler.startup();                
        
               
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(1, TimeUnit.MINUTES);
            }
        });
        

        
        
    }
}
