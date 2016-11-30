package com.ociweb.pronghorn;

import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.network.HTTPModuleFileReadStage;
import com.ociweb.pronghorn.network.ModuleConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
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

			@Override
			public long addModule(int a, GraphManager graphManager, Pipe<HTTPRequestSchema> input,
					Pipe<ServerResponseSchema> output,
					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> spec) {
				
				HTTPModuleFileReadStage.newInstance(graphManager, input, output, spec, "/home/nate/elmForm");
				
				//return needed headers
				return 0;
			}

			@Override
			public CharSequence getPathRoute(int a) {
				return "/%b";
			}
        	
        	
        };
		gm = NetGraphBuilder.buildHTTPTLSServerGraph(true, gm, groups, 2, apps, config, 8443); 
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
