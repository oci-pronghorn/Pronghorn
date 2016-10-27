package com.ociweb.pronghorn;

import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class HTTPServer {

    private static final int groups = 3;
    private static final int apps = 2; 
      
    public HTTPServer() {     
  
    }  
    
    public static void main(String[] args) {
        
    	GraphManager gm = new GraphManager();
    	GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000); //TODO: this must happen before the graph is built? why?
        gm = NetGraphBuilder.buildHTTPServerGraph(gm, groups, apps);
        
        
       // GraphManager.exportGraphDotFile(gm, "RealHTTPServer");
        
       
        //MonitorConsoleStage.attach(gm);
        
        
        
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        scheduler.startup();                
        
        
        
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(1, TimeUnit.MINUTES);
            }
        });
        
        try {
        	Thread.sleep(1000);
        } catch (InterruptedException e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
        
   //     System.exit(0);
        
        
    }
}
