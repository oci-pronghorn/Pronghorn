package com.ociweb.pronghorn;

import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.network.HTTPModuleFileReadStage;
import com.ociweb.pronghorn.stage.network.HTTPRouterStage;
import com.ociweb.pronghorn.stage.network.ServerConnectionReaderStage;
import com.ociweb.pronghorn.stage.network.ServerConnectionWriterStage;
import com.ociweb.pronghorn.stage.network.ServerCoordinator;
import com.ociweb.pronghorn.stage.network.ServerNewConnectionStage;
import com.ociweb.pronghorn.stage.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPSpecification;
import com.ociweb.pronghorn.stage.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.stage.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;


//TODO: server with static files in folder
//TODO: server wtih static resources files
//TODO: rest call
//TODO: websocket call
//TODO: API Integrations SQRL, Yubico? Braintree? Twitter? 


public class HTTPServer {

    private final int groups = 3;
    private final int apps = 2; 
        
    private final PipeConfig<ServerConnectionSchema> newConnectionsConfig;  
    private final PipeConfig<HTTPRequestSchema> httpRequestPipeConfig;
    private final PipeConfig<ServerResponseSchema> outgoingDataConfig;
    private final PipeConfig<ServerRequestSchema> incomingDataConfig;
    
    public HTTPServer() {     
  
        newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);       
        outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 10, 4000);
        incomingDataConfig = new PipeConfig<ServerRequestSchema>(ServerRequestSchema.instance, 10, 4000);
        httpRequestPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 10, 4000);
    }
    
    
    public static void main(String[] args) {
        HTTPServer instance  = new HTTPServer();
        
        GraphManager gm = instance.buildGraph(new GraphManager());
        
        
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        scheduler.startup();
        
        //sleep forever?
        
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(1, TimeUnit.MINUTES);
            }
        });
        
        
    }

    private GraphManager buildGraph(GraphManager graphManager) {
        
        ServerCoordinator coordinator = new ServerCoordinator(groups, 8081); 
        
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);

        
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(graphManager, coordinator, newConnectionsPipe);
        PipeCleanerStage<ServerConnectionSchema> dump = new PipeCleanerStage<>(graphManager, newConnectionsPipe);
                
        Pipe[][] incomingGroup = new Pipe[groups][];

        int g = groups;
        while (--g >= 0) {//create each connection group            
            
            Pipe<ServerResponseSchema>[] fromApps = new Pipe[apps];
            Pipe<HTTPRequestSchema>[] toApps = new Pipe[apps];
            
            long[] headers = new long[apps];
            int[] msgIds = new int[apps];
            
            int a = apps;
            while (--a>=0) { //create every app for this connection group
                fromApps[a] = new Pipe<ServerResponseSchema>(outgoingDataConfig);
                toApps[a] =  new Pipe<HTTPRequestSchema>(httpRequestPipeConfig);
                headers[a] = newApp(graphManager, toApps[a], fromApps[a], a);
                msgIds[a] =  HTTPRequestSchema.MSG_FILEREQUEST_200;//TODO: add others as needed
            }
            
            CharSequence[] paths = new CharSequence[] {"/%d\n",
                                           "/WebSocket/connect"};
            
            
            
            Pipe<ServerRequestSchema> staticRequestPipe = new Pipe<ServerRequestSchema>(incomingDataConfig);
            incomingGroup[g] = new Pipe[] {staticRequestPipe};
            
            Pool<Pipe<ServerRequestSchema>> pool = new Pool<Pipe<ServerRequestSchema>>(incomingGroup[g]);
            
            
            
            //TODO: revisit this part of the test later.
            Pipe errorPipe = new Pipe(new PipeConfig(RawDataSchema.instance));
            
            
            ConsoleJSONDumpStage dumpErr = new ConsoleJSONDumpStage(graphManager,errorPipe); //TODO: Build error creation stage.
            
            //reads from the socket connection
            ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(graphManager, incomingGroup[g], coordinator, g); //TODO: must take pool 
            
            
            HTTPRouterStage.newInstance(graphManager, pool, toApps, errorPipe, paths, headers, msgIds);        
            
                        
            //writes to the socket connection           
            ServerConnectionWriterStage writerStage = new ServerConnectionWriterStage(graphManager, fromApps, coordinator, g); 
                                       
        }
       
        
        StringBuilder dot = new StringBuilder();
        GraphManager.writeAsDOT(graphManager, dot);
        System.out.println(dot);
                
        
        return graphManager;
    }

    //TODO: build all the application
    private long newApp(GraphManager graphManager, Pipe<HTTPRequestSchema> fromRequest, Pipe<ServerResponseSchema>  toSend, int appId) {
        
        //TODO: build apps to connect these
        //Static file load
        //RestCall
        //WebSocketAPI.
        
        
        //We only support a single component now, the static file loader
        HTTPModuleFileReadStage.newInstance(graphManager, fromRequest, toSend, HTTPSpecification.defaultSpec(), "\rootPath");
        
        return 0;
    }
}
