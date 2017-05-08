package com.ociweb.pronghorn.stage.network;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStage;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;

public class HTTPRouterStageTest {
        
    
    private final CharSequence[] paths = new CharSequence[] {
            "/hello/x/x%b",
            "/hello/x/longer",
            "/hello/myfile",
            "/hello/x/zlonger",            
            "/hello/zmyfile",
            "/hello/x/qlonger",
            "/hello/qmyfile",
            "/elsewhere/this/is/the/longest/path/to/be/checked",
            "/elsewhere/myfile"
    };
    
    @Ignore
    public void rapidValidRequestTest() {
        
    	HTTP1xRouterStageConfig routerConfig = new HTTP1xRouterStageConfig(HTTPSpecification.defaultSpec()); 
    	ServerCoordinator coordinator = new ServerCoordinator(false, "127.0.0.1", 8080, 5 ,5 ,1);
    	
    	for(CharSequence route: paths) {
    		routerConfig.registerRoute(route, IntHashTable.EMPTY); //no headers
    	}
    	
    	
        GraphManager gm = new GraphManager();
        
        final int apps = paths.length;
        final int iterations = 1_000_000;        
        final PipeConfig<NetPayloadSchema> rawRequestPipeConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 200, 512) ;
        final PipeConfig<HTTPRequestSchema> appPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 100, 512); ///consumers
        
        
        Pipe<NetPayloadSchema> rawRequestPipe = new Pipe<NetPayloadSchema>(rawRequestPipeConfig);               
        Pipe[] pipes = new Pipe[]{ rawRequestPipe};
        
        PipeConfig<ReleaseSchema> ackConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance);
		Pipe<ReleaseSchema> ack = new Pipe<ReleaseSchema>(ackConfig );
	 
		Pipe<ServerResponseSchema> errorResponsePipe = ServerResponseSchema.instance.newPipe(4, 512);
		PipeCleanerStage.newInstance(gm, errorResponsePipe);
		 
		 
		PipeCleanerStage<ReleaseSchema> cleaner = new PipeCleanerStage<ReleaseSchema>(gm, ack);
        
        PronghornStage stage = ClientHTTPRequestDataGeneratorStage.newInstance(gm, rawRequestPipe, iterations, paths);  
        HTTP1xRouterStage stage2 = buildRouterStage(gm, apps, appPipeConfig, pipes, errorResponsePipe, ack, routerConfig, coordinator);
               
        runGraph(gm, apps, iterations, stage2);
        
        //TODO: add more test asserts into here, to confirm correctness of the test.
        
        
    }

    private void runGraph(GraphManager gm, final int apps, final int iterations, PronghornStage stage) {
        boolean monitorPipes = false;
        if (monitorPipes) {
            MonitorConsoleStage.attach(gm);        
        } 
        GraphManager.enableBatching(gm);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        long start = System.currentTimeMillis();

        scheduler.startup();  
        
        if (monitorPipes) {
          try {
              Thread.sleep(1000);
          } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
          }
        }
        
        gm.blockUntilStageBeginsShutdown(stage); //generator gets done very early and begins the shutdown before the route completes so this block is required.
        
        scheduler.awaitTermination(30, TimeUnit.SECONDS);
        
        long duration = System.currentTimeMillis()-start;
        long totalRequests = iterations*(long)apps;
        
        float requestPerMsSecond = totalRequests/(float)duration;
        System.out.println("totalRequests: "+totalRequests+" perMs:"+requestPerMsSecond);
    }

    private HTTP1xRouterStage buildRouterStage(GraphManager gm, final int apps,
            final PipeConfig<HTTPRequestSchema> appPipeConfig, Pipe<NetPayloadSchema>[] pipes, Pipe<ServerResponseSchema> errorResponsePipe, Pipe<ReleaseSchema> ack, HTTP1xRouterStageConfig routerConfig, ServerCoordinator coordinator) {


    	Pipe[] routedAppPipes = new Pipe[apps];

        int i = apps;
        while (--i >= 0) {
            routedAppPipes[i] = new Pipe<HTTPRequestSchema>(appPipeConfig);

            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000, 
                    PipeCleanerStage.newInstance(gm, routedAppPipes[i])
                    
                    );
                  
            
            
        }
        
        
        //TODO: revisit this part of the test later.
        Pipe errorPipe = new Pipe(new PipeConfig(RawDataSchema.instance));
        ConsoleJSONDumpStage dump = new ConsoleJSONDumpStage(gm,errorPipe);
        
        
		HTTP1xRouterStage stage = HTTP1xRouterStage.newInstance(gm, pipes, new Pipe[][]{routedAppPipes}, errorResponsePipe, ack, routerConfig, coordinator);
        return stage;
    }
 
}
