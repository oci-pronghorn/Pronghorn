package com.ociweb.pronghorn.stage.network;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.HTTP1xRouterStage;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.Pool;
//TODO: read up on chunked and add support, plus length
public class PipelineIntegrationTest {

    private final static boolean testRoutingOnly = false;
    
    private final int fileCount = 8;
    private final int fileSize =4096;
    private final TestDataFiles testDataFiles = new TestDataFiles(new File(System.getProperty("java.io.tmpdir"),"staticFileRequestGeneratorStage"), fileCount, fileSize);
//    
    private final int apps = 1;
    private final CharSequence[] urls = new CharSequence[]{"/root/%b "};
    
    @Ignore
    public void rapidIntegrationTest() {
        
       
        GraphManager gm = new GraphManager();
               
        
        int c = testDataFiles.testFilePaths.length;
        CharSequence[] paths = new CharSequence[c];
        while (--c>=0) {
              String file = "/root/"+testDataFiles.testFilePaths[c].getFileName().toString();
              //System.out.println("request "+file);
              paths[c] = file;
        }
        
        final int iterations = 1_000_000;        
        final PipeConfig<NetPayloadSchema> rawRequestPipeConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 200, 512) ;
        final PipeConfig<HTTPRequestSchema> appPipeConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 100, 512); ///consumers
        
        
        Pipe<NetPayloadSchema> rawRequestPipe = new Pipe<NetPayloadSchema>(rawRequestPipeConfig);
        Pipe[] pipes = new Pipe[]{ rawRequestPipe};

        ClientHTTPRequestDataGeneratorStage genStage = ClientHTTPRequestDataGeneratorStage.newInstance(gm, rawRequestPipe, iterations, paths);  
        
        //route all urls to the same static loader
        HTTP1xRouterStage stage = buildRouterStage(gm, apps, appPipeConfig, pipes, testDataFiles, urls);
               
        runGraph(gm, paths.length, iterations, genStage);
        
        //TODO: add more test asserts into here, to confirm correctness of the test.
        
        
    }

    private void runGraph(GraphManager gm, final int testDataSize, final int iterations, PronghornStage watchStage) {
        
    	GraphManager.exportGraphDotFile(gm, getClass().getSimpleName());
    	
    	boolean monitorPipes = true;
        if (monitorPipes) {
            MonitorConsoleStage.attach(gm);        
        } 
        GraphManager.enableBatching(gm);
        
        
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
      //  scheduler.playNice = false;
        long start = System.currentTimeMillis();

        scheduler.startup();  
        
        //TODO: file read is getting blocked and stops moving.
        gm.blockUntilStageBeginsShutdown(watchStage); //generator gets done very early and begins the shutdown before the route completes so this block is required.
        
        scheduler.awaitTermination(30, TimeUnit.SECONDS);
        
        long duration = System.currentTimeMillis()-start;
        long totalRequests = iterations*(long)testDataSize;
        
        float requestPerMsSecond = totalRequests/(float)duration;
        System.out.println("totalRequests: "+totalRequests+" perMs:"+requestPerMsSecond);
    }

    private HTTP1xRouterStage buildRouterStage(GraphManager gm, final int apps,
            final PipeConfig<HTTPRequestSchema> appPipeConfig, Pipe<NetPayloadSchema>[] pipes, TestDataFiles testDataFiles, CharSequence[] paths) {
        
        Pipe[] routedAppPipes = new Pipe[apps];
        long[] appHeaders = new long[apps];
        int[] msgIds = new int[apps];

        PipeConfig<ServerResponseSchema> responseConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 10000, 20000);
        
        
        int i = apps;
        while (--i >= 0) {
            routedAppPipes[i] = new Pipe<HTTPRequestSchema>(appPipeConfig);
            appHeaders[i] = 0;//NO HEADERS NEEDED FOR FILE READING,  TODO: PUT THIS BACK AND BUILD EXCEPTION CAPTURE FOR THIS ERROR. (1<<HTTPHeaderRequestKeyDefaults.UPGRADE.ordinal());//headers needed.
            msgIds[i] =  HTTPRequestSchema.MSG_FILEREQUEST_200;        
       
            if (testRoutingOnly) {
                PipeCleanerStage.newInstance(gm, routedAppPipes[i]);
            } else {
                FileReadModuleStage fielStage = FileReadModuleStage.newInstance(gm, routedAppPipes[i], new Pipe<ServerResponseSchema>(responseConfig), HTTPSpecification.defaultSpec(), testDataFiles.tempDirectory);  
                PipeCleanerStage.newInstance(gm, gm.getOutputPipe(gm, fielStage));
            }            
        }
        
        PipeConfig<ReleaseSchema> ackConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance);
		Pipe<ReleaseSchema> ack = new Pipe<ReleaseSchema>(ackConfig );
        PipeCleanerStage.newInstance(gm, ack);
		
        HTTP1xRouterStage stage = HTTP1xRouterStage.newInstance(gm, pipes, routedAppPipes, ack, paths, appHeaders, msgIds, null);
        return stage;
    }

    
}
