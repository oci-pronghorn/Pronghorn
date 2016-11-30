package com.ociweb.pronghorn.stage.network;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.*;

import com.ociweb.pronghorn.network.ServerConnectionReaderStage;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerNewConnectionStage;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class ServerConnectionReaderStageTest {

    private final CharSequence[] pathTemplates = new CharSequence[] {
            "/root/%b ", 
            "/Unrequested"
    };
    
    @Ignore    
    public void manualTest() {
        PipeConfig<NetPayloadSchema> rawRequestPipeConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 1000, 2048);//defines largest read chunk /2
        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 500);  
                
        GraphManager gm = new GraphManager();
                
        Pipe<NetPayloadSchema>[] output = new Pipe[]{new Pipe<NetPayloadSchema>(rawRequestPipeConfig)};
                
        //TODO: add support for more than one in this test.
        int socketGroups = 1;  //only have 1 group listener for this test
        int socketGroupId = 0; //we only have the 0th reader in use.
        
        int port = 8089;
        ServerCoordinator coordinator = new ServerCoordinator(socketGroups, port, 1, 15);
        
        ///////////////////////
        Pipe<NetPayloadSchema> rawRequestPipe = new Pipe<NetPayloadSchema>(rawRequestPipeConfig);
        

        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(gm, coordinator, newConnectionsPipe,2);
        
        ConsoleJSONDumpStage<ServerConnectionSchema> connectionNotice = new ConsoleJSONDumpStage<ServerConnectionSchema>(gm, newConnectionsPipe);

        PipeConfig<NetParseAckSchema> ackConfig = new PipeConfig<NetParseAckSchema>(NetParseAckSchema.instance);
		Pipe<NetParseAckSchema> ack = new Pipe<NetParseAckSchema>(ackConfig );
        
        ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(gm, new Pipe[]{ack}, output, coordinator, socketGroupId, false);

        int i = output.length;
        while (--i>=0) { 
            ConsoleJSONDumpStage<NetPayloadSchema> contentDump = new ConsoleJSONDumpStage<NetPayloadSchema>(gm, output[i]);
        }
        
         MonitorConsoleStage.attach(gm);
      //   GraphManager.enableBatching(gm);
         
         ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
         System.out.println("started");

         scheduler.startup();  
                  
         try {
        	 System.out.println("waiting for URL hits");
			Thread.sleep(240_000);
			
		 } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 }
         System.out.println("begin shutdown");
         newConStage.requestShutdown();
         readerStage.requestShutdown();
         scheduler.shutdown();
         
         scheduler.awaitTermination(30, TimeUnit.MINUTES);
        
        
    }
    
    @Ignore
    public void rapidServerConnectionReaderTest() {
        
        PipeConfig<NetPayloadSchema> rawRequestPipeConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 1000, 2048);//defines largest read chunk /2
        PipeConfig<ServerConnectionSchema> newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 500);  
        
        
        int fileCount = 60; //also defines batch size for pipelined requests.
        int fileSize = 0;//large file size not needed for socket test, 4096;//2048;
        int apps = pathTemplates.length;
        int iterations = 8_000;//10_000;//must be more than 1000 to get hot spot opti
        
        int connections = 30;//1000;
        
        
        GraphManager gm = new GraphManager();
                
        Pipe<NetPayloadSchema>[] output = new Pipe[]{new Pipe<NetPayloadSchema>(rawRequestPipeConfig)};
                
        //TODO: add support for more than one in this test.
        int socketGroups = 1;  //only have 1 group listener for this test
        int socketGroupId = 0; //we only have the 0th reader in use.
        
        int port = 8089;
        ServerCoordinator coordinator = new ServerCoordinator(socketGroups, port, 1, 15);
        
        ///////////////////////
        Pipe<NetPayloadSchema> rawRequestPipe = new Pipe<NetPayloadSchema>(rawRequestPipeConfig);
        
        //multiple channels/connections       
        ClientHTTPSocketRequestGeneratorStage[] gens = new ClientHTTPSocketRequestGeneratorStage[connections];
        int j = connections;
        while (--j>=0) {
            gens[j] = addGeneratorStage(fileCount, fileSize, iterations, gm, port, "staticFileRequestGeneratorStage"+j); 
        }
        
        Pipe<ServerConnectionSchema> newConnectionsPipe = new Pipe<ServerConnectionSchema>(newConnectionsConfig);
        ServerNewConnectionStage newConStage = new ServerNewConnectionStage(gm, coordinator, newConnectionsPipe,2);
        PipeCleanerStage<ServerConnectionSchema> newConnectionNotice = new PipeCleanerStage<>(gm, newConnectionsPipe);
        
        PipeConfig<NetParseAckSchema> ackConfig = new PipeConfig<NetParseAckSchema>(NetParseAckSchema.instance);
		Pipe<NetParseAckSchema> ack = new Pipe<NetParseAckSchema>(ackConfig );
        
        ServerConnectionReaderStage readerStage = new ServerConnectionReaderStage(gm, new Pipe[]{ack}, output, coordinator, socketGroupId, false);
        
        long totalRequests = iterations*(long)fileCount*(long)connections;
        
        int i = output.length;
        while (--i>=0) { //TODO: confirm squence number is incrmenting.
            PipeCleanerStage<NetPayloadSchema> dump = new PipeCleanerStage<NetPayloadSchema>(gm, output[i]);
           // ConsoleJSONDumpStage<NetPayloadSchema> dump = new ConsoleJSONDumpStage<NetPayloadSchema>(gm, output[i]);
        }
        
         MonitorConsoleStage.attach(gm);
         GraphManager.enableBatching(gm);
         
         ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
         System.out.println("started");
         long start = System.currentTimeMillis();
         
         scheduler.startup();  
         
         //block until all the data is generated
         j = connections;
         while (--j>=0) {
             gm.blockUntilStageBeginsShutdown(gens[j]);
         }
         newConStage.requestShutdown();
         readerStage.requestShutdown();
         
         scheduler.awaitTermination(30, TimeUnit.MINUTES);
        
         
         long duration = System.currentTimeMillis()-start;
         float requestPerMsSecond = totalRequests/(float)duration;
         System.out.println("totalRequests: "+totalRequests+" perMs:"+requestPerMsSecond);
                
        
    }


    protected ClientHTTPSocketRequestGeneratorStage addGeneratorStage(int fileCount, int fileSize, int iterations,
            GraphManager gm, int port, String baseFolder) {
        TestDataFiles testDataFiles = new TestDataFiles(new File(System.getProperty("java.io.tmpdir"),baseFolder), fileCount, fileSize);
        int c =  testDataFiles.testFilePaths.length;
        CharSequence[] pathRequests = new CharSequence[c];
        while (--c>=0) {
            String file = "/root/"+testDataFiles.testFilePaths[c].getFileName().toString();
            //System.out.println("request "+file);
            pathRequests[c] = file;
        }        
        ClientHTTPSocketRequestGeneratorStage genStage = ClientHTTPSocketRequestGeneratorStage.newInstance(gm, iterations, pathRequests, port);
        return genStage;
    }
    
    
    
}
