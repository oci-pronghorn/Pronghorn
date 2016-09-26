package com.ociweb.pronghorn.stage.network;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert.*;
import org.junit.Assert;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTableVisitor;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPSpecification;
import com.ociweb.pronghorn.stage.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.stage.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class HTTPModuleFileReadStageTest {

    
    //NOTE: stages can be limited by -Data throughput or -Cycle rate
    //    * Throughput is helpful when controlling data bus usage 
    //    * Cycle rate helps when there are large windows with no traffic
    //TODO: BB, Add auto adjust cycle delay, if output is empty it was too short, if output is full it was too long.
    
    //TODO: urgent fix must load all files and use faster optimized query.
    
    //test not complete yet.
    @Test
    public void rapidValidReadTest() {
        
        GraphManager gm = new GraphManager();

        //1 -20 gb is not hard to saturate, above this is scales more slowly and it never reaches 40 gb (may work on new PCIe hardware...)
        int fileCount = 8;
        int fileSize = 3000;//selected to make the output 20gbs
        int iterations = 500_000;
        HTTPVerbDefaults verb = HTTPVerbDefaults.GET;//HTTPVerbDefaults.HEAD; //HTTPVerbDefaults.GET;
        
        System.out.println("fileSize is "+fileSize);
                
        long totalRequests = iterations*(long)fileCount;
        long expectedBytes = totalRequests*fileSize;

        PipeConfig<HTTPRequestSchema> requestConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 100, 80);
        PipeConfig<ServerResponseSchema> responseConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 1000, 40000);
                
        Pipe<HTTPRequestSchema> requestPipe =  new Pipe<HTTPRequestSchema>(requestConfig);
        Pipe<ServerResponseSchema> responsePipe = new Pipe<ServerResponseSchema>(responseConfig);
               
        StaticFileRequestGeneratorStage gs = StaticFileRequestGeneratorStage.newInstance(gm, requestPipe, iterations, fileCount, fileSize, verb);  
        //PipeCleanerStage.newInstance(gm, requestPipe);//generates about 15m req/sec
                
        HTTPModuleFileReadStage.newInstance(gm, requestPipe, responsePipe, HTTPSpecification.defaultSpec(), gs.tempFolder());
        
        
        //TODO: AAAA, add stage to validate responses
        
        PipeCleanerStage.newInstance(gm, responsePipe);
        
                
        
        
//              MonitorConsoleStage.attach(gm);        
        GraphManager.enableBatching(gm);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        
        long start = System.currentTimeMillis();       
        scheduler.startup();  
        
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        
        scheduler.awaitTermination(60, TimeUnit.MINUTES);      
                
        
        long duration = System.currentTimeMillis()-start;
        float requestPerMsSecond = totalRequests/(float)duration;
        System.out.println("totalFileRequests: "+totalRequests+" perMs:"+requestPerMsSecond+" duration:"+duration);
        System.out.println("data bytes:"+expectedBytes+" plus headers");
        
        
    }
    
    //TODO: add generator for fuzz test of invalid file name.
    
    @Test
    public void testFileExtCollide() {
        IntHashTable table = HTTPModuleFileReadStage.buildFileExtHashTable(HTTPContentTypeDefaults.class);
       
        final AtomicInteger count = new AtomicInteger();
        
        IntHashTableVisitor visitor = new IntHashTableVisitor(){

          @Override
          public void visit(int key, int value) {
             //System.out.println("key "+key+" value "+value);
             
             HTTPContentTypeDefaults type = HTTPContentTypeDefaults.values()[value];
             
             Assert.assertEquals( HTTPModuleFileReadStage.extHash(type.fileExtension()), key);

             count.incrementAndGet();             
              
          }
          
       };
      IntHashTable.visit(table, visitor );
      
      int c = HTTPContentTypeDefaults.values().length;
      int total = 0;
      while (--c>=0) {
          if (!HTTPContentTypeDefaults.values()[c].isAlias()) {
              total++;
          }
      }      
      Assert.assertEquals(count.get(),total);
      
    }
    
    
    
}
