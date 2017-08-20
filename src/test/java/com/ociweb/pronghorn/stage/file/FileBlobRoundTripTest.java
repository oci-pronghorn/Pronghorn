package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileControlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileResponseSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;
import com.ociweb.pronghorn.stage.test.ByteArrayProducerStage;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class FileBlobRoundTripTest {

    private static final int testSize = 200000; 
    private static final Random r = new Random(42);
    private static final byte[] rawData = new byte[testSize];
    
    static {
        r.nextBytes(rawData);        
    }
    
    @Test
    public void fileBlobWriteTest() {
        
        try {
        
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10, 65536);
            Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);
            
            File f2 = File.createTempFile("roundTipTestB", "dat");
            f2.deleteOnExit();
            
            boolean append = false;
            new ByteArrayProducerStage(gm, rawData, inputPipe);
            FileBlobWriteStage lastStage = new FileBlobWriteStage(gm, inputPipe, append, f2.getAbsolutePath()); //TODO: need a FileBlobRead that can tail a file
                        
            GraphManager.enableBatching(gm);
            
            
            NonThreadScheduler scheduler= new NonThreadScheduler(gm);
            scheduler.startup();
            
            while (!GraphManager.isStageTerminated(gm, lastStage.stageId)) {
            	scheduler.run();
            }
            scheduler.shutdown();
         
                        
            confirmFileContentsMatchTestData(f2);
            
        
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
    
    @Test
    public void testReadWriteStage() {
    	
        File file = null;
    	 
		try {
			file = File.createTempFile("testReadWriteStageTest", "dat");
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	 
    	 
    	 String[] paths = new String[] {file.getAbsolutePath()};
    	 GraphManager gm = new GraphManager();
	 
    	 
    	 Pipe<SequentialFileControlSchema>[] control = new Pipe[]{SequentialFileControlSchema.instance.newPipe(10, 1000)};
		 Pipe<RawDataSchema>[] input = new Pipe[]{RawDataSchema.instance.newPipe(10, 1000)};
		 
		 Pipe<RawDataSchema>[] output = new Pipe[]{RawDataSchema.instance.newPipe(10, 1000)};
		 Pipe<SequentialFileResponseSchema>[] response = new Pipe[]{SequentialFileResponseSchema.instance.newPipe(10, 1000)};
		
		 
		 control[0].initBuffers();
		 input[0].initBuffers();
		 
		 //data to be written
		 Pipe.addMsgIdx(input[0], 0);
		 Pipe.addByteArray("hello".getBytes(), input[0]);
		 Pipe.confirmLowLevelWrite(input[0]);
		 Pipe.publishWrites(input[0]);
		 
		 SequentialFileControlSchema.publishIdToSave(control[0], 123);
		 
		 //this replay command is expected to only happen after the data is written since pipe has data.
		 SequentialFileControlSchema.publishReplay(control[0]);
		 SequentialFileControlSchema.publishMetaRequest(control[0]);
		 
		 PipeWriter.publishEOF(control[0]);
		 
		 
		 SequentialFileReadWriteStage readWriteStage = new SequentialFileReadWriteStage(gm, control, response, input, output, paths);
    	
		 StringBuilder outputData = new StringBuilder();
		 StringBuilder responseData = new StringBuilder();
		 
		 ConsoleJSONDumpStage watch = ConsoleJSONDumpStage.newInstance(gm, output[0],outputData);
		 ConsoleJSONDumpStage.newInstance(gm, response[0], responseData);
		 
         NonThreadScheduler scheduler= new NonThreadScheduler(gm);
         scheduler.startup();
         while (    (!GraphManager.isStageTerminated(gm, watch.stageId)) ) {
         	scheduler.run();
         }
         scheduler.shutdown();
    	
         String responseString = responseData.toString();
         assertTrue(responseString, responseString.indexOf("WriteAck")>=0);
         assertTrue(responseString, responseString.indexOf("{\"Size\":5}")>=0);
         
         String outputString = outputData.toString();
         assertTrue(outputString, outputString.indexOf("0x68,0x65,0x6c,0x6c,0x6f")>=0);
                 
    }
    
    
    @Test
    public void roundTripTest() {
        
                 
        try {            
            ///////////////////////// 
            //create test file with known random data
            /////////////////////////
            File f = fileFullOfTestData();
            
            File f2 = File.createTempFile("roundTipTest", "dat");
            f2.deleteOnExit();
                        
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10, 65536);
            
            Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);      
            Pipe<RawDataSchema> midCheckPipe = new Pipe<RawDataSchema>(config.grow2x());
            Pipe<RawDataSchema> outputPipe = new Pipe<RawDataSchema>(config.grow2x());
            
            new FileBlobReadStage(gm, inputPipe,f.getAbsolutePath());
            new ReplicatorStage(gm, inputPipe, midCheckPipe, outputPipe);
            
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(testSize);
            ToOutputStreamStage lastStage1 = new ToOutputStreamStage(gm, midCheckPipe, outputStream, false);
            
           // ConsoleStage cs = new ConsoleStage(gm, midCheckPipe);
            
            boolean append = false;
            FileBlobWriteStage lastStage2 = new FileBlobWriteStage(gm, outputPipe, append, f2.getAbsolutePath());
                        
            //MonitorConsoleStage.attach(gm);

           // gm.enableTelemetry(8089);
            
            NonThreadScheduler scheduler= new NonThreadScheduler(gm);
            scheduler.startup();
            while (    (!GraphManager.isStageTerminated(gm, lastStage1.stageId))
            		|| (!GraphManager.isStageTerminated(gm, lastStage2.stageId))) {
            	scheduler.run();
            }
            scheduler.shutdown();
            
            
            //when done check the captured bytes from teh middle to ensure they match
            assertArrayEquals(rawData, outputStream.toByteArray());
           
            //when done read the file from disk one more time and confirm its the same            
            confirmFileContentsMatchTestData(f2);
            
            
        } catch (IOException e) {
            fail(e.getMessage());
        }
           
        
    }

    private void confirmFileContentsMatchTestData(File f2) throws FileNotFoundException, IOException {
        FileInputStream fist = new FileInputStream(f2);
        byte[] reLoaded = new byte[testSize];
        int off = 0;           
        while (off<testSize) {
            int readCount = fist.read(reLoaded, off, testSize-off);
            if (readCount<0) {
                fist.close();
                if (off<testSize) {
                    fail("the file is shorter than expected");
                }
                return;
            }
            off += readCount;
        }
        fist.close(); 
         
        assertArrayEquals(rawData, reLoaded);
    }

    private File fileFullOfTestData() throws IOException, FileNotFoundException {
        File f = File.createTempFile("roundTipTest", "dat");
        f.deleteOnExit();
        
        FileOutputStream fost = new FileOutputStream(f);
        fost.write(rawData);
        fost.close();
        assertEquals(testSize, (int)f.length());
        return f;
    }
    
    
    
    
}
