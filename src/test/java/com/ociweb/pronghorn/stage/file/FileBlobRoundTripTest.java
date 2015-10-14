package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.ConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;

public class FileBlobRoundTripTest {

    int testSize = 1000000; 
    
    @Test
    public void roundTripTest() {
        
         
        Random r = new Random(42);
        
        
        byte[] rawData = new byte[testSize];
        r.nextBytes(rawData);
        
        try {
            
            ///////////////////////// 
            //create test file with known random data
            /////////////////////////
            File f = File.createTempFile("roundTipTest", "dat");
            f.deleteOnExit();
            
            FileOutputStream fost = new FileOutputStream(f);
            fost.write(rawData);
            fost.close();
            assertEquals(testSize, (int)f.length());
            
            File f2 = File.createTempFile("roundTipTest", "dat");
            f2.deleteOnExit();
            
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10, 4096);
            
            Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);      
            Pipe<RawDataSchema> midCheckPipe = new Pipe<RawDataSchema>(config.grow2x());
            Pipe<RawDataSchema> outputPipe = new Pipe<RawDataSchema>(config.grow2x());
            
            new FileBlobReadStage(gm, new RandomAccessFile(f,"r"), inputPipe);
            new SplitterStage(gm, inputPipe, midCheckPipe, outputPipe);
            
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(testSize);
            new ToOutputStreamStage(gm, midCheckPipe, outputStream, false);
            
           // ConsoleStage cs = new ConsoleStage(gm, midCheckPipe);
            
            new FileBlobWriteStage(gm, outputPipe, new RandomAccessFile(f2,"rws"));
                        
            MonitorConsoleStage.attach(gm);
            
            System.out.println("running test");
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.startup();
            
            scheduler.awaitTermination(3, TimeUnit.SECONDS);
            System.out.println("finished running test");
            
            //when done check the captured bytes from teh middle to ensure they match
            assertArrayEquals(rawData, outputStream.toByteArray());
           
            //when done read the file from disk one more time and confirm its the same
            
            FileInputStream fist = new FileInputStream(f2);
            byte[] reLoaded = new byte[testSize];
            int off = 0;           
            while (off<testSize) {
                off += fist.read(reLoaded, off, testSize-off);
            }
            fist.close(); 
           
            assertArrayEquals(rawData, reLoaded);
            
            
        } catch (IOException e) {
            fail(e.getMessage());
        }
           
        
    }
    
    
    
    
}
