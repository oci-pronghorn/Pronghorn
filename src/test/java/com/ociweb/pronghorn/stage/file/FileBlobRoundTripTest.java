package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;
import com.ociweb.pronghorn.stage.test.ByteArrayProducerStage;

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
            
            new ByteArrayProducerStage(gm, rawData, inputPipe);
            new FileBlobWriteStage(gm, inputPipe, new RandomAccessFile(f2,"rws")); //TODO: need a FileBlobRead that can tail a file
                        
            GraphManager.enableBatching(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            scheduler.startup();            
            scheduler.awaitTermination(3, TimeUnit.SECONDS);
                        
            confirmFileContentsMatchTestData(f2);
            
        
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
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
            
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
            System.out.println("finished running test");
            
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
