package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;
import com.ociweb.pronghorn.stage.test.ByteArrayProducerStage;
import com.ociweb.pronghorn.util.ZeroCopyByteArrayOutputStream;

public class SpeedTest {

    //////////////////////////////////
    //THIS IS NOT A UNIT TEST
    //IT DOES NOT RUN WHEN TESTS RUN
    //////////////////////////////////
    
    private static int testSize = 200*1024*1024;// 200MB
    private static final Random r = new Random(42);
    private static final byte[] rawData = new byte[testSize];
    private static final int timeoutSeconds = 30;
    private static int largestPipe = 500;
    
    static {
        /////////////////////////////////////////////////////////////////
        //build repeating blocks that can be detected by compression algo.
        /////////////////////////////////////////////////////////////////
        //we rotate over a set of repeating data blocks and 
        //occationally put in a fully random block that was not repeated
        ////////////////////////////////////////////////////////////////
        int blockSize = 256;
        int blocksCount = 7; //7 repeating blocks and (16-7) random blocks 
        
        byte[][] data = new byte[blocksCount][];
        
        int i = blocksCount;
        while (--i>=0) {
            data[i] = new byte[blockSize];
            r.nextBytes(data[i]);
        }
        byte[] randomBlock = new byte[blockSize];
        
        int limit = testSize/blockSize;
        int j = 0;
        int pos = 0;
        while (j<limit) {
            //find block
            byte[] blockToUse;
            
            if ((0xF&j)>=blocksCount) {
                r.nextBytes(randomBlock);
                blockToUse = randomBlock;
            } else {
                blockToUse = data[0xF&j];
            }
                    
            int length = pos+blockSize>rawData.length ? rawData.length-pos : blockSize;
            System.arraycopy(blockToUse, 0, rawData, pos, length);
            pos += length;
            
            j++;
        } 
    }
    
    public static void main(String[] args) {
        
        /*
         * This test shows that reading and writing blob data can run at full hardware speeds.
         * This was tested on a samsung 840 evo 500GB which has sustaned read and write speeds of ~ 500MB/S
         * Write CPU used was minimal since all the work is done by the hardware.
         * Read CPU used needed almost 1 core since the test validates every byte matches the test data.
         */
        
        //TODO: return median value for each.
        
        try {

                long medBlobReadDuration = fileBlobReadTest();
                long medTapeReadDuration = fileTapeReadTest();
                long medBlobWriteDuration = fileBlobWriteTest(); //faster than tape but to use this a serialize stage wlll be required.
                long medTapeWriteDuration = fileTapeWriteTest(); //slower than blob write but can write any schema as-is
                            
                System.out.println("Test Data size "+(testSize/(1024*1024))+"MB");
                System.out.println("Median BLOB read duration: "+medBlobReadDuration+"ms  "+mBytesPerSecond(medBlobReadDuration)+"MB/s");
                System.out.println("Median Tape read duration: "+medTapeReadDuration+"ms  "+mBytesPerSecond(medTapeReadDuration)+"MB/s");
                System.out.println("Median BLOB write duration: "+medBlobWriteDuration+"ms  "+mBytesPerSecond(medBlobWriteDuration)+"MB/s");
                System.out.println("Median Tape write duration: "+medTapeWriteDuration+"ms  "+mBytesPerSecond(medTapeWriteDuration)+"MB/s");

            
        } catch (IOException e) {
           
            e.printStackTrace();
        }
        
    }

    private static long fileBlobReadTest() throws IOException {
        final ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream(testSize);
        
        File tempFile = fileFullOfTestData();
        
        int largestBlock = testSize;
        
        long[] times = new long[200]; //guess on size
        int timeIdx = 0;
                
        
        int p = largestPipe;
        while (p>=2) {           
            
            int blockSize = largestBlock/p;
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, p, blockSize);
            Pipe<RawDataSchema> loadedDataPipe = new Pipe<RawDataSchema>(config);
                                    
            
            new FileBlobReadStage(gm, new RandomAccessFile(tempFile, "r"), loadedDataPipe);            
            outputStream.reset();
            new ToOutputStreamStage(gm, loadedDataPipe, outputStream, false);
            
                        
            GraphManager.enableBatching(gm);//lower contention over head and tail            
          //  MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            
            long startTime = System.currentTimeMillis();
            scheduler.startup();        
            
            scheduler.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis()-startTime;
            
            times[timeIdx++] = duration;
            
            validateReadData(outputStream);
            
            if (duration>(timeoutSeconds*1000)) {
                System.err.println("unable to get duration, timedout");
            } else {                
                int mbytesPerSecond = mBytesPerSecond(duration);                
                System.out.println("BLOB READ: pipeLength:"+p+" blockSize:"+(blockSize/1024)+"k  duration:"+duration+" totalBlobRingSize:"+loadedDataPipe.sizeOfBlobRing+" MB/S:"+mbytesPerSecond);               
            }
            if (p>200) {
                p-=100;
            } else {
                if (p>30) {
                    p-=10;
                } else {            
                    p-=2;
                }
            }
        }
        
        Arrays.sort(times,0, timeIdx);
        int middle = timeIdx/2;
        return times[middle];
        
    }

    private static int mBytesPerSecond(long duration) {
        return (int)((1000l*testSize)/(duration*1024l*1024l));
    }
    
    private static File fileFullOfTestData() throws IOException, FileNotFoundException {
        File f = File.createTempFile("roundTipTest", "dat");
        f.deleteOnExit();
        
        FileOutputStream fost = new FileOutputStream(f);
        fost.write(rawData);
        fost.close();
        assertEquals(testSize, (int)f.length());
        return f;
    }
    
    private static long fileTapeReadTest() throws IOException {
               
        
        
        final ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream(testSize);
        
        int largestBlock = testSize;
        
        long[] times = new long[200]; //guess on size
        int timeIdx = 0;
        
        
        int p = largestPipe;
        while (p>=2) {           
            
            int blockSize = largestBlock/p;
            int blobRingSize;
            long duration;
            {
                ////////////////////
                //must create a readable file of this size
                ////////////////////
                PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, p, blockSize);
                Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);        
                File tapeFile = TapeRoundTripTest.writeTapeToFileUsingPipe(inputPipe, rawData);
                                
                
                GraphManager gm = new GraphManager();
                
                Pipe<RawDataSchema> loadedDataPipe = new Pipe<RawDataSchema>(config.grow2x());
                blobRingSize = loadedDataPipe.sizeOfBlobRing;
                new TapeReadStage(gm, new RandomAccessFile(tapeFile, "r"), loadedDataPipe); //TODO: Must check that loading pipe is large enough
                
                outputStream.reset();
                new ToOutputStreamStage(gm, loadedDataPipe, outputStream, false);
                
                GraphManager.enableBatching(gm);//lower contention over head and tail            
              //  MonitorConsoleStage.attach(gm);
                
                ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
                
                long startTime = System.currentTimeMillis();
                scheduler.startup();        
                
                scheduler.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
                duration = System.currentTimeMillis()-startTime;
            }
            validateReadData(outputStream);
            
            times[timeIdx++] = duration;
            
            if (duration>(timeoutSeconds*1000)) {
                System.err.println("unable to get duration, timedout");
            } else {                
                int mbytesPerSecond = mBytesPerSecond(duration);                
                System.out.println("TAPE READ: pipeLength:"+p+" blockSize:"+(blockSize/1024)+"k  duration:"+duration+" totalBlobRingSize:"+blobRingSize+" MB/S:"+mbytesPerSecond);               
            }
            if (p>200) {
                p-=100;
            } else {
                if (p>30) {
                    p-=10;
                } else {            
                    p-=2;
                }
            }
        }
        
        Arrays.sort(times,0, timeIdx);
        int middle = timeIdx/2;
        return times[middle];
        
    }

    private static void validateReadData(final ZeroCopyByteArrayOutputStream outputStream) {

        byte[] capturedArray = outputStream.backingArray();
        int i = 0;
        while (i<rawData.length && i<outputStream.backingArrayCount()) {
            if (rawData[i]!=capturedArray[i]) {
                System.err.println("Arrays do not match starting at index "+i);
                break;
            }   
            i++;
        }                
        if (rawData.length!=outputStream.backingArrayCount()) {
            System.err.println("Expected length of "+rawData.length+" but captured array was "+outputStream.backingArrayCount());
        }

    }
    
    
    private static long fileBlobWriteTest() throws IOException {
          
        
        long[] times = new long[200]; //guess on size
        int timeIdx = 0;
        
        
        int largestBlock = testSize;
        
        int p = largestPipe;
        while (p>=2) {           
            
            int blockSize = largestBlock/p;
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, p, blockSize);
            Pipe<RawDataSchema> loadedDataPipe = new Pipe<RawDataSchema>(config);
            
            
            File tempFile = File.createTempFile("blobWrite", "speedTest");        
            tempFile.deleteOnExit();
            
            PronghornStage s1 = new ByteArrayProducerStage(gm, rawData, loadedDataPipe);        
            PronghornStage s2 = new FileBlobWriteStage(gm, loadedDataPipe, new RandomAccessFile(tempFile,"rw"));  //NOTE: use rwd/rws to sync flush with every write (much slower)
            
            GraphManager.enableBatching(gm);//lower contention over head and tail
         //   MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            
            long startTime = System.currentTimeMillis();
            scheduler.startup();        
            
            scheduler.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis()-startTime;
            
            if (tempFile.length() != testSize) {
                System.err.println("file produced is not the right length. "+tempFile.length()+" expected "+testSize);
            }
            
            times[timeIdx++] = duration;
            tempFile.delete();
            
            if (duration>(timeoutSeconds*1000)) {
                System.err.println("unable to get duration, timedout");
            } else {                
                int mbytesPerSecond = mBytesPerSecond(duration);                
                System.out.println("BLOB WRITE: pipeLength:"+p+" blockSize:"+(blockSize/1024)+"k  duration:"+duration+" totalBlobRingSize:"+loadedDataPipe.sizeOfBlobRing+" MB/S:"+mbytesPerSecond);               
            }
            if (p>200) {
                p-=100;
            } else {
                if (p>30) {
                    p-=10;
                } else {            
                    p-=2;
                }
            }
        }
        
        Arrays.sort(times,0, timeIdx);
        int middle = timeIdx/2;
        return times[middle];
        
    }

    //TODO: nice improvements, 1) write MessageSchema at head for strong type checking 2) write max chunk size at head for pipe capacity check up front. 
    
    private static long fileTapeWriteTest() throws IOException {
        
        long[] times = new long[200]; //guess on size
        int timeIdx = 0;
        
        
        int largestBlock = testSize;
        
        int p = largestPipe;
        while (p>=2) {           
            
            int blockSize = largestBlock/p;
            
            GraphManager gm = new GraphManager();
            
            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, p, blockSize);
            Pipe<RawDataSchema> loadedDataPipe = new Pipe<RawDataSchema>(config);
            
            
            File tempFile = File.createTempFile("blobWrite", "speedTest");        
            tempFile.deleteOnExit();
            
            PronghornStage s1 = new ByteArrayProducerStage(gm, rawData, loadedDataPipe);        
            PronghornStage s2 = new TapeWriteStage(gm, loadedDataPipe, new RandomAccessFile(tempFile,"rw"));  //NOTE: use rwd/rws to sync flush with every write (much slower)
            
            GraphManager.enableBatching(gm);//lower contention over head and tail
          //  MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
            
            long startTime = System.currentTimeMillis();
            scheduler.startup();        
            
            scheduler.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis()-startTime;
            
            if (tempFile.length() <= testSize) {
                System.err.println("file produced is not the right length. "+tempFile.length()+" expected something larger than "+testSize);
            }
            tempFile.delete();
            
            times[timeIdx++] = duration;
            
            if (duration>(timeoutSeconds*1000)) {
                System.err.println("unable to get duration, timedout");
            } else {                
                int mbytesPerSecond = mBytesPerSecond(duration);                
                System.out.println("TAPE WRITE: pipeLength:"+p+" blockSize:"+(blockSize/1024)+"k  duration:"+duration+" totalBlobRingSize:"+loadedDataPipe.sizeOfBlobRing+" MB/S:"+mbytesPerSecond);               
            }
            if (p>200) {
                p-=100;
            } else {
                if (p>30) {
                    p-=10;
                } else {            
                    p-=2;
                }
            }
        }
        Arrays.sort(times,0, timeIdx);
        int middle = timeIdx/2;
        return times[middle];
    }
    
    
}
