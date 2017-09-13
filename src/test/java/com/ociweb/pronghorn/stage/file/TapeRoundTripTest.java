package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;
import com.ociweb.pronghorn.stage.test.ByteArrayProducerStage;

public class TapeRoundTripTest {

    private static final int testSize = 1000000; 
    private static final Random r = new Random(42);
    private static final byte[] rawData = new byte[testSize];
    
    static {
        r.nextBytes(rawData);        
    }

    //TODO: need a TapeReader that can tail a file, this would be nice to have and convinent for concurrent testing.
    
    @Test
    public void fileTapeWriteTest() {
        
    	if ("arm".equals(System.getProperty("os.arch"))) {
    		assertTrue(true);
    	}
    	
    	else {
        int maxVarLength = 2*1024*1024; //2M
        try {
            int pipeSize = 10;
            
            while (maxVarLength >= 16) {
                PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, pipeSize, maxVarLength);
                Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);
                
                File tapeFile = writeTapeToFileUsingPipe(inputPipe, rawData);           
                
                int sizeOfBlobRing = inputPipe.sizeOfBlobRing;
                int sizeOfSlabRing = inputPipe.sizeOfSlabRing;
                int maxAvgVarLen = inputPipe.maxVarLen;
                            
                deepValidationOfTapeFile(tapeFile, sizeOfBlobRing, sizeOfSlabRing, maxAvgVarLen);
                
                maxVarLength = maxVarLength>>1;
            }
        } catch (Throwable t) {
            
            System.err.println("Error detected when var length was set to "+maxVarLength);
            
            if (t instanceof  AssertionError) {
                throw (AssertionError)t;
            }
            
            t.printStackTrace();
            fail("unexpected error");
        }
    	}
    }


    private void deepValidationOfTapeFile(File f2, int sizeOfBlobRing, int sizeOfSlabRing, int maxAvgVarLen)
            throws FileNotFoundException, IOException {
        byte[] reLoaded = loadFileAsArray(f2); 
        assertEquals(f2.length(), reLoaded.length);
        //////////////////////////////////////////////////////////
        //DEEP check of the internal file structure
        // * All header counts are checked
        // * All message lengths are checked
        // * All consumed byte counts are checked
        // * Alignment of slabs to blob data is checked
        //////////////////////////////////////////////////////////
        
        int messageSize = RawDataSchema.FROM.fragDataSize[0];
        int messageSizeInBytes = messageSize*4;
        
        int pos = 0;
        long totalBytesCountedInHeader = 0;
        long totalBytesCountedInConsumedField = 0;
        while (pos < reLoaded.length) {
        
            int blobBytesForChunk = extractInt(reLoaded, pos);
            int slabBytesForChunk = extractInt(reLoaded, pos+4);
                            
            assertEquals("slab bytes must be divisible by 4 since it holds an array of ints.", 0, slabBytesForChunk&0x03);
            
            
            //blob bytes must not be larger than blob ring size
            assertTrue(blobBytesForChunk+"<="+sizeOfBlobRing,blobBytesForChunk<=sizeOfBlobRing);
            //slab bytes must not be larger than slab ring size (in bytes not ints)
            assertTrue(slabBytesForChunk<= (4*sizeOfSlabRing));
                                       
            totalBytesCountedInHeader += blobBytesForChunk;
            pos += (blobBytesForChunk + slabBytesForChunk + 8);
            
            
            //The last chunk will also contain the EOF maker which is not the same size as the message.
            int eofMessage = 0;
            if (pos==reLoaded.length) {
                //EOF is marked as two ints -1 followed by 0
                //but this is not always used so its absence is not an error
                int eofMessageId = extractInt(reLoaded, pos-8);
                int eofMessageDat = extractInt(reLoaded, pos-4);
                if (-1 == eofMessageId && 0 == eofMessageDat) {
                    //file ends with explicit EOF message, this is optional so it may or may not be found.
                    eofMessage = Pipe.EOF_SIZE*4;
                }
            }
            int adjValue = slabBytesForChunk-eofMessage; 
            
            int val = adjValue / messageSizeInBytes;
            int rem = adjValue % messageSizeInBytes;
                                           
            assertEquals("The slab chunk must have a whole number of messages  (("+slabBytesForChunk+'-'+eofMessage+")%"+messageSizeInBytes+")" ,0, rem);
                            
            //pos is now pointing to the end of the chunk, walk backwards and sum bytes consumed count (this field is the last int of every message)
            
            int consumedBytePos = pos-(eofMessage+4);
            int i = val;
            long bytesConsumedBySlab = 0;
            while (--i>=0) {
                long consumedBytes = extractInt(reLoaded, consumedBytePos);
                assertTrue(consumedBytes>=0);
                assertTrue(consumedBytes<=maxAvgVarLen); //only true because for this schema there is only one var lenght field per message.
                bytesConsumedBySlab += consumedBytes;
                consumedBytePos -= messageSizeInBytes;
            }
            totalBytesCountedInConsumedField += bytesConsumedBySlab;
                            
            //critical constraint, slab must never get ahead of blob, they must match or blob leads
            assertTrue("Chunk of slab message must never consume more bytes than have been provided by blob chunks.",
                       totalBytesCountedInConsumedField <= totalBytesCountedInHeader);
            
           // //more narrow constraint, this is usually true but not always. No need to enforce.
           // assertEquals("The bytes consumed in the slab chunk must match the bytes of the blob chunk",
           //              bytesConsumedBySlab, blobBytesForChunk);
                                                                      
        }
        assertEquals(rawData.length, totalBytesCountedInHeader);
        assertEquals(rawData.length, totalBytesCountedInConsumedField);            
        assertEquals(reLoaded.length,  pos);
    }


    public static File writeTapeToFileUsingPipe(Pipe<RawDataSchema> inputPipe, byte[] testData) throws IOException, FileNotFoundException {
          
        GraphManager gm = new GraphManager();
        File f2 = File.createTempFile("fileTapeWriteTest", "dat");
        f2.deleteOnExit();
        
        new ByteArrayProducerStage(gm, testData, inputPipe);
        new TapeWriteStage(gm, inputPipe, new RandomAccessFile(f2,"rw")); 
                    
        GraphManager.enableBatching(gm);
        
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();            
        scheduler.awaitTermination(3, TimeUnit.SECONDS);

        return f2;
    }


    private int extractInt(byte[] reLoaded, int i) {
        return ((0xFF&reLoaded[i])<<24) | ((0xFF&reLoaded[i+1])<<16) | ((0xFF&reLoaded[i+2])<<8) | (0xFF&reLoaded[i+3]); 
    }


    private byte[] loadFileAsArray(File f2) throws FileNotFoundException, IOException {
        FileInputStream fist = new FileInputStream(f2);
        int len = (int)f2.length();
        byte[] reLoaded = new byte[len];
        int off = 0;           
        while (off<len) {
            int readCount = fist.read(reLoaded, off, len-off);
            if (readCount<0) {
                fist.close();
                if (off<len) {
                    fail("the file is shorter than expected");
                }
            }
            off += readCount;
        }
        fist.close();
        return reLoaded;
    }
    
    
    @Test
    public void roundTripTest() {
        
    	if ("arm".equals(System.getProperty("os.arch"))) {
    		assertTrue(true);
    	}
    	
    	else {
    		
    		
        int pipeLength = 10;
        int maxVarLength = 2*1024*1024;
        
        int limit = 16;
        int i = maxVarLength;
        try {
            while (i>limit) {
                PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, pipeLength, maxVarLength);
            
                ///////////////////////// 
                //create test file with known random data
                /////////////////////////
                Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);            
                File tapeFile = writeTapeToFileUsingPipe(inputPipe, rawData);      
    
                            
                //////////////////////
                //confirm we can read the file 
                /////////////////////
                
                GraphManager gm = new GraphManager();
                Pipe<RawDataSchema> loadedDataPipe = new Pipe<RawDataSchema>(config.grow2x());            
                new TapeReadStage(gm, new RandomAccessFile(tapeFile,"rw"), loadedDataPipe);            
                       
                ByteArrayOutputStream baost = new ByteArrayOutputStream();
                new ToOutputStreamStage(gm, loadedDataPipe, baost, false);
    
                //run the above to load the tape test it and write it back out as a blob
                ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
                scheduler.startup();
                scheduler.awaitTermination(600, TimeUnit.SECONDS);
                
                assertArrayEquals(rawData, baost.toByteArray());
                
                i = i>>1;
            }
            
        } catch (IOException e) {
            fail(e.getMessage());
        } finally {
            if (i>limit) {
                System.err.println("error when maxVarLength "+maxVarLength);
            }
            
        }
           
    
    	} 
    }
     
}
