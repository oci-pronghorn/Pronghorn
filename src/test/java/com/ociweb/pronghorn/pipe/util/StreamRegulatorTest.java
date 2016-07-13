package com.ociweb.pronghorn.pipe.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class StreamRegulatorTest {

    
    @Test
    public void simpleStreamWriteReadTest() {
        
        long mbps = 100L*1024L*1024L*1024L;
        
        {            
        
            int testSize = 10000;  //at 1MB per write we can hit 100Gbps
            int inFlight = 300;
            int iterations = 1000;//Integer.MAX_VALUE;//10000;
            
            
            byte[] testdata = new byte[testSize];
            int i = testSize;
            while (--i>=0) {
                testdata[i]=(byte)i;
            }
                       
            long mega = 1024*1024;
            long bitPerSecond = mbps*mega; //Go no faster than this
            int maxWrittenChunksInFlight   = inFlight;
            int maxWrittenChunkSizeInBytes = testSize;
            StreamRegulator sr = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
            
            
            byte[] target = new byte[testSize];
            DataOutputBlobWriter<RawDataSchema> out = sr.getBlobWriter();
            DataInputBlobReader<RawDataSchema> in = sr.getBlobReader();
            int a = iterations;
            int b = iterations;
            
            long writeCount = 0;
            long readCount = 0;
            
            final long specialLongValue = Long.MIN_VALUE;
            final int specialIntValue = Integer.MIN_VALUE;
            assertEquals(Integer.MIN_VALUE, 0-Integer.MIN_VALUE);
            assertEquals(Long.MIN_VALUE, 0-Long.MIN_VALUE);
            
            long startTime = System.currentTimeMillis();
            while (a>0 || b>0) {
                boolean isStuck = true;
                while (sr.hasRoomForChunk() && --a>=0) {          
                    isStuck = false;
                    try {
                      //  System.out.println("write "+writeCount);
                       
                        // out.write(testdata);
                        
                        out.writeLong(writeCount);
                        out.writeShort((short)writeCount);
                        out.writeByte((byte)writeCount);
                        
                        out.writePackedLong(writeCount);
                        out.writePackedInt((int)writeCount);
                        
                        out.writePackedLong(specialLongValue);
                        out.writePackedInt(specialIntValue);
                        
                        out.writeUTF(Long.toHexString(writeCount));
                        
                        writeCount++;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                
                while (sr.hasNextChunk() && --b>=0) {    
                    isStuck = false;
   
                      //  int length = in.read(target);
                        long actualLongCount = in.readLong();
                        assertEquals(readCount, actualLongCount);
                        assertEquals((short)readCount, in.readShort());
                        assertEquals((byte)readCount, in.readByte());
                        
                        assertEquals(readCount, in.readPackedLong());
                        assertEquals((int)readCount, in.readPackedInt());
                        
                        assertEquals(specialLongValue, in.readPackedLong());
                        assertEquals(specialIntValue, in.readPackedInt());
                        
                        assertEquals(Long.toHexString(readCount), in.readUTF());
                        
                        
                        readCount++;

                }
                if (isStuck) {
                    System.err.println(sr);
                    fail();
                }
            }
           
        } 
    }
    
    @Test
    public void simpleStreamSpeedLimitTest() {
        
        long topMbps = 2000;
        long step = 100;  
        
        for(long mbps = topMbps; mbps>0; mbps-=step ) {            
        
            int testSize = 10000;  //at 1MB per write we can hit 100Gbps
            int inFlight = 300;
            int iterations = 1000;//Integer.MAX_VALUE;//10000;
            
            
            byte[] testdata = new byte[testSize];
            int i = testSize;
            while (--i>=0) {
                testdata[i]=(byte)i;
            }
                       
            long mega = 1024*1024;
            long bitPerSecond = mbps*mega; //Go no faster than this
            int maxWrittenChunksInFlight   = inFlight;
            int maxWrittenChunkSizeInBytes = testSize;
            StreamRegulator sr = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
            
            
            byte[] target = new byte[testSize];
            DataOutputBlobWriter<RawDataSchema> out = sr.getBlobWriter();
            DataInputBlobReader<RawDataSchema> in = sr.getBlobReader();
            int a = iterations;
            int b = iterations;
            
            long writeCount = 0;
            long readCount = 0;
            
            long startTime = System.currentTimeMillis();
            while (a>0 || b>0) {
                boolean isStuck = true;
                while (sr.hasRoomForChunk() && --a>=0) {          
                    isStuck = false;
                    try {
                        out.write(testdata);                        
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                
                while (sr.hasNextChunk() && --b>=0) {    
                    isStuck = false;
 
                        int length = in.read(target);

                        if (length != testSize) {
                            System.err.println("Length read:"+length);
                            System.err.println(sr);
                            fail();
                        }
                        if (!Arrays.equals(testdata, target)) {
                            System.err.println("Expected:"+Arrays.toString(testdata));
                            System.err.println("Measured:"+Arrays.toString(target));
                            System.err.println("Remaining bytes "+in.available());
                            System.err.println("Length read:"+length);
                            System.err.println(sr);
                            fail();
                        }

                }
                if (isStuck) {
                    System.err.println(sr);
                    fail();
                }
            }
            long duration = System.currentTimeMillis()-startTime; 
            long expectedBytesTotal = iterations*(long)testSize;
            long bitsSent = expectedBytesTotal*8;        
                    
            long bitsPerSecond = bitsSent*1000/duration;
            
            assertEquals(expectedBytesTotal,sr.getBytesWritten());
            assertEquals(expectedBytesTotal,sr.getBytesRead());
            
            long measuredMbps = bitsPerSecond/mega;
            assertTrue("Expected "+measuredMbps+"<"+mbps,measuredMbps < mbps);            
        } 
    }
    
    
    
}
