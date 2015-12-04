package com.ociweb.pronghorn.pipe.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.junit.Test;

public class StreamRegulatorTest {

    
    @Test
    public void simpleStreamTest() {
        
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
            OutputStream out =  sr.getOutputStream();
            InputStream in = sr.getInputStream();
            int a = iterations;
            int b = iterations;
            
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
                    try {                    
                        
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
                    } catch (IOException e) {
                        throw new RuntimeException(e);
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
