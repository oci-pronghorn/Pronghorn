package com.ociweb.jfast.mux;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class DriveTest {

    
    
    @Test
    public void testDrive() {
        //In order to get a real measure of the base hardware without memory cashes getting in the way
        //the test must be 8Gbytes up to 32Gbytes
       
        long size = 1l<<22;   //1l<<32;        
        long bufferSize = 1<<19;//26;
        
        
        long startTime = System.nanoTime();
        
        long i = size;
        long start = 0;
        MappedByteBuffer mem=null;
        FileChannel fc=null;
        File temp=null;
        try {
            temp = File.createTempFile("driveTest","test");
            temp.delete();
            fc = new RandomAccessFile(temp, "rw").getChannel();            
            mem = fc.map(FileChannel.MapMode.READ_WRITE, start, bufferSize);
            
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        
        
        while (--i>=0) {
            
            if(!mem.hasRemaining()) {
                    start+=mem.position();
                    try {
                        mem =fc.map(FileChannel.MapMode.READ_WRITE, start, bufferSize);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
            }
            
            mem.putLong(i);
            
        }        
        
        try {
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        long duration = System.nanoTime()-startTime;
        long fileSize = temp.length()>>20;
        
        System.err.println(duration+" ns");
        System.err.println(fileSize+" MBytes");
        float mBytesPerSecond = 1000f*temp.length()/duration;
        System.err.println(mBytesPerSecond+ " mBps");
        
        
    }
    
}
