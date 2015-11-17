package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class DataInputOutputTest {

    private static final int testSpace = 100000000;
    
    private static final PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 5, testSpace);
        
    
    
    private long testLongValueGenerator(Random r, int seed) {
        
        int powOf2 = seed & 0x3F;
        long range = (1l<<powOf2);
        long maskValue = range-1l;
        
        
        long testValue =   ((r.nextLong()&maskValue) | range );
        
        //System.out.println(testValue);
        return testValue;
    }
    
    private int testIntValueGenerator(Random r, int seed) {
        
        int powOf2 = seed & 0x1F;
        int range = (1<<powOf2);
        int maskValue = range-1;
                
        int testValue =   ((r.nextInt()&maskValue) | range );
        
        //System.out.println(testValue);
        return testValue;
    }
    
    
    @Test
    public void testPackedLong() {
        int testSize = testSpace/10;
        Random r;
        Pipe<RawDataSchema> testPipe = new Pipe<RawDataSchema>(config);        
        
        testPipe.initBuffers();
        
        DataOutputBlobWriter<RawDataSchema> out = new DataOutputBlobWriter<>(testPipe);
        DataInputBlobReader<RawDataSchema> in = new DataInputBlobReader<>(testPipe);
        
        
        assertTrue(PipeWriter.tryWriteFragment(testPipe, 0));
        
         
        out.openField();
        
        r = new Random(101);
        long start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            out.writePackedLong(testLongValueGenerator(r,i));
        }
        long duration = System.nanoTime()-start;
        long nsPerWrite = duration/testSize;
        
        int length = out.closeField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        
        float compression = 1f-(length/(float)(testSize*8));
        
        System.out.println(nsPerWrite+"ns per written long, longs per second "+(1000l*1000l*1000l/nsPerWrite)+" compression "+compression);
        
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

        r = new Random(101);
        start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            long expected = testLongValueGenerator(r,i);
            long actual = in.readPackedLong();
            if (expected!=actual) {
                String expBinaryString = Long.toBinaryString(expected);
                String actBinaryString = Long.toBinaryString(actual);
                while (actBinaryString.length()<expBinaryString.length()) {
                    actBinaryString = '0'+actBinaryString;
                }
                
                System.err.println("Expected:"+expBinaryString);
                System.err.println("Actual  :"+actBinaryString);                
                assertEquals(expected, actual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
        System.out.println(nsPerRead+"ns per read long, longs per second "+(1000l*1000l*1000l/nsPerRead));
       
        
    }
    
    
    @Test
    public void testPackedInt() {
        int testSize = testSpace/5;
        Random r;
        Pipe<RawDataSchema> testPipe = new Pipe<RawDataSchema>(config);        
        
        testPipe.initBuffers();
        
        DataOutputBlobWriter<RawDataSchema> out = new DataOutputBlobWriter<>(testPipe);
        DataInputBlobReader<RawDataSchema> in = new DataInputBlobReader<>(testPipe);
        
        
        assertTrue(PipeWriter.tryWriteFragment(testPipe, 0));
        
         
        out.openField();
        
        r = new Random(101);
        long start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            out.writePackedInt(testIntValueGenerator(r,i));
        }
        long duration = System.nanoTime()-start;
        long nsPerWrite = duration/testSize;
        
        
        
        int length = out.closeField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        
        float compression = 1f-(length/(float)(testSize*4));
        System.out.println(nsPerWrite+"ns per written int, ints per second "+(1000l*1000l*1000l/nsPerWrite)+" compression "+compression);
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

        r = new Random(101);
        start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            int expected = testIntValueGenerator(r,i);
            int actual = in.readPackedInt();
            if (expected!=actual) {
                String expBinaryString = Long.toBinaryString(expected);
                String actBinaryString = Long.toBinaryString(actual);
                while (actBinaryString.length()<expBinaryString.length()) {
                    actBinaryString = '0'+actBinaryString;
                }
                
                System.err.println("Expected:"+expBinaryString);
                System.err.println("Actual  :"+actBinaryString);                
                assertEquals(expected, actual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
        System.out.println(nsPerRead+"ns per read int, ints per second "+(1000l*1000l*1000l/nsPerRead));
       
        
    }
    
    
}
