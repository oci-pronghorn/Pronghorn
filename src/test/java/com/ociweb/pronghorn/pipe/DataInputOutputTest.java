package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class DataInputOutputTest {

    private static final int testSpace = 100000000;
    
    private static final PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 5, testSpace);
        
    
    
    private long testLongValueGenerator(Random r, int iter) {
        
        if (iter<2000) {
            int value = iter>>1;
            if (0==(iter&1)) {
                return value;
            } else {
                return -value;
            }
        }
        if (iter==2001) {
            return Long.MIN_VALUE;
        }
        if (iter==2002) {
            return Long.MAX_VALUE;
        }
        
        
        int powOf2 = iter & 0x3F;
        long range = (1l<<powOf2);
        long maskValue = range-1l;
        
        
        long testValue =   ((r.nextLong()&maskValue) | range );
        
        //System.out.println(testValue);
        return testValue;
    }
    
    private int testIntValueGenerator(Random r, int iter) {
        
        if (iter<2000) {
            int value = iter>>1;
            if (0==(iter&1)) {
                return value;
            } else {
                return -value;
            }
        }
        if (iter==2001) {
            return Integer.MIN_VALUE;
        }
        if (iter==2002) {
            return Integer.MAX_VALUE;
        }
        
        return r.nextInt();
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
        
        int length = out.closeHighLevelField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        
        float compression = 1f-(length/(float)(testSize*8));
        
       // System.out.println(nsPerWrite+"ns per written long, longs per second "+(1000l*1000l*1000l/nsPerWrite)+" compression "+compression);
        
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

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
                
       //         System.err.println("Expected:"+expBinaryString);
      //          System.err.println("Actual  :"+actBinaryString);                
                assertEquals(expected, actual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
     //   System.out.println(nsPerRead+"ns per read long, longs per second "+(1000l*1000l*1000l/nsPerRead));
       
        
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
        
        
        
        int length = out.closeHighLevelField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        
        float compression = 1f-(length/(float)(testSize*4));
   //     System.out.println(nsPerWrite+"ns per written int, ints per second "+(1000l*1000l*1000l/nsPerWrite)+" compression "+compression);
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

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
                
     //           System.err.println("Expected:"+expBinaryString);
     //           System.err.println("Actual  :"+actBinaryString);                
                assertEquals(expected, actual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
  //      System.out.println(nsPerRead+"ns per read int, ints per second "+(1000l*1000l*1000l/nsPerRead));
       
        
    }
    
    @Test
    public void testPackedChars() {
        int testSize = testSpace/50;
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
            out.writePackedString(Integer.toHexString(testIntValueGenerator(r,i)));
        }
        long duration = System.nanoTime()-start;
        long nsPerWrite = duration/testSize;
                
        
        int length = out.closeHighLevelField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        
        float compression = 1f-(length/(float)(testSize*4));
   //     System.out.println(nsPerWrite+"ns per written int, ints per second "+(1000l*1000l*1000l/nsPerWrite)+" compression "+compression);
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

        r = new Random(101);
        start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            String expected = Integer.toHexString(testIntValueGenerator(r,i));
            String actual = null;
            try {
                actual = in.readPackedChars(new StringBuilder()).toString();
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
            
            if (!expected.equals(actual)) {
                String expBinaryString = expected;
                String actBinaryString = actual;
                while (actBinaryString.length()<expBinaryString.length()) {
                    actBinaryString = '0'+actBinaryString;
                }
                
     //           System.err.println("Expected:"+expBinaryString);
     //           System.err.println("Actual  :"+actBinaryString);                
                assertEquals(expected, actual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
  //      System.out.println(nsPerRead+"ns per read int, ints per second "+(1000l*1000l*1000l/nsPerRead));
               
    }
    
    
    
    
    @Test
    public void testBytesArray() {
        int testSize = testSpace/50;
        Random r;
        Pipe<RawDataSchema> testPipe = new Pipe<RawDataSchema>(config);        
        
        testPipe.initBuffers();
        
        DataOutputBlobWriter<RawDataSchema> out = new DataOutputBlobWriter<>(testPipe);
        DataInputBlobReader<RawDataSchema> in = new DataInputBlobReader<>(testPipe);
        
        
        assertTrue(PipeWriter.tryWriteFragment(testPipe, 0));
        
         
        out.openField();
        
        int testSourceSize = 4;
        byte[] testSource = new byte[testSourceSize];
        
        r = new Random(101);
        long start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            
            for(int s=0;s<testSourceSize;s++) {
                testSource[s] = (byte)testIntValueGenerator(r,i);
            }            
            
                out.write(testSource);

        }
        long duration = System.nanoTime()-start;
        long nsPerWrite = duration/testSize;
        assertEquals(testSourceSize*testSize,out.length());
            
        
        int length = out.closeHighLevelField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        assertEquals(testSourceSize*testSize,length);
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

        r = new Random(101);
        byte[] testActual = new byte[testSourceSize];
        start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            
            
            for(int s=0;s<testSourceSize;s++) {
                testSource[s] = (byte)testIntValueGenerator(r,i);
            }            
                        
            in.read(testActual);
                        
            if (!Arrays.equals(testSource, testActual)) {               
                assertEquals(testSource, testActual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
  //      System.out.println(nsPerRead+"ns per read int, ints per second "+(1000l*1000l*1000l/nsPerRead));
               
    }
    
    @Test
    public void testBytesArray2() {
        int testSize = testSpace/50;
        Random r;
        Pipe<RawDataSchema> testPipe = new Pipe<RawDataSchema>(config);        
        
        testPipe.initBuffers();
        
        DataOutputBlobWriter<RawDataSchema> out = new DataOutputBlobWriter<>(testPipe);
        DataInputBlobReader<RawDataSchema> in = new DataInputBlobReader<>(testPipe);
        
        
        assertTrue(PipeWriter.tryWriteFragment(testPipe, 0));
        
         
        out.openField();
        
        int testSourceSize = 4;
        byte[] testSource = new byte[testSourceSize];
        
        r = new Random(101);
        long start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            
            for(int s=0;s<testSourceSize;s++) {
                testSource[s] = (byte)testIntValueGenerator(r,i);
            }            
            
                for(int s=0;s<testSourceSize;s++) {
                    out.writeByte(testSource[s]);
                }
                  
        
        }
        long duration = System.nanoTime()-start;
        long nsPerWrite = duration/testSize;
        assertEquals(testSourceSize*testSize,out.length());
        
        
        int length = out.closeHighLevelField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
        assertEquals(testSourceSize*testSize,length);
        
        PipeWriter.publishWrites(testPipe);
        
        assertTrue(PipeReader.tryReadFragment(testPipe));

        
        in.openHighLevelAPIField(RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);

        r = new Random(101);
        byte[] testActual = new byte[testSourceSize];
        start = System.nanoTime();
        for(int i = 0; i<testSize; i++) {
            
            
            for(int s=0;s<testSourceSize;s++) {
                testSource[s] = (byte)testIntValueGenerator(r,i);
            }            
                        
            in.read(testActual);
                        
            if (!Arrays.equals(testSource, testActual)) {               
                assertEquals(testSource, testActual);
            }
        }
        duration = System.nanoTime()-start;
        long nsPerRead = duration/testSize;
  //      System.out.println(nsPerRead+"ns per read int, ints per second "+(1000l*1000l*1000l/nsPerRead));
               
    }
    
    
    
}
