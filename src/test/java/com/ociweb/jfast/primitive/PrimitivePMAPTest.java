//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.AfterClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class PrimitivePMAPTest {

    @AfterClass
    public static void cleanup() {
        System.gc();
    }

    @Test
    public void testWriterSingle() {

        ByteArrayOutputStream baost = new ByteArrayOutputStream();
        FASTOutputStream output = new FASTOutputStream(baost);

        PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);

        PrimitiveWriter.openPMap(10, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);

        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.closePMap(writer); // skip should be 7?
        PrimitiveWriter.flush(writer);

        byte[] data = baost.toByteArray();
        assertEquals("01010101", toBinaryString(data[0]));
        assertEquals("00101010", toBinaryString(data[1]));
        assertEquals("11000000", toBinaryString(data[2]));

    }

    @Test
    public void testWriterNested2() {

        ByteArrayOutputStream baost = new ByteArrayOutputStream();
        FASTOutputStream output = new FASTOutputStream(baost);

        PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);

        PrimitiveWriter.openPMap(10, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);

        // push will save where we are so we can continue after pop
        PrimitiveWriter.openPMap(3, writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // implied zero

        // continue with parent pmap
        PrimitiveWriter.closePMap(writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);

        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.closePMap(writer);
        PrimitiveWriter.flush(writer);

        byte[] data = baost.toByteArray();
        assertEquals("01010101", toBinaryString(data[0]));
        assertEquals("00101010", toBinaryString(data[1]));
        assertEquals("11000000", toBinaryString(data[2]));
        // pmap 2
        assertEquals("10001110", toBinaryString(data[3]));

    }

    @Test
    public void testWriterNested3() {

        ByteArrayOutputStream baost = new ByteArrayOutputStream();
        FASTOutputStream output = new FASTOutputStream(baost);

        PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);

        PrimitiveWriter.openPMap(3, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);

        // push will save where we are so we can continue after pop
        PrimitiveWriter.openPMap(3, writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);

        PrimitiveWriter.openPMap(4, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.closePMap(writer);

        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // implied zero

        // continue with parent pmap
        PrimitiveWriter.closePMap(writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);

        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.closePMap(writer);
        PrimitiveWriter.flush(writer);

        byte[] data = baost.toByteArray();
        assertEquals("01010101", toBinaryString(data[0]));
        assertEquals("00101010", toBinaryString(data[1]));
        assertEquals("11000000", toBinaryString(data[2]));
        // pmap 2
        assertEquals("10001110", toBinaryString(data[3]));
        // pmap 3
        assertEquals("10101010", toBinaryString(data[4]));

    }

    @Test
    public void testWriterSequential2() {

        ByteArrayOutputStream baost = new ByteArrayOutputStream();
        FASTOutputStream output = new FASTOutputStream(baost);

        PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);

        // pw.pushPMap(3);

        PrimitiveWriter.openPMap(10, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);

        PrimitiveWriter.writePMapBit((byte) 1, writer);

        PrimitiveWriter.closePMap(writer);

        // push will save where we are so we can continue after pop
        PrimitiveWriter.openPMap(3, writer);

        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 0, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // implied zero

        // continue with parent pmap
        PrimitiveWriter.closePMap(writer);

        // pw.popPMap();

        PrimitiveWriter.flush(writer);

        byte[] data = baost.toByteArray();
        assertEquals("01010101", toBinaryString(data[0]));
        assertEquals("00101010", toBinaryString(data[1]));
        assertEquals("11000000", toBinaryString(data[2]));
        // pmap 2
        assertEquals("10001110", toBinaryString(data[3]));

    }

    // not fast but it gets the job done.
    private String toBinaryString(byte b) {
        String result = Integer.toBinaryString(b);
        if (result.length() > 8) {
            return result.substring(result.length() - 8, result.length());
        }
        while (result.length() < 8) {
            result = "0" + result;
        }
        return result;
    }

    @Test
    public void testReaderSingle() {

        byte[] testData = new byte[] { ((byte) Integer.valueOf("00111010", 2).intValue()),
                ((byte) Integer.valueOf("10001011", 2).intValue()) };

        PrimitiveReader reader = new PrimitiveReader(testData);

        int maxPMapSize = testData.length; // in bytes
        // open this pmap
        PrimitiveReader.openPMap(maxPMapSize, reader);

        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // next byte
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // unwritten and assumed trailing zeros test
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // close
        PrimitiveReader.closePMap(reader);

    }
    
    @Test
    public void testReaderSingleOneByte() {

        byte[] testData = new byte[] { ((byte) Integer.valueOf("10111011", 2).intValue()) };

        PrimitiveReader reader = new PrimitiveReader(testData);

        int maxPMapSize = testData.length; // in bytes
        // open this pmap
        PrimitiveReader.openPMap(maxPMapSize, reader);

        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        
        // close
        PrimitiveReader.closePMap(reader);

    }

    @Test
    public void testReaderNested2() {

        byte[] testData = new byte[] { ((byte) Integer.valueOf("00111010", 2).intValue()),
                ((byte) Integer.valueOf("10001011", 2).intValue()),
                // second pmap starts here
                ((byte) Integer.valueOf("00000000", 2).intValue()), ((byte) Integer.valueOf("11111111", 2).intValue()) };

        PrimitiveReader reader = new PrimitiveReader(testData);

        // open this pmap
        PrimitiveReader.openPMap(2, reader);

        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // stop at this point to load another pmap and read it all before
        // continuing
        PrimitiveReader.openPMap(2, reader);
        // first byte of second pmap
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // second byte of second pmap
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // unwritten and assumed trailing zeros test
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // resume with first pmap
        PrimitiveReader.closePMap(reader);
        // /
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // next byte from first pmap
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // close
        PrimitiveReader.closePMap(reader);

    }

    @Test
    public void testReaderNested3() {

        byte[] testData = new byte[] { ((byte) Integer.valueOf("00111010", 2).intValue()),
                ((byte) Integer.valueOf("10001011", 2).intValue()),
                // second pmap starts here
                ((byte) Integer.valueOf("00000000", 2).intValue()), ((byte) Integer.valueOf("11111111", 2).intValue()),
                // third pmap starts here
                ((byte) Integer.valueOf("00110011", 2).intValue()), ((byte) Integer.valueOf("11001100", 2).intValue())

        };

        FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
        PrimitiveReader reader = new PrimitiveReader(2048, input, 32);

        // open this pmap
        PrimitiveReader.openPMap(2, reader);

        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // stop at this point to load another pmap and read it all before
        // continuing
        PrimitiveReader.openPMap(2, reader);
        // first byte of second pmap
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // stop here and load the third pmap
        PrimitiveReader.openPMap(2, reader);
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // second byte of third map
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));

        PrimitiveReader.closePMap(reader);
        // second byte of second pmap
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // unwritten and assumed trailing zeros test
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // resume with first pmap
        PrimitiveReader.closePMap(reader);
        // /
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        // next byte from first pmap
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(0, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        assertEquals(1, PrimitiveReader.readPMapBit(reader));
        // close
        PrimitiveReader.closePMap(reader);

    }

    //this test appears to be broken
//    @SuppressWarnings("unused")
//    @Test
//    public void testDogFood() {
//
//        final int pmaps = 3000000;
//        // the longer the max PMap the faster write becomes, so the push/pop is
//        // the expensive part and each sliced flush.
//        int maxDataBytes = 2;// this is 16 PMap bits, a good sized record
//
//        // //////////////////////////////////
//        // compute max bytes written to stream based on test data
//        // //////////////////////////////////
//        int maxWrittenBytes = (int) Math.ceil((maxDataBytes * 8) / 7d);
//
//        final int localBufferSize = pmaps * maxWrittenBytes;
//
//        byte[][] testPmaps = buildTestPMapData(pmaps, maxDataBytes);
//
//        // Run through our testing loops without calling any of the methods we
//        // want to test
//        // this gives us the total time our testing framework is consuming.
//        // CAUTION: this code must be identical to the code run in the test.
//        long writeOverhead = 0;
//        {
//            int ta;
//            int tb;
//            int tc;
//            int td;
//            int te;
//            int tf;
//            int tg;
//            int th;
//
//            int i = pmaps;
//            long start = System.nanoTime();
//            while (--i >= 0) {
//                byte[] pmapData = testPmaps[i];
//                int j = pmapData.length;
//                while (--j >= 0) {
//                    byte b = pmapData[j];
//                    ta = (b & 1);
//                    tb = ((b >> 1) & 1);
//                    tc = ((b >> 2) & 1);
//                    td = ((b >> 3) & 1);
//                    te = ((b >> 4) & 1);
//                    tf = ((b >> 5) & 1);
//                    tg = ((b >> 6) & 1);
//                    th = ((b >> 7) & 1);
//                }
//            }
//            writeOverhead = (System.nanoTime() - start);
//        }
//        //
//        int result = 0;
//        long readOverhead = 0;
//        {
//            int i = pmaps;
//            long start = System.nanoTime();
//            while (--i >= 0) {
//                byte[] pmapData = testPmaps[i];
//                int j = pmapData.length;
//                while (--j >= 0) {
//                    byte b = pmapData[j];
//                    result |= b;
//                    result |= b;
//                    result |= b;
//                    result |= b;
//
//                    result |= b;
//                    result |= b;
//                    result |= b;
//                    result |= b;
//                }
//            }
//            readOverhead = (System.nanoTime() - start);
//        }
//
//        // //////////////////////////////
//        // write all the bytes to the writtenBytes array
//        // this is timed to check performance
//        // same array will be verified for correctness in the next step.
//        // ////////////////////////////
//        final ByteBuffer buffer = ByteBuffer.allocateDirect(localBufferSize);
//        final FASTOutputByteBuffer output = new FASTOutputByteBuffer(buffer);
//        final FASTInputByteBuffer input = new FASTInputByteBuffer(buffer);
//
//        final PrimitiveWriter writer = new PrimitiveWriter(localBufferSize, output, false);
//        final PrimitiveReader reader = new PrimitiveReader(localBufferSize, input, pmaps);
//
//        int q = 3; // set to a large number when using profiler.
//        while (--q >= 0) {
//
//            output.reset();
//            PrimitiveWriter.reset(writer);
//
//            int i = pmaps;
//            try {
//                long start = System.nanoTime();
//                while (--i >= 0) {
//                    byte[] pmapData = testPmaps[i];
//                    // none of these are nested, we don't want to test nesting
//                    // here.
//                    PrimitiveWriter.openPMap(maxWrittenBytes, writer); // many are shorter but we
//                                                  // want to test the trailing
//                                                  // functionality
//                    int j = pmapData.length;
//                    // j=0 1 byte written - no bits are sent so its all 7 zeros
//                    // j=1 2 bytes written (bytes * 8)/7 to compute bytes
//                    // written
//                    // j=2 3 bytes written
//                    // j=3 4 bytes written
//
//                    while (--j >= 0) {
//
//                        byte b = pmapData[j];
//
//                        // put in first byte
//                        PrimitiveWriter.writePMapBit((byte) (b & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 1) & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 2) & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 3) & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 4) & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 5) & 1), writer);
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 6) & 1), writer);
//                        // put in next byte
//                        PrimitiveWriter.writePMapBit((byte) ((b >> 7) & 1), writer);
//                        // 6 zeros are assumed
//
//                    }
//                    PrimitiveWriter.closePMap(writer); // push/pop consumes 20% of the time.
//                }
//                // single flush, this is the bandwidth optimized approach.
//
//                // NOTE: flush now takes 22% of pmap test
//                PrimitiveWriter.flush(writer); // as of last test this only consumes 14% of the
//                            // time
//                long duration = (System.nanoTime() - start);
//
//                if (duration > writeOverhead) {
//                    if (0 == q) {
//                        System.out.println("total duration with overhead:" + duration + " overhead:" + writeOverhead
//                                + " pct " + (100 * writeOverhead / (float) duration));
//                    }
//                    duration -= writeOverhead;
//                } else {
//                    System.out.println();
//                    System.out.println("unable to compute overhead measurement. per byte:"
//                            + (writeOverhead / (double) buffer.position()));
//
//                    writeOverhead = 0;
//                }
//                if (0 == q) {
//                    assertTrue("total duration: " + duration + "ns but nothing written.", buffer.position() > 0);
//                }
//                float nsPerByte = duration / (float) buffer.position();
//                if (0 == q) {
//                    System.out.println("pure PMap write " + nsPerByte + "ns per byte. TotalBytes:" + buffer.position());
//                }
//            } finally {
//                int testedMaps = (pmaps - (i + 1));
//                if (0 == q) {
//                    System.out.println("finished testing write after " + testedMaps
//                            + " unique pmaps and testing overhead of " + writeOverhead);
//                }
//            }
//
//            // //////////////////////////////////
//            // read all the bytes from the written bytes array to ensure
//            // that they all match with the original test data
//            // this has extra unit test logic in it so it is NOT timed for
//            // performance
//            // /////////////////////////////////
//
//            i = pmaps;
//            try {
//                input.reset();
//                PrimitiveReader.reset(reader);
//
//                while (--i >= 0) {
//                    byte[] pmapData = testPmaps[i];
//                    PrimitiveReader.openPMap(maxWrittenBytes, reader);
//
//                    int j = pmapData.length;
//                    if (j == 0) {
//
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        assertEquals(0, PrimitiveReader.readPMapBit(reader));
//
//                    } else {
//                        int totalBits = maxWrittenBytes * 7;
//                        while (--j >= 0) {
//                            byte b = pmapData[j];
//
//                            assertEquals((b & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 1) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 2) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 3) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 4) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 5) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 6) & 1), PrimitiveReader.readPMapBit(reader));
//                            assertEquals(((b >> 7) & 1), PrimitiveReader.readPMapBit(reader));
//
//                            totalBits -= 8;
//                        }
//                        // confirm the rest of the "unwriten bits are zeros"
//                        while (--totalBits >= 0) {
//                            assertEquals(0, PrimitiveReader.readPMapBit(reader));
//                        }
//                    }
//                    PrimitiveReader.closePMap(reader);
//                }
//
//            } finally {
//                int readTestMaps = (pmaps - (i + 1));
//                if (0 == q) {
//                    System.out.println("finished testing read after " + readTestMaps
//                            + " unique pmaps and testing overhead of " + writeOverhead);
//                }
//            }
//            input.reset();
//            PrimitiveReader.reset(reader);
//
//            // ///////////////////////////////////////
//            // test read pmap timing
//            // //////////////////////////////////////
//            readPmapTest2(pmaps, maxWrittenBytes, testPmaps, readOverhead, buffer, reader, 0 == q);
//
//        }
//
//        // */
//        // cleanup before next test
//        Runtime.getRuntime().gc();
//
//    }

    private int readPmapTest2(int pmaps, int maxWrittenBytes, byte[][] testPmaps, long readOverhead, ByteBuffer buffer,
            PrimitiveReader reader, boolean printResults) {
        int i = pmaps;
        int result = 0;

        long start = System.nanoTime();
        while (--i >= 0) {
            PrimitiveReader.openPMap(maxWrittenBytes, reader);

            int j = testPmaps[i].length;
            while (--j >= 0) {
                // 8 of these will force 1 at least 1 byte change per pass
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);
                result |= PrimitiveReader.readPMapBit(reader);

                result |= PrimitiveReader.readPMapBit(reader);
            }

            PrimitiveReader.closePMap(reader);
        }
        if (printResults) {
            printReadResults(readOverhead, buffer, System.nanoTime() - start);
        }
        return result;
    }

    private void printReadResults(long readOverhead, ByteBuffer buffer, long totalDuration) {
        System.out.println("total duration with overhead:" + totalDuration + " overhead:" + readOverhead);
        long duration = totalDuration - readOverhead;
        System.out.println("pure PMap read " + (duration / (double) buffer.position()) + "ns per byte");
    }

    private byte[][] buildTestPMapData(int pmaps, int maxLengthBytes) {
        byte[][] testPmaps = new byte[pmaps][];

        double r = 0d;
        double step = (2d * Math.PI) / (double) (pmaps);

        int i = pmaps;
        int len = maxLengthBytes;
        while (--i >= 0) {
            byte[] newPmap = new byte[len];
            int j = len;
            while (--j >= 0) {
                // this produces large repeatable test data with a good coverage
                newPmap[j] = (byte) (0xFF & (int) (128 * Math.sin(r)));
                r += step;
            }
            // System.err.println(Arrays.toString(newPmap));
            testPmaps[i] = newPmap;
            if (--len < 0) {
                len = maxLengthBytes;
            }
        }
        return testPmaps;
    }

}
