//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.benchmark.TestUtil;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class StreamingASCIITest extends BaseStreamingTest {

    final int fields = 2000;
    final ByteBuffer[] testData = buildTestData(fields);

    final byte[] testConst = new byte[0];
    final ByteBuffer testContByteBuffer = ByteBuffer.wrap(testConst);

    boolean sendNulls = true;

    final byte[][] testDataBytes = buildTestDataBytes(testData);

    FASTOutputByteArray output;
    PrimitiveWriter writer;

    FASTInputByteArray input;
    PrimitiveReader reader;

    public static final int INIT_VALUE_MASK = 0x80000000;

    @AfterClass
    public static void cleanup() {
        System.gc();
    }

    private byte[][] buildTestDataBytes(ByteBuffer[] source) {
        int i = source.length;
        byte[][] result = new byte[i][];
        while (--i >= 0) {
            result[i] = testData[i].array();
        }
        return result;
    }

    @Test
    public void asciiTest() {
        int[] types = new int[] { 
                TypeMask.TextASCII,
                TypeMask.TextASCIIOptional,
                };
        int[] operators = new int[] { 
                OperatorMask.Field_None, 
     //           OperatorMask.Field_Constant, 
                OperatorMask.Field_Copy,
      //         OperatorMask.Field_Default,
        //        OperatorMask.Field_Delta, 
       //         OperatorMask.Field_Tail, 
                };

        asciiTester(types, operators, "ASCII");
    }

 

    private void asciiTester(int[] types, int[] operators, String label) {

        int singleBytesLength = 256;
        int fieldsPerGroup = 10;
        int maxMPapBytes = (int) Math.ceil(fieldsPerGroup / 7d);
        int operationIters = 7;
        int warmup = 20;
        int sampleSize = 40;
        int avgFieldSize = ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK * 2 + 1;
        String readLabel = "Read " + label + " NoOpp in groups of " + fieldsPerGroup;
        String writeLabel = "Write " + label + " NoOpp in groups of " + fieldsPerGroup;

        int streamByteSize = operationIters * ((maxMPapBytes * (fields / fieldsPerGroup)) + (fields * avgFieldSize));
                
        int maxGroupCount = operationIters * fields / fieldsPerGroup;

        int[] tokenLookup = TestUtil.buildTokens(fields, types, operators);
        byte[] writeBuffer = new byte[streamByteSize];

        // /////////////////////////////
        // test the writing performance.
        // ////////////////////////////

        long byteCount = performanceWriteTest(fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes,
                operationIters, warmup, sampleSize, writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

        // /////////////////////////////
        // test the reading performance.
        // ////////////////////////////

        performanceReadTest(fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes, operationIters,
                warmup, sampleSize, readLabel, streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);

        int i = 0;
        for (ByteBuffer d : testData) {
            i += d.remaining();
        }

        long dataCount = (operationIters * (long) fields * (long) i) / testData.length;

        System.out.println("FullData:" + dataCount + " XmitData:" + byteCount + " compression:"
                + (byteCount / (float) dataCount));

    }

    @Override
    protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
            int[] tokenLookup, DictionaryFactory dcr) {

        FASTWriterInterpreterDispatch fw = FASTWriterInterpreterDispatch
				.createFASTWriterInterpreterDispatch(new TemplateCatalogConfig(dcr, 3, new int[0][0], null,
				64,4, 100, new ClientConfig(8 ,7) ));

        long start = System.nanoTime();
        int i = operationIters;
        if (i < 3) {
            throw new UnsupportedOperationException("must allow operations to have 3 data points but only had " + i);
        }
        int g = fieldsPerGroup;

        int groupToken = TokenBuilder.buildToken(TypeMask.Group, maxMPapBytes > 0 ? OperatorMask.Group_Bit_PMap : 0,
                maxMPapBytes);

        fw.openGroup(groupToken, maxMPapBytes, writer);
        

        RingBuffer ring = new RingBuffer((byte)7,(byte)7,LocalHeap.rawInitAccess(fw.byteHeap), FieldReferenceOffsetManager.RAW_BYTES);
        RingBuffer.dump(ring);
        
        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                int token = tokenLookup[f];

                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && ((i & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                    	
                    	byte[] array = testConst;
                    	
                        RingBuffer.dump(ring);
                        RingBuffer.addNullByteArray(ring);
                        RingBuffer.publishWrites(ring);
                    	
                        fw.acceptCharSequenceASCIIOptional(token, writer, fw.byteHeap, 0, ring);
                    } else {
                        {
                            byte[] array = testConst;

                            RingBuffer.addByteArray(array, 0, array.length, ring);
                            RingBuffer.publishWrites(ring);
                            
                            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                                fw.acceptCharSequenceASCII(token, writer, fw.byteHeap, 0, ring);
                            } else {
                                fw.acceptCharSequenceASCIIOptional(token, writer, fw.byteHeap, 0, ring);
                            }
                        }
                    }
                } else {
                    if (sendNulls && ((f & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                    	
                    	byte[] array = testDataBytes[f];
                    	
                        RingBuffer.dump(ring);
                        RingBuffer.addNullByteArray(ring);
                        RingBuffer.publishWrites(ring);
                    	
                    	
                        //BaseStreamingTest.write(token, writer, fw);
                        fw.acceptCharSequenceASCIIOptional(token, writer, fw.byteHeap, 0, ring);
                    } else {
                        {
                            byte[] array = testDataBytes[f];
                            
                            RingBuffer.dump(ring);
                            RingBuffer.addByteArray(array, 0, array.length, ring);
                            RingBuffer.publishWrites(ring);                                                       
                           
                            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                                fw.acceptCharSequenceASCII(token, writer, fw.byteHeap, 0, ring);
                            } else {
                                fw.acceptCharSequenceASCIIOptional(token, writer, fw.byteHeap, 0, ring);
                            }
                        }
                    }
                }

                g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, maxMPapBytes,writer);
            }
        }
        if (((fieldsPerGroup * fields) % fieldsPerGroup) == 0) {
            fw.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), writer);
        }
        fw.flush(writer);
        fw.flush(writer);
        long duration = System.nanoTime() - start;
        return duration;
    }

    @Override
    protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup, DictionaryFactory dcr) {

        PrimitiveReader.reset(reader);
        TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dcr, 3, new int[0][0], null, 64, maxGroupCount * 10, -1,  new ClientConfig(8 ,7));
        
        FASTReaderInterpreterDispatch fr = new FASTReaderInterpreterDispatch(testCatalog, RingBuffers.buildNoFanRingBuffers(new RingBuffer((byte)testCatalog.clientConfig().getPrimaryRingBits(),(byte)testCatalog.clientConfig().getTextRingBits(),testCatalog.ringByteConstants(), testCatalog.getFROM())));
        LocalHeap byteHeap = fr.byteHeap;

        int token = 0;
        int prevToken = 0;

        long start = System.nanoTime();
        int i = operationIters;
        if (i < 3) {
            throw new UnsupportedOperationException("must allow operations to have 3 data points but only had " + i);
        }
        int g = fieldsPerGroup;
        int groupToken = TokenBuilder.buildToken(TypeMask.Group, maxMPapBytes > 0 ? OperatorMask.Group_Bit_PMap : 0,
                maxMPapBytes);

        fr.openGroup(groupToken, maxMPapBytes, reader);

        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                prevToken = token;
                token = tokenLookup[f];
                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && (i & 0xF) == 0 && TokenBuilder.isOptional(token)) {

                        int idx = fr.readASCII(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));
                        if (!LocalHeap.isNull(idx,byteHeap)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token), Boolean.TRUE, LocalHeap.isNull(idx,byteHeap));
                        }

                    } else {
                        try {
                            int textIdx = fr.readASCII(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));

                            byte[] tdc = testConst;                           
                            
                            assertEquals(tdc.length, LocalHeap.length(textIdx,byteHeap));
                            
                            if (0!=tdc.length) {
                                assertTrue("Error:" + TokenBuilder.tokenToString(token)+ 
                                           "\n " + Arrays.toString(tdc) +
                                           " vs \n" + byteHeap.toString(textIdx),
                                        LocalHeap.equals(textIdx,tdc,0,tdc.length,0xFFFFFFFF,byteHeap));
                            }

                        } catch (Exception e) {
                            System.err.println("expected text; " + testData[f]);
                            e.printStackTrace();
                            throw new FASTException(e);
                        }
                    }
                } else {
                    if (sendNulls && (f & 0xF) == 0 && TokenBuilder.isOptional(token)) {

                        int idx = fr.readASCII(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));
                        if (!LocalHeap.isNull(idx,byteHeap)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token) + "Expected null found len "
                                    + LocalHeap.length(idx,byteHeap), Boolean.TRUE, LocalHeap.isNull(idx,byteHeap));
                        }

                    } else {
                        try {
                            int heapIdx = fr.readASCII(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));

                            if ((1 & i) == 0) {                              
                                assertTrue("Error: Token:" + TokenBuilder.tokenToString(token) + " PrevToken:"
                                        + TokenBuilder.tokenToString(prevToken),
                                        LocalHeap.equals(heapIdx,testDataBytes[f],0,testDataBytes[f].length,0xFFFFFFFF,byteHeap));
                            } else {
                                byte[] tdc = testDataBytes[f];
                                assertEquals(tdc.length, LocalHeap.length(heapIdx,byteHeap));
                                assertTrue("Error:" + TokenBuilder.tokenToString(token)+ " length "+tdc.length,
                                            LocalHeap.equals(heapIdx,tdc,0,tdc.length,0xFFFFFFFF,byteHeap));
                            }

                        } catch (Exception e) {
                            System.err.println("expected text; " + testData[f]);
                            System.err.println("PrevToken:" + TokenBuilder.tokenToString(prevToken));
                            System.err.println("token:" + TokenBuilder.tokenToString(token));
                            e.printStackTrace();
                            throw new FASTException(e);
                        }
                    }
                }

                g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f, maxMPapBytes, reader);
            }
        }
        if (((fieldsPerGroup * fields) % fieldsPerGroup) == 0) {
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            fr.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER),idx, reader);
        }
        long duration = System.nanoTime() - start;
        return duration;
    }

    private ByteBuffer[] buildTestData(int count) {

        byte[][] seedData = ReaderWriterPrimitiveTest.stringDataBytes;
        int s = seedData.length;
        int i = count;
        ByteBuffer[] target = new ByteBuffer[count];
        while (--i >= 0) {
            target[i] = ByteBuffer.wrap(seedData[--s]);
            if (0 == s) {
                s = seedData.length;
            }
        }
        return target;
    }

    public long totalWritten() {
        return PrimitiveWriter.totalWritten(writer);
    }

    protected void resetOutputWriter() {
        output.reset();
        PrimitiveWriter.reset(writer);
    }

    protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
        output = new FASTOutputByteArray(writeBuffer);
        writer = new PrimitiveWriter(writeBuffer.length, output, false);
    }

    protected long totalRead() {
        return PrimitiveReader.totalRead(reader);
    }

    protected void resetInputReader() {
        input.reset();
        PrimitiveReader.reset(reader);
    }

    protected void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes) {
        input = new FASTInputByteArray(writtenData, writtenBytes);
        reader = new PrimitiveReader(writtenData.length, input, maxGroupCount);
    }

}
