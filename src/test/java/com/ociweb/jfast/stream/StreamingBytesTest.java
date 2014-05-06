//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.benchmark.HomogeniousRecordWriteReadLongBenchmark;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;

public class StreamingBytesTest extends BaseStreamingTest {

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
    public void bytesTest() {
        int[] types = new int[] { TypeMask.ByteArray, TypeMask.ByteArrayOptional, };
        int[] operators = new int[] { OperatorMask.Field_None, OperatorMask.Field_Constant, OperatorMask.Field_Copy,
                OperatorMask.Field_Default, OperatorMask.Field_Delta, OperatorMask.Field_Tail, };

        byteTester(types, operators, "Bytes");
    }

    @Test
    public void TailTest() {

        byte[] buffer = new byte[2048];
        FASTOutput output = new FASTOutputByteArray(buffer);
        PrimitiveWriter writer = new PrimitiveWriter(4096, output, 128, false);

        int singleSize = 14;
        int singleGapSize = 8;
        int fixedTextItemCount = 16; // must be power of two

        ByteHeap dictionaryWriter = new ByteHeap(singleSize, singleGapSize, fixedTextItemCount);

        int token = TokenBuilder.buildToken(TypeMask.ByteArray, OperatorMask.Field_Tail, 0,
                TokenBuilder.MASK_ABSENT_DEFAULT);
        byte[] value = new byte[] { 1, 2, 3 };
        int offset = 0;
        int length = value.length;
        int instanceMask = null==dictionaryWriter? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (dictionaryWriter.itemCount()-1));
        writeBytesTail(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesDelta(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesConstant();

        writer.openPMap(1);
        writeBytesCopy(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesDefault(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writer.closePMap();
        writer.flush(writer);

        FASTInput input = new FASTInputByteArray(buffer);
        PrimitiveReader reader = new PrimitiveReader(2048, input, 32);

        ByteHeap dictionaryReader = new ByteHeap(singleSize, singleGapSize, fixedTextItemCount);
        int byteInstanceMask = null == dictionaryReader ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,
        dictionaryReader.itemCount() - 1);

        // read value back
        int id;
        int idx = token & byteInstanceMask;
        id = readBytesTail(idx, reader, dictionaryReader);
        assertTrue(dictionaryReader.equals(id, value, offset, length));
        int idx1 = token & byteInstanceMask;

        id = readBytesDelta(idx1, reader, dictionaryReader);
        assertTrue(dictionaryReader.equals(id, value, offset, length));

        id = token & byteInstanceMask;
        assertTrue(dictionaryReader.equals(id, value, offset, length));

        PrimitiveReader.openPMap(1, reader);
        int idx2 = token & byteInstanceMask;
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            int length1 = PrimitiveReader.readIntegerUnsigned(reader) - 0;
            PrimitiveReader.readByteData(dictionaryReader.rawAccess(), dictionaryReader.allocate(idx2, length1), length1, reader);
        }

        id = idx2;
        assertTrue(dictionaryReader.equals(id, value, offset, length));
        int idx3 = token & byteInstanceMask;

        id = readBytesDefault(idx3, reader, dictionaryReader);
        assertTrue(dictionaryReader.equals(id, value, offset, length));

        PrimitiveReader.closePMap(reader);

    }

    private int readBytesTail(int idx, PrimitiveReader reader, ByteHeap byteHeap) {
        int id;
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        int length = PrimitiveReader.readIntegerUnsigned(reader);
        
        // append to tail
        int targetOffset = byteHeap.makeSpaceForAppend(idx, trim, length);
        PrimitiveReader.readByteData(byteHeap.rawAccess(), targetOffset, length, reader);
        id = idx;
        return id;
    }

    
    private int readBytesDefault(int idx3, PrimitiveReader reader, ByteHeap byteHeap) {
        int id;
        int result;
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            // System.err.println("z");
            result = idx3 | INIT_VALUE_MASK;// use constant
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx3, length), length, reader);
            result = idx3;
        }
        id = result;
        return id;
    }

    private int readBytesDelta(int idx1, PrimitiveReader reader, ByteHeap byteHeap) {
        int id;
        int trim = PrimitiveReader.readIntegerSigned(reader);
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx1, trim, utfLength), utfLength, reader);
        } else {
            // append to head
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx1, -trim, utfLength), utfLength, reader);
        }
        id = idx1;
        return id;
    }

    private void writeBytesDefault(PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length);
        	writer.writeByteArrayData(value,offset,length);
        }
    }

    private void writeBytesCopy(PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        if (byteHeap.equals(idx, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        }
        else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length);
        	writer.writeByteArrayData(value,offset,length);
        	byteHeap.set(idx, value, offset, length);
        }
    }

    private void writeBytesConstant() {
    }

    private void writeBytesDelta(PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = byteHeap.countTailMatch(idx, value, offset+length, length);
        if (headCount>tailCount) {
        	int trimTail = byteHeap.length(idx)-headCount;
            writer.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount+headCount;
            
            writer.writeIntegerUnsigned(valueSend);
            writer.writeByteArrayData(value, startAfter, valueSend);
            byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        } else {
        	//replace head, tail matches to tailCount
            int trimHead = byteHeap.length(idx)-tailCount;
            writer.writeIntegerSigned(trimHead==0? 0: -trimHead); 
            
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeByteArrayData(value, offset, len);
            
            byteHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    private void writeBytesTail(PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }

    private void byteTester(int[] types, int[] operators, String label) {

        int singleBytesLength = 2048;
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

        int[] tokenLookup = HomogeniousRecordWriteReadLongBenchmark.buildTokens(fields, types, operators);
        byte[] writeBuffer = new byte[streamByteSize];

        // /////////////////////////////
        // test the writing performance.
        // ////////////////////////////

        long byteCount = performanceWriteTest(fields, fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes,
                operationIters, warmup, sampleSize, writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

        // /////////////////////////////
        // test the reading performance.
        // ////////////////////////////

        performanceReadTest(fields, fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes, operationIters,
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

        FASTWriterScriptPlayerDispatch fw = new FASTWriterScriptPlayerDispatch(writer, dcr, 100, 64, 64, 8, 8, null, 3, new int[0][0], null, 64);

        long start = System.nanoTime();
        int i = operationIters;
        if (i < 3) {
            throw new UnsupportedOperationException("must allow operations to have 3 data points but only had " + i);
        }
        int g = fieldsPerGroup;

        int groupToken = TokenBuilder.buildToken(TypeMask.Group, maxMPapBytes > 0 ? OperatorMask.Group_Bit_PMap : 0,
                maxMPapBytes, TokenBuilder.MASK_ABSENT_DEFAULT);

        fw.openGroup(groupToken, maxMPapBytes);

        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                int token = tokenLookup[f];

                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && ((i & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                        fw.write(token);
                    } else {
                        if ((i & 1) == 0) {
                            testContByteBuffer.mark();
                            fw.write(token, testContByteBuffer); // write byte
                                                                 // buffer
                            testContByteBuffer.reset();

                        } else {
                            byte[] array = testConst;
                            fw.write(token, array, 0, array.length);
                        }
                    }
                } else {
                    if (sendNulls && ((f & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                        fw.write(token);
                    } else {
                        if ((i & 1) == 0) {
                            // first failing test
                            testData[f].mark();
                            fw.write(token, testData[f]); // write byte buffer
                            testData[f].reset();
                        } else {
                            byte[] array = testDataBytes[f];
                            fw.write(token, array, 0, array.length);
                        }
                    }
                }

                g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, maxMPapBytes);
            }
        }
        if (((fieldsPerGroup * fields) % fieldsPerGroup) == 0) {
            fw.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER));
        }
        fw.flush();
        fw.flush();
        long duration = System.nanoTime() - start;
        return duration;
    }

    @Override
    protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup, DictionaryFactory dcr) {

        PrimitiveReader.reset(reader);
        FASTReaderScriptPlayerDispatch fr = new FASTReaderScriptPlayerDispatch(reader, dcr, 3, new int[0][0], 0, 128, 4, 4, null, 64, 8, 7);
        ByteHeap byteHeap = fr.byteHeap;

        int token = 0;
        int prevToken = 0;

        long start = System.nanoTime();
        int i = operationIters;
        if (i < 3) {
            throw new UnsupportedOperationException("must allow operations to have 3 data points but only had " + i);
        }
        int g = fieldsPerGroup;
        int groupToken = TokenBuilder.buildToken(TypeMask.Group, maxMPapBytes > 0 ? OperatorMask.Group_Bit_PMap : 0,
                maxMPapBytes, TokenBuilder.MASK_ABSENT_DEFAULT);

        fr.openGroup(groupToken, maxMPapBytes);

        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                prevToken = token;
                token = tokenLookup[f];
                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && (i & 0xF) == 0 && TokenBuilder.isOptional(token)) {

                        int idx = fr.readBytes(tokenLookup[f]);
                        if (!byteHeap.isNull(idx)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token), Boolean.TRUE, byteHeap.isNull(idx));
                        }

                    } else {
                        try {
                            int textIdx = fr.readBytes(tokenLookup[f]);

                            byte[] tdc = testConst;
                            assertTrue("Error:" + TokenBuilder.tokenToString(token),
                                    byteHeap.equals(textIdx, tdc, 0, tdc.length));

                        } catch (Exception e) {
                            System.err.println("expected text; " + testData[f]);
                            e.printStackTrace();
                            throw new FASTException(e);
                        }
                    }
                } else {
                    if (sendNulls && (f & 0xF) == 0 && TokenBuilder.isOptional(token)) {

                        int idx = fr.readBytes(tokenLookup[f]);
                        if (!byteHeap.isNull(idx)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token) + "Expected null found len "
                                    + byteHeap.length(idx), Boolean.TRUE, byteHeap.isNull(idx));
                        }

                    } else {
                        try {
                            int textIdx = fr.readBytes(tokenLookup[f]);

                            if ((1 & i) == 0) {
                                assertTrue("Error: Token:" + TokenBuilder.tokenToString(token) + " PrevToken:"
                                        + TokenBuilder.tokenToString(prevToken), byteHeap.equals(textIdx, testData[f]));
                            } else {
                                byte[] tdc = testDataBytes[f];
                                assertTrue("Error:" + TokenBuilder.tokenToString(token),
                                        byteHeap.equals(textIdx, tdc, 0, tdc.length));

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

                g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f, maxMPapBytes);
            }
        }
        if (((fieldsPerGroup * fields) % fieldsPerGroup) == 0) {
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            fr.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER),idx);
        }
        long duration = System.nanoTime() - start;
        return duration;
    }

    private ByteBuffer[] buildTestData(int count) {

        byte[][] seedData = ReaderWriterPrimitiveTest.byteData;
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
        return writer.totalWritten(writer);
    }

    protected void resetOutputWriter() {
        output.reset();
        writer.reset(writer);
    }

    protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
        output = new FASTOutputByteArray(writeBuffer);
        writer = new PrimitiveWriter(writeBuffer.length, output, maxGroupCount, false);
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
