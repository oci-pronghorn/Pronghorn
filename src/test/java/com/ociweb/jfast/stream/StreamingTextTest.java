//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.benchmark.HomogeniousRecordWriteReadLongBenchmark;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;

public class StreamingTextTest extends BaseStreamingTest {

    final int fields = 3000;
    final CharSequence[] testData = buildTestData(fields);
    final String testConstSeq = "";
    final char[] testConst = testConstSeq.toCharArray();
    boolean sendNulls = true;

    int NULL_SEND_MASK = 0xF;

    final char[][] testDataChars = buildTestDataChars(testData);

    FASTOutputByteArray output;
    PrimitiveWriter pw;

    FASTInputByteArray input;
    PrimitiveReader reader;

    @AfterClass
    public static void cleanup() {
        System.gc();
    }

    private char[][] buildTestDataChars(CharSequence[] source) {
        int i = source.length;
        char[][] result = new char[i][];
        while (--i >= 0) {
            result[i] = seqToArray(testData[i]);
        }
        return result;
    }

    @Test
    public void asciiTest() {
        int[] types = new int[] { TypeMask.TextASCII, TypeMask.TextASCIIOptional, };

        // TODO: Z, Force this error and introduce more discriptive error, will
        // fail without good message if HeapText is too small!!
        int[] operators = new int[] { OperatorMask.Field_None, // W5 R16 w/o
                                                               // equals
                OperatorMask.Field_Constant, // W6 R16 w/o equals
                OperatorMask.Field_Copy, // W84 R31 w/o equals
                OperatorMask.Field_Default, // W6 R16
                OperatorMask.Field_Delta, // W85 R39 .37
                OperatorMask.Field_Tail, // W46 R15 w/o equals
        };

        textTester(types, operators, "ASCII");
    }

    @Test
    public void utf8Test() {
        int[] types = new int[] { TypeMask.TextUTF8, TypeMask.TextUTF8Optional, };
        int[] operators = new int[] { OperatorMask.Field_None, // W9 R17 1.08
                OperatorMask.Field_Constant, // W9 R17 1.09
                OperatorMask.Field_Copy, // W83 R84 .163
                OperatorMask.Field_Default, // W10 R18
                OperatorMask.Field_Delta, // W110 R51 .31
                OperatorMask.Field_Tail, // W57 R51 .31
        };

        textTester(types, operators, "UTF8");
    }

    private void textTester(int[] types, int[] operators, String label) {

        int singleCharLength = 1024;
        int fieldsPerGroup = 10;
        int maxMPapBytes = (int) Math.ceil(fieldsPerGroup / 7d);
        int operationIters = 7;
        int warmup = 20;
        int sampleSize = 40;
        int avgFieldSize = ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK * 2 + 1;
        String readLabel = "Read " + label + "Text NoOpp in groups of " + fieldsPerGroup;
        String writeLabel = "Write " + label + "Text NoOpp in groups of " + fieldsPerGroup;

        int streamByteSize = operationIters * ((maxMPapBytes * (fields / fieldsPerGroup)) + (fields * avgFieldSize));
        int maxGroupCount = operationIters * fields / fieldsPerGroup;

        int[] tokenLookup = HomogeniousRecordWriteReadLongBenchmark.buildTokens(fields, types, operators);
        byte[] writeBuffer = new byte[streamByteSize];

        // /////////////////////////////
        // test the writing performance.
        // ////////////////////////////

        long byteCount = performanceWriteTest(fields, fields, fields, singleCharLength, fieldsPerGroup, maxMPapBytes,
                operationIters, warmup, sampleSize, writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

        // /////////////////////////////
        // test the reading performance.
        // ////////////////////////////

        performanceReadTest(fields, fields, fields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters,
                warmup, sampleSize, readLabel, streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);

        int i = 0;
        for (CharSequence d : testData) {
            i += d.length();
        }

        long dataCount = (operationIters * (long) fields * (long) i) / testData.length;

        System.out.println("FullData:" + dataCount + " XmitData:" + byteCount + " compression:"
                + (byteCount / (float) dataCount));

    }

    @Override
    protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
            int[] tokenLookup, DictionaryFactory dcr) {

        FASTWriterDispatch fw = new FASTWriterDispatch(pw, dcr, 100, 64, 64, 8, 8, null, 3, new int[0][0], null, 64);

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
                    if (testNullString(i, token)) {
                        fw.write(token);
                    } else {
                        if (testCharSequence(i)) {
                            fw.write(token, testConstSeq);
                        } else {
                            char[] array = testConst;
                            fw.write(token, array, 0, array.length);
                        }
                    }
                } else {
                    if (testNullString(f, token)) {
                        fw.write(token);
                    } else {
                        if (testCharSequence(i)) {
                            fw.write(token, testData[f]);
                        } else {
                            char[] array = testDataChars[f];
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

    private boolean testNullString(int i, int token) {
        return sendNulls && ((i & NULL_SEND_MASK) == 0) && TokenBuilder.isOptional(token);
    }

    private boolean testCharSequence(int i) {
        return (i & 1) == 0;
    }

    @Override
    protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
            int[] tokenLookup, DictionaryFactory dcr) {

        PrimitiveReader.reset(reader);
        FASTReaderDispatch fr = new FASTReaderDispatch(reader, dcr, 3, new int[0][0], 300, 0, 4, 4, null, 64, 8, 7);
        TextHeap textHeap = fr.textHeap;

        long start = System.nanoTime();
        int i = operationIters;
        if (i < 3) {
            throw new UnsupportedOperationException("must allow operations to have 3 data points but only had " + i);
        }
        int g = fieldsPerGroup;
        int groupToken = TokenBuilder.buildToken(TypeMask.Group, maxMPapBytes > 0 ? OperatorMask.Group_Bit_PMap : 0,
                maxMPapBytes, TokenBuilder.MASK_ABSENT_DEFAULT);

        fr.openGroup(groupToken, maxMPapBytes);
        StringBuilder builder = new StringBuilder();

        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                int token = tokenLookup[f];
                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && (i & NULL_SEND_MASK) == 0 && TokenBuilder.isOptional(token)) {

                        int textIdx = fr.readText(tokenLookup[f]);
                        if (!textHeap.isNull(textIdx)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(tokenLookup[f]), Boolean.TRUE,
                                    textHeap.isNull(textIdx));
                        }

                    } else {
                        try {
                            int textIdx = fr.readText(tokenLookup[f]);

                            char[] tdc = testConst;

                            if (!textHeap.equals(textIdx, tdc, 0, tdc.length)) {

                                assertEquals("Error:" + TokenBuilder.tokenToString(tokenLookup[f]), testConst, textHeap
                                        .get(textIdx, new StringBuilder()).toString());
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new FASTException(e);
                        }
                    }
                } else {
                    if (sendNulls && (f & NULL_SEND_MASK) == 0 && TokenBuilder.isOptional(token)) {

                        int textIdx = fr.readText(tokenLookup[f]);
                        if (!textHeap.isNull(textIdx)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(tokenLookup[f])
                                    + "Expected null found len " + textHeap.length(textIdx), Boolean.TRUE,
                                    textHeap.isNull(textIdx));
                        }
                    } else {
                        try {
                            int textIdx = fr.readText(tokenLookup[f]);
                            char[] tdc = testDataChars[f];

                            if (!textHeap.equals(textIdx, tdc, 0, tdc.length)) {

                                builder.setLength(0);
                                assertEquals("Error:" + TokenBuilder.tokenToString(tokenLookup[f]), testData[f],
                                        textHeap.get(textIdx, new StringBuilder()).toString());
                            }

                        } catch (Exception e) {
                            System.err.println("expected text; " + testData[f]);
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
            fr.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), idx);
        }
        long duration = System.nanoTime() - start;
        return duration;
    }

    private char[] seqToArray(CharSequence seq) {
        char[] result = new char[seq.length()];
        int i = seq.length();
        while (--i >= 0) {
            result[i] = seq.charAt(i);
        }
        return result;
    }

    private CharSequence[] buildTestData(int count) {

        CharSequence[] seedData = ReaderWriterPrimitiveTest.stringData;
        int s = seedData.length;
        int i = count;
        CharSequence[] target = new CharSequence[count];
        while (--i >= 0) {
            target[i] = seedData[--s];
            if (0 == s) {
                s = seedData.length;
            }
        }
        return target;
    }

    public long totalWritten() {
        return pw.totalWritten();
    }

    protected void resetOutputWriter() {
        output.reset();
        pw.reset();
    }

    protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
        output = new FASTOutputByteArray(writeBuffer);
        pw = new PrimitiveWriter(writeBuffer.length, output, maxGroupCount, false);
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
