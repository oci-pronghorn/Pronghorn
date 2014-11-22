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
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;

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
        int[] types = new int[] { 
                TypeMask.ByteArray,
                TypeMask.ByteArrayOptional,
                };
        int[] operators = new int[] { 
                OperatorMask.Field_None, 
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Copy,
                OperatorMask.Field_Default,
                OperatorMask.Field_Delta, 
                OperatorMask.Field_Tail, 
                };

        byteTester(types, operators, "Bytes");
    }

    @Test
    public void TailTest() {

        byte[] buffer = new byte[2048];
        FASTOutput output = new FASTOutputByteArray(buffer);
        PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);

        int singleSize = 14;
        int singleGapSize = 8;
        int fixedTextItemCount = 16; // must be power of two

        LocalHeap dictionaryWriter = new LocalHeap(singleSize, singleGapSize, fixedTextItemCount);

        int token = TokenBuilder.buildToken(TypeMask.ByteArray, OperatorMask.Field_Tail, 0);
        byte[] value = new byte[] { 1, 2, 3 };
        int offset = 0;
        int length = value.length;
        int instanceMask = null==dictionaryWriter? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(dictionaryWriter)-1));
        writeBytesTail(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesDelta(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesConstant();

        PrimitiveWriter.openPMap(1, writer);
        writeBytesCopy(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        writeBytesDefault(writer, dictionaryWriter, instanceMask, token, value, offset, length);
        PrimitiveWriter.closePMap(writer);
        PrimitiveWriter.flush(writer);

        FASTInput input = new FASTInputByteArray(buffer);
        PrimitiveReader reader = new PrimitiveReader(2048, input, 32);

        LocalHeap dictionaryReader = new LocalHeap(singleSize, singleGapSize, fixedTextItemCount);
        int byteInstanceMask = null == dictionaryReader ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,
        LocalHeap.itemCount(dictionaryReader) - 1);

        // read value back
        int id;
        int idx = token & byteInstanceMask;
        id = readBytesTail(idx, reader, dictionaryReader);
        assertTrue(LocalHeap.equals(id,value,offset,length,0xFFFFFFFF,dictionaryReader));
        int idx1 = token & byteInstanceMask;

        id = readBytesDelta(idx1, reader, dictionaryReader);
        assertTrue(LocalHeap.equals(id,value,offset,length,0xFFFFFFFF,dictionaryReader));

        id = token & byteInstanceMask;
        assertTrue(LocalHeap.equals(id,value,offset,length,0xFFFFFFFF,dictionaryReader));

        PrimitiveReader.openPMap(1, reader);
        int idx2 = token & byteInstanceMask;
        if (PrimitiveReader.readPMapBit(reader) != 0) {
            int length1 = PrimitiveReader.readIntegerUnsigned(reader) - 0;
            PrimitiveReader.readByteData(LocalHeap.rawAccess(dictionaryReader), LocalHeap.allocate(idx2, length1, dictionaryReader), length1, reader);
        }

        id = idx2;
        assertTrue(LocalHeap.equals(id,value,offset,length,0xFFFFFFFF,dictionaryReader));
        int idx3 = token & byteInstanceMask;

        id = readBytesDefault(idx3, reader, dictionaryReader);
        assertTrue(LocalHeap.equals(id,value,offset,length,0xFFFFFFFF,dictionaryReader));

        PrimitiveReader.closePMap(reader);

    }

    private int readBytesTail(int idx, PrimitiveReader reader, LocalHeap byteHeap) {
        int id;
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        int length = PrimitiveReader.readIntegerUnsigned(reader);
        
        // append to tail
        int targetOffset = LocalHeap.makeSpaceForAppend(idx,trim,length,byteHeap);
        PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), targetOffset, length, reader);
        id = idx;
        return id;
    }

    
    private int readBytesDefault(int idx3, PrimitiveReader reader, LocalHeap byteHeap) {
        int id;
        int result;
        if (PrimitiveReader.readPMapBit(reader) == 0) {
            // System.err.println("z");
            result = idx3 | INIT_VALUE_MASK;// use constant
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.allocate(idx3, length, byteHeap), length, reader);
            result = idx3;
        }
        id = result;
        return id;
    }

    private int readBytesDelta(int idx1, PrimitiveReader reader, LocalHeap byteHeap) {
        int id;
        int trim = PrimitiveReader.readIntegerSigned(reader);
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForAppend(idx1,trim,utfLength,byteHeap), utfLength, reader);
        } else {
            // append to head
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForPrepend(idx1,-trim,utfLength,byteHeap), utfLength, reader);
        }
        id = idx1;
        return id;
    }

    private void writeBytesDefault(PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        if (LocalHeap.equals(idx|INIT_VALUE_MASK,value,offset,length,0xFFFFFFFF,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
        }
    }

    private void writeBytesCopy(PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        if (LocalHeap.equals(idx,value,offset,length,0xFFFFFFFF,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        }
        else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
        	LocalHeap.set(idx,value,offset,length,0xFFFFFFFF,byteHeap);
        }
    }

    private void writeBytesConstant() {
    }

    private void writeBytesDelta(PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        
        //count matching front or back chars
        int headCount = LocalHeap.countHeadMatch(idx,value,offset,length,0xFFFFFFFF,byteHeap);
        int tailCount = LocalHeap.countTailMatch(idx,value,offset+length,length,0xFFFFFFFF,byteHeap);
        if (headCount>tailCount) {
        	int trimTail = LocalHeap.length(idx,byteHeap)-headCount;
        	PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail, writer);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount+headCount;
            
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
            LocalHeap.appendTail(idx,trimTail,value,startAfter,valueSend,0xFFFFFFFF,byteHeap);
        } else {
        	//replace head, tail matches to tailCount
            int trimHead = LocalHeap.length(idx,byteHeap)-tailCount;
            PrimitiveWriter.writeIntegerSigned(trimHead==0? 0: -trimHead, writer); 
            
            int len = length - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeByteArrayData(value, offset, len, writer);
            
            LocalHeap.appendHead(idx,trimHead,value,offset,len,0xFFFFFFFF,byteHeap);
        }
    }

    private void writeBytesTail(PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask, int token, byte[] value, int offset, int length) {
        int idx = token & instanceMask;
        int headCount = LocalHeap.countHeadMatch(idx,value,offset,length,0xFFFFFFFF,byteHeap);
        
        int trimTail = LocalHeap.length(idx,byteHeap)-headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail, writer);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        LocalHeap.appendTail(idx,trimTail,value,startAfter,valueSend,0xFFFFFFFF,byteHeap);
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

        FASTWriterInterpreterDispatch fw = new FASTWriterInterpreterDispatch(new TemplateCatalogConfig(dcr, 3, new int[0][0], null,
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


        RingBuffer ring = new RingBuffer((byte)7,(byte)7,null, FieldReferenceOffsetManager.RAW_BYTES);
        RingBuffer.dump(ring);
        
        while (--i >= 0) {
            int f = fields;

            while (--f >= 0) {

                int token = tokenLookup[f];

                if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
                    if (sendNulls && ((i & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                    	
                    	byte[] array = testConst;
                    	
                        RingBuffer.dump(ring);
                        RingBuffer.addByteArray(array, 0, -1, ring);
                        RingBuffer.publishWrites(ring);
                    	
                        fw.acceptByteArrayOptional(token, writer, fw.byteHeap, 0, ring);
                    } else {
                        {
                            byte[] array = testConst;

                            RingBuffer.addByteArray(array, 0, array.length, ring);
                            RingBuffer.publishWrites(ring);
                            
                            assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
                            assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                            assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
                            
                            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                                fw.acceptByteArray(token, writer, fw.byteHeap, 0, ring);
                            } else {
                                fw.acceptByteArrayOptional(token, writer, fw.byteHeap, 0, ring);
                            }
                        }
                    }
                } else {
                    if (sendNulls && ((f & 0xF) == 0) && TokenBuilder.isOptional(token)) {
                        
                        BaseStreamingTest.writeNullBytes(token, writer, fw.byteHeap, fw.instanceBytesMask, fw);

                    } else {
                        {
                            byte[] array = testDataBytes[f];
                            
                            RingBuffer.dump(ring);
                            RingBuffer.addByteArray(array, 0, array.length, ring);
                            RingBuffer.publishWrites(ring);
                                                        
                            assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
                            assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                            assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
                            
                            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                                fw.acceptByteArray(token, writer, fw.byteHeap, 0, ring);
                            } else {
                                fw.acceptByteArrayOptional(token, writer, fw.byteHeap, 0, ring);
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
        
        FASTReaderInterpreterDispatch fr = new FASTReaderInterpreterDispatch(testCatalog);
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

                        int idx = fr.readBytes(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));
                        if (!LocalHeap.isNull(idx,byteHeap)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token), Boolean.TRUE, LocalHeap.isNull(idx,byteHeap));
                        }

                    } else {
                        try {
                            int textIdx = fr.readBytes(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));

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

                        int idx = fr.readBytes(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));
                        if (!LocalHeap.isNull(idx,byteHeap)) {
                            assertEquals("Error:" + TokenBuilder.tokenToString(token) + "Expected null found len "
                                    + LocalHeap.length(idx,byteHeap), Boolean.TRUE, LocalHeap.isNull(idx,byteHeap));
                        }

                    } else {
                        try {
                            int heapIdx = fr.readBytes(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0));

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
