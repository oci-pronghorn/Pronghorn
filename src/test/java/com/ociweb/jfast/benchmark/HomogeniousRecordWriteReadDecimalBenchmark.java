//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.jfast.stream.BaseStreamingTest;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

public class HomogeniousRecordWriteReadDecimalBenchmark extends Benchmark {

    // Caliper tests///////////////
    // Write and read 1 record, this will time the duration of sending a fixed
    // value end to end.
    // --The records contain 10 fields of the same type and compression
    // operation
    // --This is an estimate of what kind of throughput can be achieved given
    // some test data.
    // --Both the static and dynamic readers/writers will be tested for full
    // comparisons
    // --lowLatency vs bandwidth optimized should both be tested
    //
    // This is NOT the same as the other tests which measure the duration to
    // produce 1 byte on the stream.
    // --The ns/byte tests are an estimate of how much bandwidth can be
    // saturated given the CPU available.

    static final int internalBufferSize = 512;
    static final int maxGroupCount = 10;
    static final int fields = 10;
    static final int singleCharLength = 128;

    // list all types
    static final int[] types = new int[] { TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional,
            TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.LongUnsigned,
            TypeMask.LongUnsignedOptional, TypeMask.LongSigned, TypeMask.LongSignedOptional, };

    // list all operators
    static final int[] operators = new int[] { OperatorMask.Field_None, OperatorMask.Field_Constant,
            OperatorMask.Field_Copy, OperatorMask.Field_Delta, OperatorMask.Field_Default,
            OperatorMask.Field_Increment, OperatorMask.Field_Tail };

    static final int[] tokenLookup = buildTokens(fields, types, operators);

    static final DictionaryFactory dictionaryFactory = new DictionaryFactory();
    static {
        dictionaryFactory.setTypeCounts(fields, fields, fields, 16, 128);
    }
    static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);

    static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
    static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);

    static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, false);
    static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount * 10);

    static final int[] intTestData = new int[] { 0, 0, 1, 1, 2, 2, 2000, 2002, 10000, 10001 };
    static final long[] longTestData = new long[] { 0, 0, 1, 1, 2, 2, 2000, 2002, 10000, 10001 };

    static final FASTWriterInterpreterDispatch staticWriter = FASTWriterInterpreterDispatch
			.createFASTWriterInterpreterDispatch(new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,4, 100, new ClientConfig(8 ,7) ));
    
    
    static final TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,  maxGroupCount * 10, -1, new ClientConfig(8, 7));
    
    static RingBuffers ringBuffers= RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)15, (byte)7, testCatalog.ringByteConstants(), testCatalog.getFROM())));
        
    static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(testCatalog, ringBuffers);

    static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_PMap, 2);
    static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group, 0, 0);

    public static int[] buildTokens(int count, int[] types, int[] operators) {
        int[] lookup = new int[count];
        int typeIdx = types.length - 1;
        int opsIdx = operators.length - 1;
        while (--count >= 0) {
            // high bit set
            // 7 bit type (must match method)
            // 4 bit operation (must match method)
            // 20 bit instance (MUST be lowest for easy mask and frequent use)

            // find next pattern to be used, rotating over them all.
            do {
                if (--typeIdx < 0) {
                    if (--opsIdx < 0) {
                        opsIdx = operators.length - 1;
                    }
                    typeIdx = types.length - 1;
                }
            } while (isInValidCombo(types[typeIdx], operators[opsIdx]));

            int tokenType = types[typeIdx];
            int tokenOpp = operators[opsIdx];
            lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count);

        }
        return lookup;

    }

    public static boolean isInValidCombo(int type, int operator) {
        boolean isOptional = 1 == (type & 0x01);

        if (OperatorMask.Field_Constant == operator & isOptional) {
            // constant operator can never be of type optional
            return true;
        }

        if (type >= 0 && type <= TypeMask.LongSignedOptional) {
            // integer/long types do not support tail
            if (OperatorMask.Field_Tail == operator) {
                return true;
            }
        }

        return false;
    }

    //
    // /
    // //
    // /
    //

    public int timeStaticOverhead(int reps) {
        return staticWriteReadOverheadGroup(reps);
    }

    @Test
    public void testThis() {
        assertTrue(0 != timeStaticDecimalDelta(20));
    }

    public long timeStaticDecimalNone(int reps) {
        return staticWriteReadDecimalGroup(
                reps,
                TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_None, 0),
                groupTokenNoMap, 0);
    }

    public long timeStaticDecimalNoneOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_None, 0), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalCopy(int reps) {
        return staticWriteReadDecimalGroup(
                reps,
                TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Copy, 0),
                groupTokenMap, 4);
    }

    public long timeStaticDecimalCopyOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Copy, 0), groupTokenMap, 4);
    }

    public long timeStaticDecimalConstant(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(// special
                                                                         // because
                                                                         // there
                                                                         // is
                                                                         // no
                                                                         // optional
                                                                         // constant
                TypeMask.Decimal, // constant operator can not be optional
                OperatorMask.Field_Constant, 0), groupTokenMap, 4);
    }

    public long timeStaticDecimalDefault(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Default,
                0), groupTokenMap, 4);
    }

    public long timeStaticDecimalDefaultOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Default, 0), groupTokenMap, 4);
    }

    public long timeStaticDecimalDelta(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Delta, 0), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalDeltaOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Delta, 0), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalIncrement(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal,
                OperatorMask.Field_Increment, 0), groupTokenMap, 4);
    }

    public long timeStaticDecimalIncrementOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Increment, 0), groupTokenMap, 4);
    }

    //
    // //
    // ///
    // //
    //

    //
    // //
    // ////
    // //
    //

    protected int staticWriteReadOverheadGroup(int reps) {
        int result = 0;
        int groupToken = groupTokenNoMap;
        int pmapSize = 0;
        RingBuffer rbRingBuffer = RingBuffers.get(staticReader.ringBuffers,0);
        for (int i = 0; i < reps; i++) {
            output.reset(); // reset output to start of byte buffer
            PrimitiveWriter.reset(writer); // clear any values found in writer

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticWriter.reset(); //reset message to clear out old values;

            // ////////////////////////////////////////////////////////////////
            // This is an example of how to use the staticWriter
            // Note that this is fast but does not allow for dynamic templates
            // ////////////////////////////////////////////////////////////////
            staticWriter.openGroup(groupToken, pmapSize, writer);
            int j = intTestData.length;
            while (--j >= 0) {
                result |= intTestData[j];// do nothing
            }
            staticWriter.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), writer);
            staticWriter.flush(writer);

            input.reset(); // for testing reset bytes back to the beginning.
            PrimitiveReader.reset(reader);// for testing clear any data found in reader

            FASTDecoder.reset(dictionaryFactory, staticReader); // reset message to clear the previous values

            staticReader.openGroup(groupToken, pmapSize, reader);
            j = intTestData.length;
            while (--j >= 0) {
                result |= j;// doing more nothing.
            }
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            
            //ringBuffers.
            
            staticReader.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), idx, reader);
        }
        return result;
    }
    RingBuffer rbRingBufferLocal = new RingBuffer(new RingBufferConfig((byte)2, (byte)2, null, FieldReferenceOffsetManager.RAW_BYTES));

    protected long staticWriteReadDecimalGroup(int reps, int token, int groupToken, int pmapSize) {
        long result = 0;
        for (int i = 0; i < reps; i++) {
            output.reset(); // reset output to start of byte buffer
            PrimitiveWriter.reset(writer); // clear any values found in writer

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticWriter.reset(); //reset message to clear out old values;

            // ////////////////////////////////////////////////////////////////
            // This is an example of how to use the staticWriter
            // Note that this is fast but does not allow for dynamic templates
            // ////////////////////////////////////////////////////////////////
            staticWriter.openGroup(groupToken, pmapSize, writer);
            int j = longTestData.length;
            while (--j >= 0) {
                long mantissa = longTestData[j];
                assert (0 == (token & (2 << TokenBuilder.SHIFT_TYPE)));
                assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

                //bridge solution as the ring buffer is introduce into all the APIs
                RingBuffer.dump(rbRingBufferLocal);
                RingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,1);
                RingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,(int) (mantissa >>> 32));
                RingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,(int) (mantissa & 0xFFFFFFFF)); 
                RingBuffer.publishWrites(rbRingBufferLocal);
                int rbPos = 0;

                if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                                             
                      staticWriter.acceptIntegerSigned(token, rbPos, rbRingBufferLocal, writer);
                      staticWriter.acceptLongSigned(token, rbPos+1, rbRingBufferLocal, writer);
                } else {
                                                
                    int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                    
                    staticWriter.acceptIntegerSignedOptional(token, valueOfNull, rbPos, rbRingBufferLocal, writer);

                    staticWriter.acceptLongSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, rbPos+1, rbRingBufferLocal, writer);
                   
                }
            }
            staticWriter.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), writer);
            staticWriter.flush(writer);

            input.reset(); // for testing reset bytes back to the beginning.
            PrimitiveReader.reset(reader);// for testing clear any data found in reader

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticReader.reset(); //reset message to clear the previous
            // values

            RingBuffer rbRingBuffer = RingBuffers.get(staticReader.ringBuffers,0);
            
            staticReader.openGroup(groupToken, pmapSize, reader);
            j = intTestData.length;
            while (--j >= 0) {
                
				staticReader.dispatchReadByTokenForDecimal(reader,token,token, rbRingBuffer);
                result |= j;
            }
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            staticReader.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), idx, reader);
        }
        return result;
    }

}
