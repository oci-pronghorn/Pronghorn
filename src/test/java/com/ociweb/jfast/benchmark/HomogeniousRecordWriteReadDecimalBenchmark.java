//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
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

    static final DictionaryFactory dcr = new DictionaryFactory();
    static {
        dcr.setTypeCounts(fields, fields, fields, fields);
    }
    static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);

    static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
    static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);

    static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, maxGroupCount, false);
    static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount * 10);

    static final int[] intTestData = new int[] { 0, 0, 1, 1, 2, 2, 2000, 2002, 10000, 10001 };
    static final long[] longTestData = new long[] { 0, 0, 1, 1, 2, 2, 2000, 2002, 10000, 10001 };

    static final FASTWriterInterpreterDispatch staticWriter = new FASTWriterInterpreterDispatch(writer, dcr, 100, 64, 64, 8, 8, null, 3,
            new int[0][0], null, 64);
    static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(dcr, 3, new int[0][0], 0, 0, 4, 4, null, 64,
            8, 7);

    static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_PMap, 2,
            TokenBuilder.MASK_ABSENT_DEFAULT);
    static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group, 0, 0, TokenBuilder.MASK_ABSENT_DEFAULT);

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
            lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count, TokenBuilder.MASK_ABSENT_DEFAULT);

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
                TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_None, 0, TokenBuilder.MASK_ABSENT_DEFAULT),
                groupTokenNoMap, 0);
    }

    public long timeStaticDecimalNoneOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_None, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalCopy(int reps) {
        return staticWriteReadDecimalGroup(
                reps,
                TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Copy, 0, TokenBuilder.MASK_ABSENT_DEFAULT),
                groupTokenMap, 4);
    }

    public long timeStaticDecimalCopyOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Copy, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
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
                OperatorMask.Field_Constant, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
    }

    public long timeStaticDecimalDefault(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Default,
                0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
    }

    public long timeStaticDecimalDefaultOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Default, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
    }

    public long timeStaticDecimalDelta(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal, OperatorMask.Field_Delta, 0,
                TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalDeltaOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Delta, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
    }

    public long timeStaticDecimalIncrement(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.Decimal,
                OperatorMask.Field_Increment, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
    }

    public long timeStaticDecimalIncrementOptional(int reps) {
        return staticWriteReadDecimalGroup(reps, TokenBuilder.buildToken(TypeMask.DecimalOptional,
                OperatorMask.Field_Increment, 0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 4);
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
        for (int i = 0; i < reps; i++) {
            output.reset(); // reset output to start of byte buffer
            writer.reset(writer); // clear any values found in writer

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticWriter.reset(); //reset message to clear out old values;

            // ////////////////////////////////////////////////////////////////
            // This is an example of how to use the staticWriter
            // Note that this is fast but does not allow for dynamic templates
            // ////////////////////////////////////////////////////////////////
            staticWriter.openGroup(groupToken, pmapSize);
            int j = intTestData.length;
            while (--j >= 0) {
                result |= intTestData[j];// do nothing
            }
            staticWriter.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER));
            staticWriter.flush();

            input.reset(); // for testing reset bytes back to the beginning.
            PrimitiveReader.reset(reader);// for testing clear any data found in reader

            staticReader.reset(); // reset message to clear the previous values

            staticReader.openGroup(groupToken, pmapSize, reader);
            j = intTestData.length;
            while (--j >= 0) {
                result |= j;// doing more nothing.
            }
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            staticReader.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), idx, reader);
        }
        return result;
    }

    protected long staticWriteReadDecimalGroup(int reps, int token, int groupToken, int pmapSize) {
        long result = 0;
        for (int i = 0; i < reps; i++) {
            output.reset(); // reset output to start of byte buffer
            writer.reset(writer); // clear any values found in writer

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticWriter.reset(); //reset message to clear out old values;

            // ////////////////////////////////////////////////////////////////
            // This is an example of how to use the staticWriter
            // Note that this is fast but does not allow for dynamic templates
            // ////////////////////////////////////////////////////////////////
            staticWriter.openGroup(groupToken, pmapSize);
            int j = longTestData.length;
            while (--j >= 0) {
                long mantissa = longTestData[j];
                assert (0 == (token & (2 << TokenBuilder.SHIFT_TYPE)));
                assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

                if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                    staticWriter.acceptIntegerSigned(token, 1);
                    staticWriter.acceptLongSigned(token, mantissa);
                } else {
                    staticWriter.acceptIntegerSignedOptional(token, 1);

                    if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG == mantissa) {
                        int idx = token & staticWriter.longInstanceMask;

                        staticWriter.writeNullLong(token, idx, writer, staticWriter.longValues);
                    } else {
                        staticWriter.acceptLongSignedOptional(token, mantissa);
                    }
                }
            }
            staticWriter.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER));
            staticWriter.flush();

            input.reset(); // for testing reset bytes back to the beginning.
            PrimitiveReader.reset(reader);// for testing clear any data found in reader

            // Not a normal part of read/write record and will slow down test
            // (would be needed per template)
            // staticReader.reset(); //reset message to clear the previous
            // values

            staticReader.openGroup(groupToken, pmapSize, reader);
            j = intTestData.length;
            while (--j >= 0) {
                staticReader.readDecimalExponent(token, reader);
                result |= staticReader.readDecimalMantissa(token, reader);
            }
            int idx = TokenBuilder.MAX_INSTANCE & groupToken;
            staticReader.closeGroup(groupToken | (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER), idx, reader);
        }
        return result;
    }

}
