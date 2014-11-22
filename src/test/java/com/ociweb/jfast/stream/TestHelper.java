package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class TestHelper {

    public static long readLong(int token, PrimitiveReader reader, RingBuffer ringBuffer, FASTReaderInterpreterDispatch readerInterpreterDispatch) {
    
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
    
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readLongUnsigned(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readLongSigned(token, readerInterpreterDispatch.rLongDictionary, readerInterpreterDispatch.MAX_LONG_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readLongUnsignedOptional(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readLongSignedOptional(token, readerInterpreterDispatch.rLongDictionary, readerInterpreterDispatch.MAX_LONG_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        }
        //NOTE: for testing we need to check what was written
        return RingBuffer.peekLong(ringBuffer.buffer, ringBuffer.workingHeadPos.value-2, ringBuffer.mask);
    }

    public static int readInt(int token, PrimitiveReader reader, RingBuffer ringBuffer, FASTReaderInterpreterDispatch readerInterpreterDispatch) {
    
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readIntegerUnsigned(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readIntegerSigned(token, readerInterpreterDispatch.rIntDictionary, readerInterpreterDispatch.MAX_INT_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readIntegerUnsignedOptional(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readIntegerSignedOptional(token, readerInterpreterDispatch.rIntDictionary, readerInterpreterDispatch.MAX_INT_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        }
        //NOTE: for testing we need to check what was written
        return RingBuffer.peek(ringBuffer.buffer, ringBuffer.workingHeadPos.value-1, ringBuffer.mask);
    }

}
