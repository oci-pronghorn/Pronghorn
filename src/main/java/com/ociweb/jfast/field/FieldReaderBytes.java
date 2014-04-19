//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FieldReaderBytes {

    private static final int INIT_VALUE_MASK = 0x80000000;
    final byte NULL_STOP = (byte) 0x80;
    private final PrimitiveReader reader;
    private final ByteHeap heap;
    public final int INSTANCE_MASK;

    // TODO: X, improvement reader/writer bytes/chars should never build this
    // object when it is not in use.
    public FieldReaderBytes(PrimitiveReader reader, ByteHeap byteDictionary) {
        assert (null == byteDictionary || byteDictionary.itemCount() < TokenBuilder.MAX_INSTANCE);
        assert (null == byteDictionary || TokenBuilder.isPowerOfTwo(byteDictionary.itemCount()));

        this.INSTANCE_MASK = null == byteDictionary ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,
                byteDictionary.itemCount() - 1);

        this.reader = reader;
        this.heap = byteDictionary;
    }

    public int readBytesTail2(int idx) {
        int trim = reader.readIntegerUnsigned();
        int length = reader.readIntegerUnsigned();

        // append to tail
        int targetOffset = heap.makeSpaceForAppend(idx, trim, length);
        reader.readByteData(heap.rawAccess(), targetOffset, length);

        return idx;
    }

    public int readBytesDelta2(int idx) {
        int trim = reader.readIntegerSigned();
        int utfLength = reader.readIntegerUnsigned();
        if (trim >= 0) {
            // append to tail
            reader.readByteData(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
        } else {
            // append to head
            reader.readByteData(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
        }

        return idx;
    }

    public int readBytesDefault2(int idx) {
        if (reader.popPMapBit() == 0) {
            // System.err.println("z");
            return idx | INIT_VALUE_MASK;// use constant
        } else {
            return readBytesData(idx, 0);
        }
    }

    public int readBytesData(int idx, int optOff) {
        int length = reader.readIntegerUnsigned() - optOff;
        reader.readByteData(heap.rawAccess(), heap.allocate(idx, length), length);

        return idx;
    }

    public int readBytesTailOptional2(int idx) {
        int trim = reader.readIntegerUnsigned();
        if (trim == 0) {
            heap.setNull(idx);
            return idx;
        }
        trim--;

        int utfLength = reader.readIntegerUnsigned();

        // append to tail
        reader.readByteData(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);

        return idx;
    }

    public int readBytesDeltaOptional2(final int idx) {
        int trim = reader.readIntegerSigned();
        if (0 == trim) {
            heap.setNull(idx);
            return idx;
        }
        if (trim > 0) {
            trim--;// subtract for optional
        }

        int utfLength = reader.readIntegerUnsigned();

        if (trim >= 0) {
            // append to tail
            reader.readByteData(heap.rawAccess(), heap.makeSpaceForAppend(idx, trim, utfLength), utfLength);
        } else {
            // append to head
            reader.readByteData(heap.rawAccess(), heap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength);
        }

        return idx;
    }

    public ByteHeap byteHeap() {
        return heap;
    }

    public void reset(int idx) {
        if (null != heap) {
            heap.setNull(idx);
        }
    }

}
