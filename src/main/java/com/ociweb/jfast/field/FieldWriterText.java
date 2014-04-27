//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterText {

    public final TextHeap heap;
    private final PrimitiveWriter writer;
    public final int INSTANCE_MASK;
    public static final int INIT_VALUE_MASK = 0x80000000;

    public FieldWriterText(PrimitiveWriter writer, TextHeap charDictionary) {
        assert (null == charDictionary || charDictionary.itemCount() < TokenBuilder.MAX_INSTANCE);
        assert (null == charDictionary || TokenBuilder.isPowerOfTwo(charDictionary.itemCount()));

        this.INSTANCE_MASK = null == charDictionary ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,
                (charDictionary.itemCount() - 1));
        this.heap = charDictionary;
        this.writer = writer;
    }

    public void writeUTF8DeltaOptional(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;

        // count matching front or back chars
        int headCount = heap.countHeadMatch(idx, value);
        int tailCount = heap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = heap.length(idx) - headCount;
            writer.writeIntegerSigned(trimTail >= 0 ? trimTail + 1 : trimTail); // plus
                                                                                // 1
                                                                                // for
                                                                                // optional
            int length = (value.length() - headCount);
            writeUTF8Tail(idx, trimTail, headCount, value, length);
        } else {
            writeUTF8Head(value, idx, tailCount, 1);
        }

    }

    public void writeUTF8Delta(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;

        // count matching front or back chars
        int headCount = heap.countHeadMatch(idx, value);
        int tailCount = heap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = heap.length(idx) - headCount;
            writer.writeIntegerSigned(trimTail);
            writeUTF8Tail(idx, trimTail, headCount, value, value.length() - headCount);
        } else {
            writeUTF8Head(value, idx, tailCount, 0);
        }
    }

    private void writeUTF8Head(CharSequence value, int idx, int tailCount, int optional) {

        // replace head, tail matches to tailCount
        int trimHead = heap.length(idx) - tailCount;
        writer.writeIntegerSigned(0 == trimHead ? optional : -trimHead);

        int valueSend = value.length() - tailCount;

        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTFBefore(value, valueSend);
        heap.appendHead(idx, trimHead, value, valueSend);
    }

    public void writeUTF8TailOptional(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;
        int headCount = heap.countHeadMatch(idx, value);
        int trimTail = heap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail + 1);// plus 1 for optional
        int length = (value.length() - headCount);
        writeUTF8Tail(idx, trimTail, headCount, value, length);
    }

    public void writeUTF8Tail(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;
        
        
        int headCount = heap.countHeadMatch(idx, value);
        int trimTail = heap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        int length = (value.length() - headCount);
        writeUTF8Tail(idx, trimTail, headCount, value, length);
    }

    private void writeUTF8Tail(int idx, int firstRemove, int startAfter, CharSequence value, int length) {
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTFAfter(startAfter, value);
        heap.appendTail(idx, firstRemove, startAfter, value);
    }

    public void writeUTF8ConstantOptional(int token) {
        writer.writePMapBit((byte) 1);
    }

    public void writeASCIIDeltaOptional(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;

        if (null == value) {
            writer.writeIntegerSigned(0);
            return;
        }

        // count matching front or back chars
        int headCount = heap.countHeadMatch(idx, value);
        int tailCount = heap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = heap.length(idx) - headCount;
            assert (trimTail >= 0);
            writer.writeIntegerSigned(trimTail + 1);// must add one because this
                                                    // is optional
            writeASCIITail(idx, headCount, value, trimTail);
        } else {
            writeASCIIHead(idx, tailCount, value, 1);
        }
    }

    public void writeASCIIDelta(int token, CharSequence value) {
        int idx = token & INSTANCE_MASK;

        // count matching front or back chars
        int headCount = heap.countHeadMatch(idx, value);
        int tailCount = heap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = heap.length(idx) - headCount;
            if (trimTail < 0) {
                throw new UnsupportedOperationException(trimTail + "");
            }
            writer.writeIntegerSigned(trimTail);
            writeASCIITail(idx, headCount, value, trimTail);
        } else {
            writeASCIIHead(idx, tailCount, value, 0);
        }
    }

    private void writeASCIIHead(int idx, int tailCount, CharSequence value, int offset) {

        int trimHead = heap.length(idx) - tailCount;
        writer.writeIntegerSigned(0 == trimHead ? offset : -trimHead);

        int sentLen = value.length() - tailCount;
        writer.writeTextASCIIBefore(value, sentLen);
        heap.appendHead(idx, trimHead, value, sentLen);
    }

    private void writeASCIITail(int idx, int headCount, CharSequence value, int trimTail) {
        writer.writeTextASCIIAfter(headCount, value);
        heap.appendTail(idx, trimTail, headCount, value);
    }





}
