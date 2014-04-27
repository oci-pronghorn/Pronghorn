package com.ociweb.jfast.field;

import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class StaticGlue {

    public static int readASCIIToHeapNone(int idx, byte val, TextHeap textHeap, PrimitiveReader primitiveReader) {
        // 0x80 is a null string.
        // 0x00, 0x80 is zero length string
        if (0 == val) {
            // almost never happens
            textHeap.setZeroLength(idx);
            // must move cursor off the second byte
            val = primitiveReader.readTextASCIIByte(); // < .1%
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
            return 0;//length
        } else {
            // happens rarely when it equals 0x80
            textHeap.setNull(idx);
            return -1;//length
        }
    }

    public static void fastHeapAppend(int idx, byte val, TextHeap textHeap, PrimitiveReader primitiveReader) {
        final int offset = idx << 2;
        final int off4 = offset + 4;
        final int off1 = offset + 1;
        int nextLimit = textHeap.tat[off4];
        int targIndex = textHeap.tat[off1];

        if (targIndex >= nextLimit) {
            textHeap.makeSpaceForAppend(offset, 2); // also space for last char
            targIndex = textHeap.tat[off1];
            nextLimit = textHeap.tat[off4];
        }

        if (val < 0) {
            // heap.setSingleCharText((char)(0x7F & val), targIndex);
            textHeap.rawAccess()[targIndex++] = (char) (0x7F & val);
        } else {
            targIndex = StaticGlue.fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, textHeap,
                    primitiveReader);
        }
        textHeap.tat[off1] = targIndex;
    }

    public static int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex,
            TextHeap textHeap, PrimitiveReader primitiveReader) {
        textHeap.rawAccess()[targIndex++] = (char) val;

        int len;
        do {
            len = primitiveReader.readTextASCII2(textHeap.rawAccess(), targIndex, nextLimit);
            if (len < 0) {
                targIndex -= len;
                textHeap.makeSpaceForAppend(offset, 2); // also space for last
                                                        // char
                nextLimit = textHeap.tat[off4];
            } else {
                targIndex += len;
            }
        } while (len < 0);
        return targIndex;
    }

    public static int readASCIIToHeapValue(byte val, int chr, int idx, TextHeap textHeap,
            PrimitiveReader primitiveReader) {

        if (val < 0) {
            textHeap.setSingleCharText((char) chr, idx);
            return 1;
        } else {
            readASCIIToHeapValueLong(val, idx, textHeap, primitiveReader);
            return textHeap.valueLength(idx);
        }
    }

    public static void readASCIIToHeapValueLong(byte val, int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        final int offset = idx << 2;
        int targIndex = textHeap.tat[offset]; // because we have zero length

        int nextLimit;
        int off4;

        // ensure there is enough space for the text
        if (targIndex >= (nextLimit = textHeap.tat[off4 = offset + 4])) {
            textHeap.tat[offset + 1] = textHeap.tat[offset];// set to zero
                                                            // length
            textHeap.makeSpaceForAppend(offset, 2); // also space for last char
            targIndex = textHeap.tat[offset + 1];
            nextLimit = textHeap.tat[off4];
        }

        // copy all the text into the heap
        textHeap.tat[offset + 1] = textHeap.tat[offset];// set to zero length
        textHeap.tat[offset + 1] = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, textHeap,
                primitiveReader);
    }

    public static int readASCIIDeltaOptional2(int readFromIdx, int idx, int optionalTrim, TextHeap textHeap,
            PrimitiveReader primitiveReader) {
        return (optionalTrim > 0 ? StaticGlue.readASCIITail(idx, optionalTrim - 1, readFromIdx, textHeap,
                primitiveReader) : StaticGlue.readASCIIHead(idx, optionalTrim, readFromIdx, textHeap, primitiveReader));
    }

    public static int readASCIITail(final int idx, int trim, int readFromIdx, TextHeap textHeap,
            PrimitiveReader primitiveReader) {

        // TODO: B, if readFromIdx does not match idx must do different work.

        if (trim > 0) {
            textHeap.trimTail(idx, trim);
        }

        // System.err.println("read: trim "+trim);

        byte val = primitiveReader.readTextASCIIByte();
        if (val == 0) {
            // nothing to append
            // must move cursor off the second byte
            val = primitiveReader.readTextASCIIByte();
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
        } else {
            if (val == StaticGlue.NULL_STOP) {
                // nothing to append and sent value is null
                textHeap.setNull(idx);
            } else {
                if (textHeap.isNull(idx)) {
                    textHeap.setZeroLength(idx);
                }
                fastHeapAppend(idx, val, textHeap, primitiveReader);
            }
        }

        return idx;
    }

    public static int readASCIITailOptional(final int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        int tail = primitiveReader.readIntegerUnsigned();
        if (0 == tail) {
            textHeap.setNull(idx);
            return idx;
        }
        tail--;

        textHeap.trimTail(idx, tail);
        byte val = primitiveReader.readTextASCIIByte();
        if (val == 0) {
            // nothing to append
            // must move cursor off the second byte
            val = primitiveReader.readTextASCIIByte();
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
        } else {
            if (val == StaticGlue.NULL_STOP) {
                // nothing to append
                // charDictionary.setNull(idx);
            } else {
                if (textHeap.isNull(idx)) {
                    textHeap.setZeroLength(idx);
                }
                fastHeapAppend(idx, val, textHeap, primitiveReader);
            }
        }

        return idx;
    }

    public static int readASCIIHead(final int idx, int trim, int readFromIdx, TextHeap textHeap,
            PrimitiveReader primitiveReader) {
        if (trim < 0) {
            textHeap.trimHead(idx, -trim);
        }

        byte value = primitiveReader.readTextASCIIByte();
        int offset = idx << 2;
        int nextLimit = textHeap.tat[offset + 4];

        if (trim >= 0) {
            while (value >= 0) {
                nextLimit = textHeap.appendTail(offset, nextLimit, (char) value);
                value = primitiveReader.readTextASCIIByte();
            }
            textHeap.appendTail(offset, nextLimit, (char) (value & 0x7F));
        } else {
            while (value >= 0) {
                textHeap.appendHead(offset, (char) value);
                value = primitiveReader.readTextASCIIByte();
            }
            textHeap.appendHead(offset, (char) (value & 0x7F));
        }

        return idx;
    }

    public static int readUTF8Delta(final int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        int trim = primitiveReader.readIntegerSigned();
        int utfLength = primitiveReader.readIntegerUnsigned();
        if (trim >= 0) {
            // append to tail
            primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength),
                    utfLength);
        } else {
            // append to head
            primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength),
                    utfLength);
        }

        return idx;
    }

    public static int readUTF8DeltaOptional(final int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
        int trim = primitiveReader.readIntegerSigned();
        if (0 == trim) {
            textHeap.setNull(idx);
            return idx;
        }
        if (trim > 0) {
            trim--;// subtract for optional
        }

        int utfLength = primitiveReader.readIntegerUnsigned();
        if (trim >= 0) {
            // append to tail
            primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, trim, utfLength),
                    utfLength);
        } else {
            // append to head
            primitiveReader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForPrepend(idx, -trim, utfLength),
                    utfLength);
        }

        return idx;
    }

    public final static byte NULL_STOP = (byte) 0x80;

    //These methods are here for package access to the needed methods.
    
    public static void allocateAndCopyUTF8(int idx, TextHeap textHeap, PrimitiveReader reader, int length) {
        reader.readTextUTF8(textHeap.rawAccess(), textHeap.allocate(idx, length), length);
    }

    public static void allocateSpaceUT8Copy2(int idx, TextHeap textHeap, PrimitiveReader reader, int utfLength, int t) {
        reader.readTextUTF8(textHeap.rawAccess(), textHeap.makeSpaceForAppend(idx, t, utfLength), utfLength);
    }

}
