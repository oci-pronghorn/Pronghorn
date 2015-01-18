package com.ociweb.jfast.generator;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;

public class StaticGlue {

    //May want to move to dispatch, used in 4 places.
    public static int readASCIIToHeapNone(int idx, byte val, LocalHeap byteHeap, PrimitiveReader reader) {
        // 0x80 is a null string.
        // 0x00, 0x80 is zero length string
        if (0 == val) {
            // almost never happens
            LocalHeap.setZeroLength(idx, byteHeap);
            // must move cursor off the second byte
            val = PrimitiveReader.readTextASCIIByte(reader); // < .1%
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
            return 0;//length
        } else {
            // happens rarely when it equals 0x80
            LocalHeap.setNull(idx, byteHeap);
            return -1;//length
        }
    }
    //May want to move to dispatch, used in 3 places.
    public static int readASCIIToHeapValue(int idx, byte val, int chr, LocalHeap byteHeap,
            PrimitiveReader primitiveReader) {

        if (val < 0) {
            LocalHeap.setSingleByte((byte) chr, idx, byteHeap);
            return 1;
        } else {
            readASCIIToHeapValueLong(val, idx, byteHeap, primitiveReader);
            return LocalHeap.valueLength(idx,byteHeap);
        }
    }
    private static void readASCIIToHeapValueLong(byte val, int idx, LocalHeap byteHeap, PrimitiveReader primitiveReader) {
        final int offset = idx << 2;
        int targIndex = byteHeap.tat[offset]; // because we have zero length

        int nextLimit;
        int off4;

        // ensure there is enough space for the text
        if (targIndex >= (nextLimit = byteHeap.tat[off4 = offset + 4])) {
            byteHeap.tat[offset + 1] = byteHeap.tat[offset];// set to zero
                                                            // length
            byteHeap.makeSpaceForAppend(offset, 2); // also space for last char
            targIndex = byteHeap.tat[offset + 1];
            nextLimit = byteHeap.tat[off4];
        }

        // copy all the text into the heap
        byteHeap.tat[offset + 1] = byteHeap.tat[offset];// set to zero length
        byteHeap.tat[offset + 1] = fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, byteHeap,
                primitiveReader);
    }
    


    private static int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex,
            LocalHeap byteHeap, PrimitiveReader reader) {
        LocalHeap.rawAccess(byteHeap)[targIndex++] = (byte) val;

        int len;
        do {
            len = PrimitiveReader.readTextASCII(LocalHeap.rawAccess(byteHeap), targIndex, nextLimit, reader);
            if (len < 0) {
                targIndex -= len;
                byteHeap.makeSpaceForAppend(offset, 2); // also space for last
                                                        // char
                nextLimit = byteHeap.tat[off4];
            } else {
                targIndex += len;
            }
        } while (len < 0);
        return targIndex;
    }


    public static int readASCIIHead(final int idx, int trim, LocalHeap byteHeap, PrimitiveReader reader) {
        

        if (trim < 0) {
            byteHeap.trimHead(idx, -trim);
        }

        byte value = PrimitiveReader.readTextASCIIByte(reader);
        int offset = idx << 2;
        int nextLimit = byteHeap.tat[offset + 4];

        if (trim >= 0) {
            while (value >= 0) {
                nextLimit = byteHeap.appendTail(offset, nextLimit, value);
                value = PrimitiveReader.readTextASCIIByte(reader);
            }
            byteHeap.appendTail(offset, nextLimit, (byte)(value & 0x7F));
        } else {
            while (value >= 0) {
                LocalHeap.appendHead(offset,value,byteHeap);
                value = PrimitiveReader.readTextASCIIByte(reader);
            }
            LocalHeap.appendHead(offset,(byte) (value & 0x7F),byteHeap);
        }

        return idx;
    }


    public static int readASCIITail(final int idx, LocalHeap byteHeap, PrimitiveReader reader, int tail) {
        byteHeap.trimTail(idx, tail);       
        byte val = PrimitiveReader.readTextASCIIByte(reader);
        if (val == 0) {
            // nothing to append
            // must move cursor off the second byte
            val = PrimitiveReader.readTextASCIIByte(reader);
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
        } else {
            if (val == (byte) 0x80) {
                // nothing to append
                LocalHeap.setNull(idx, byteHeap);
            } else {
                if (LocalHeap.isNull(idx,byteHeap)) {
                    LocalHeap.setZeroLength(idx, byteHeap);
                }
                fastHeapAppend(idx, val, byteHeap, reader);
            }
        }
        return idx;
    }
    
    private static void fastHeapAppend(int idx, byte val, LocalHeap byteHeap, PrimitiveReader primitiveReader) {
        final int offset = idx << 2;
        final int off4 = offset + 4;
        final int off1 = offset + 1;
        int nextLimit = byteHeap.tat[off4];
        int targIndex = byteHeap.tat[off1];

        if (targIndex >= nextLimit) {
            byteHeap.makeSpaceForAppend(offset, 2); // also space for last char
            targIndex = byteHeap.tat[off1];
            assert(targIndex>=0);
            nextLimit = byteHeap.tat[off4];
        }
        assert(targIndex>=0);
        
        if (val < 0) {
            // heap.setSingleCharText((char)(0x7F & val), targIndex);
            LocalHeap.rawAccess(byteHeap)[targIndex++] = (byte) (0x7F & val);
        } else {
            targIndex = StaticGlue.fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, byteHeap,
                    primitiveReader);
        }
        byteHeap.tat[off1] = targIndex;
    }

    
    ///////////////////
    //These methods are here for package access to the needed methods.
    ///////////////////
    
    
    public static int readASCIIToHeap(int idx, PrimitiveReader reader, LocalHeap byteHeap) {
        byte val = PrimitiveReader.readTextASCIIByte(reader);  
        int tmp = 0x7F & val;
        return (0 != tmp) ?
               readASCIIToHeapValue(idx, val, tmp, byteHeap, reader) :
               readASCIIToHeapNone(idx, val, byteHeap, reader);
    }
    
    public static void readLongSignedDeltaOptional(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask,
            PaddedLong rbPos, long value) {
        long tmpLng = rLongDictionary[idx] = (rLongDictionary[source] + (value > 0 ? value - 1 : value));
        RingBuffer.addLongValue(rbB,rbMask,rbPos, tmpLng);
    }

}
