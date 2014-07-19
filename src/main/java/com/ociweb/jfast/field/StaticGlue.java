package com.ociweb.jfast.field;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;
import com.ociweb.jfast.stream.FASTRingBufferReader;

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
            return byteHeap.valueLength(idx);
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
        byteHeap.rawAccess()[targIndex++] = (byte) val;

        int len;
        do {
            len = PrimitiveReader.readTextASCII2(byteHeap.rawAccess(), targIndex, nextLimit, reader);
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
                byteHeap.appendHead(offset, value);
                value = PrimitiveReader.readTextASCIIByte(reader);
            }
            byteHeap.appendHead(offset, (byte) (value & 0x7F));
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
                if (byteHeap.isNull(idx)) {
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
            byteHeap.rawAccess()[targIndex++] = (byte) (0x7F & val);
        } else {
            targIndex = StaticGlue.fastHeapAppendLong(val, offset, off4, nextLimit, targIndex, byteHeap,
                    primitiveReader);
        }
        byteHeap.tat[off1] = targIndex;
    }

    
    ///////////////////
    //These methods are here for package access to the needed methods.
    ///////////////////
    
    
    public static void allocateAndDeltaUTF8(final int idx, LocalHeap heap, PrimitiveReader reader, int trim) {
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            PrimitiveReader.readByteData(heap.rawAccess(),heap.makeSpaceForAppend(idx, trim, utfLength),utfLength,reader);
        } else {
            // append to head
            PrimitiveReader.readByteData(heap.rawAccess(),heap.makeSpaceForPrepend(idx, -trim, utfLength),utfLength,reader);
        }
    }
       
    public static void allocateAndCopyUTF8(int idx, LocalHeap heap, PrimitiveReader reader, int length) {

        PrimitiveReader.readByteData(heap.rawAccess(),heap.allocate(idx, length),length,reader);
        
    }
   
    public static void allocateAndAppendUTF8(int idx, LocalHeap heap, PrimitiveReader reader, int utfLength, int t) {
        
        PrimitiveReader.readByteData(heap.rawAccess(),heap.makeSpaceForAppend(idx, t, utfLength),utfLength,reader);
    } 
    
    
    
    
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
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    //byte methods
    public static void writeBytesHead(int idx, int tailCount, ByteBuffer value, int opt, LocalHeap byteHeap, PrimitiveWriter writer) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(idx)-tailCount;
        PrimitiveWriter.writeIntegerSigned(trimHead==0? opt: -trimHead, writer); 
        
        int len = value.remaining() - tailCount;
        int offset = value.position();
        PrimitiveWriter.writeIntegerUnsigned(len, writer);
        PrimitiveWriter.writeByteArrayData(value, offset, len, writer);
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }
    public static void writeBytesTail(int idx, int headCount, ByteBuffer value, final int optional, LocalHeap byteHeap, PrimitiveWriter writer) {
                   
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+optional : trimTail, writer);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        
    }
    public static void nullNoPMapInt(PrimitiveWriter writer, int[] dictionary, int idx) {
        dictionary[idx] = 0;
        PrimitiveWriter.writeNull(writer);
    }
    public static void nullCopyIncInt(PrimitiveWriter writer, int[] dictionary, int source, int target) {
        if (0 == dictionary[source]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            dictionary[target] = 0;
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }
    public static void nullPMap(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 0, writer);
    }
    public static void nullDefaultInt(PrimitiveWriter writer, int[] dictionary, int source) {
        if (0 == dictionary[source]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }
    public static final int readIntegerUnsignedCopy(int target, int source, int[] dictionary, PrimitiveReader reader) {
        //TODO: C, 4% perf problem in profiler, can be better if target== source ???
        return dictionary[target] = (PrimitiveReader.readPMapBit(reader) == 0 ? dictionary[source] : PrimitiveReader.readIntegerUnsigned(reader));
    }
    public static final int readIntegerSignedCopy(int target, int source, int[] dictionary, PrimitiveReader reader) {
        //TODO: C, 4% perf problem in profiler, can be better if target== source ???
        return (PrimitiveReader.readPMapBit(reader) == 0 ? dictionary[source] : (dictionary[target] = PrimitiveReader.readIntegerSigned(reader)));
    }
    public static final long readLongUnsignedCopy(int target, int source, long[] dictionary, PrimitiveReader reader) {
        //TODO: B, can duplicate this to make a more effecient version when source==target
        return (PrimitiveReader.readPMapBit(reader) == 0 ? dictionary[source]
                : (dictionary[target] = PrimitiveReader.readLongUnsigned(reader)));
    }
    public static final long readLongSignedCopy(int target, int source, long[] dictionary, PrimitiveReader reader) {
        //TODO: B, can duplicate this to make a more effecient version when source==target
        return dictionary[target] = (PrimitiveReader.readPMapBit(reader) == 0 ? dictionary[source] : PrimitiveReader.readLongSigned(reader));
    }

}
