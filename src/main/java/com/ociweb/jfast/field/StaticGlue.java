package com.ociweb.jfast.field;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;
import com.ociweb.jfast.stream.FASTRingBufferReader;

public class StaticGlue {

    //May want to move to dispatch, used in 4 places.
    public static int readASCIIToHeapNone(int idx, byte val, TextHeap textHeap, PrimitiveReader reader) {
        // 0x80 is a null string.
        // 0x00, 0x80 is zero length string
        if (0 == val) {
            // almost never happens
            TextHeap.setZeroLength(idx, textHeap);
            // must move cursor off the second byte
            val = PrimitiveReader.readTextASCIIByte(reader); // < .1%
            // at least do a validation because we already have what we need
            assert ((val & 0xFF) == 0x80);
            return 0;//length
        } else {
            // happens rarely when it equals 0x80
            TextHeap.setNull(idx, textHeap);
            return -1;//length
        }
    }
    //May want to move to dispatch, used in 3 places.
    public static int readASCIIToHeapValue(int idx, byte val, int chr, TextHeap textHeap,
            PrimitiveReader primitiveReader) {

        if (val < 0) {
            TextHeap.setSingleCharText((char) chr, idx, textHeap);
            return 1;
        } else {
            readASCIIToHeapValueLong(val, idx, textHeap, primitiveReader);
            return textHeap.valueLength(idx);
        }
    }
    private static void readASCIIToHeapValueLong(byte val, int idx, TextHeap textHeap, PrimitiveReader primitiveReader) {
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
    


    private static int fastHeapAppendLong(byte val, final int offset, final int off4, int nextLimit, int targIndex,
            TextHeap textHeap, PrimitiveReader reader) {
        textHeap.rawAccess()[targIndex++] = (char) val;

        int len;
        do {
            len = PrimitiveReader.readTextASCII2(textHeap.rawAccess(), targIndex, nextLimit, reader);
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


    public static int readASCIIHead(final int idx, int trim, TextHeap textHeap, PrimitiveReader reader) {
        

        if (trim < 0) {
            textHeap.trimHead(idx, -trim);
        }

        byte value = PrimitiveReader.readTextASCIIByte(reader);
        int offset = idx << 2;
        int nextLimit = textHeap.tat[offset + 4];

        if (trim >= 0) {
            while (value >= 0) {
                nextLimit = textHeap.appendTail(offset, nextLimit, (char) value);
                value = PrimitiveReader.readTextASCIIByte(reader);
            }
            textHeap.appendTail(offset, nextLimit, (char) (value & 0x7F));
        } else {
            while (value >= 0) {
                textHeap.appendHead(offset, (char) value);
                value = PrimitiveReader.readTextASCIIByte(reader);
            }
            textHeap.appendHead(offset, (char) (value & 0x7F));
        }

        return idx;
    }


    public static int readASCIITail(final int idx, TextHeap textHeap, PrimitiveReader reader, int tail) {
        textHeap.trimTail(idx, tail);       
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
                TextHeap.setNull(idx, textHeap);
            } else {
                if (textHeap.isNull(idx)) {
                    TextHeap.setZeroLength(idx, textHeap);
                }
                fastHeapAppend(idx, val, textHeap, reader);
            }
        }
        return idx;
    }
    
    private static void fastHeapAppend(int idx, byte val, TextHeap textHeap, PrimitiveReader primitiveReader) {
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

    
    ///////////////////
    //These methods are here for package access to the needed methods.
    ///////////////////
    
    public static void allocateAndDeltaUTF8(final int idx, TextHeap textHeap, PrimitiveReader reader, int trim) {
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            int offset = textHeap.makeSpaceForAppend(idx, trim, utfLength);
            { 
                byte[] temp = new byte[utfLength];//TODO: A, hack remove
                
                PrimitiveReader.readByteData(temp,0,utfLength,reader);
                
                long charAndPos = 0;        
                while (charAndPos>>32 < utfLength  ) {
                    charAndPos = FASTRingBufferReader.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                    textHeap.rawAccess()[offset++]=(char)charAndPos;
                }
            }
        } else {
            // append to head
            int offset = textHeap.makeSpaceForPrepend(idx, -trim, utfLength);
            { 
                byte[] temp = new byte[utfLength];//TODO: A, hack remove
                
                PrimitiveReader.readByteData(temp,0,utfLength,reader);
                
                long charAndPos = 0;        
                while (charAndPos>>32 < utfLength  ) {
                    charAndPos = FASTRingBufferReader.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                    textHeap.rawAccess()[offset++]=(char)charAndPos;
                }
            }
        }
    }
    
    public static void allocateAndCopyUTF8(int idx, TextHeap textHeap, PrimitiveReader reader, int length) {
        int offset = textHeap.allocate(idx, length);
        { 
            byte[] temp = new byte[length];//TODO: A, hack remove
            
            PrimitiveReader.readByteData(temp,0,length,reader);
            
            long charAndPos = 0;        
            while (charAndPos>>32 < length  ) {
                charAndPos = FASTRingBufferReader.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                textHeap.rawAccess()[offset++]=(char)charAndPos;
            }
        }
    }

    public static void allocateAndAppendUTF8(int idx, TextHeap textHeap, PrimitiveReader reader, int utfLength, int t) {
        int offset = textHeap.makeSpaceForAppend(idx, t, utfLength);
        { 
            byte[] temp = new byte[utfLength];//TODO: A, hack remove
            
            PrimitiveReader.readByteData(temp,0,utfLength,reader);
            
            long charAndPos = 0;        
            while (charAndPos>>32 < utfLength  ) {
                charAndPos = FASTRingBufferReader.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                textHeap.rawAccess()[offset++]=(char)charAndPos;
            }
        }
    }
    public static int readASCIIToHeap(int idx, PrimitiveReader reader, TextHeap textHeap) {
        byte val = PrimitiveReader.readTextASCIIByte(reader);  
        int tmp = 0x7F & val;
        return (0 != tmp) ?
               readASCIIToHeapValue(idx, val, tmp, textHeap, reader) :
               readASCIIToHeapNone(idx, val, textHeap, reader);
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
        writer.writeIntegerSigned(trimHead==0? opt: -trimHead, writer); 
        
        int len = value.remaining() - tailCount;
        int offset = value.position();
        writer.writeIntegerUnsigned(len, writer);
        writer.writeByteArrayData(value, offset, len, writer);
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }
    public static void writeBytesTail(int idx, int headCount, ByteBuffer value, final int optional, LocalHeap byteHeap, PrimitiveWriter writer) {
                   
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional : trimTail, writer);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        writer.writeIntegerUnsigned(valueSend, writer);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend, writer);
        
    }
    public static void nullNoPMapInt(PrimitiveWriter writer, int[] dictionary, int idx) {
        dictionary[idx] = 0;
        writer.writeNull(writer);
    }
    public static void nullCopyIncInt(PrimitiveWriter writer, int[] dictionary, int source, int target) {
        if (0 == dictionary[source]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            dictionary[target] = 0;
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull(writer);
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
            writer.writeNull(writer);
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
