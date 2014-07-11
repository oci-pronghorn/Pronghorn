//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTRingBufferReader;

/**
 * Holds count of how many of each type of field is required and what the
 * default values are. Default(initial) values are sparse so only the index and
 * the value are kept in this class. Upon request this class will generate the
 * needed dictionary arrays fully populated.
 * 
 * 
 * @author Nathan Tippy
 * 
 */
public class DictionaryFactory { 

    /*
     * Dictionary: Max count of all fields for dictionary shared across
     * templates. With default values.
     * 
     * Catalog: tokens lookup and IDs span across templates and dictionary and
     * belong to the catalog. hold app opional return values for each field.
     * 
     * Template: templates hold ordered list of fields/tokens.
     * 
     * 
     * catalog template dictionary (shared between templates?)
     */

    char[][] appTypes = null;// appType identifiers used in XML

    private static final int INIT_GROW_STEP = 16;

    private int integerCount;
    private int longCount;
    private int charCount;
    private int bytesCount;

    private int integerInitCount;
    private int[] integerInitIndex;
    private int[] integerInitValue;

    private int longInitCount;
    private int[] longInitIndex;
    private long[] longInitValue;

    private int charInitCount;
    private int[] charInitIndex;
    private char[][] charInitValue;
    private int charInitTotalLength;

    private int byteInitCount;
    private int[] byteInitIndex;
    private byte[][] byteInitValue;

    private int byteInitTotalLength;
    
    int singleTextSize=64;
    int gapTextSize=8;
    int singleBytesSize=46; 
    int gapBytesSize=8;

    public DictionaryFactory() {

        this.integerInitCount = 0;
        this.integerInitIndex = new int[INIT_GROW_STEP];
        this.integerInitValue = new int[INIT_GROW_STEP];

        this.longInitCount = 0;
        this.longInitIndex = new int[INIT_GROW_STEP];
        this.longInitValue = new long[INIT_GROW_STEP];

        this.charInitCount = 0;
        this.charInitIndex = new int[INIT_GROW_STEP];
        this.charInitValue = new char[INIT_GROW_STEP][];

        this.byteInitCount = 0;
        this.byteInitIndex = new int[INIT_GROW_STEP];
        this.byteInitValue = new byte[INIT_GROW_STEP][];
    }

    public void setTypeCounts(int integerCount, int longCount, int charCount, int bytesCount) {
        this.integerCount = integerCount;
        this.longCount = longCount;
        this.charCount = charCount;
        this.bytesCount = bytesCount;
    }

    public DictionaryFactory(PrimitiveReader reader) {

        this.integerCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.longCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.charCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.bytesCount = PrimitiveReader.readIntegerUnsigned(reader);

        this.integerInitCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.integerInitIndex = new int[integerInitCount];
        this.integerInitValue = new int[integerInitCount];
        int c = integerInitCount;
        while (--c >= 0) {
            integerInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
            integerInitValue[c] = PrimitiveReader.readIntegerSigned(reader);
        }

        this.longInitCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.longInitIndex = new int[longInitCount];
        this.longInitValue = new long[longInitCount];
        c = longInitCount;
        while (--c >= 0) {
            longInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
            longInitValue[c] = PrimitiveReader.readLongSigned(reader);
        }

        this.charInitCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.charInitIndex = new int[charInitCount];
        this.charInitValue = new char[charInitCount][];
        c = charInitCount;
        while (--c >= 0) {
            charInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
            int len = PrimitiveReader.readIntegerUnsigned(reader);
            char[] value = new char[len];
            int offset = 0;
            { 
                byte[] temp = new byte[len];//TODO: A, hack remove
                
                PrimitiveReader.readByteData(temp,0,len,reader);
                
                long charAndPos = 0;        
                while (charAndPos>>32 < len  ) {
                    charAndPos = FASTRingBufferReader.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                    value[offset++]=(char)charAndPos;
                }
            }
            charInitValue[c] = value;
        }
        this.charInitTotalLength = PrimitiveReader.readIntegerUnsigned(reader);

        this.byteInitCount = PrimitiveReader.readIntegerUnsigned(reader);
        this.byteInitIndex = new int[byteInitCount];
        this.byteInitValue = new byte[byteInitCount][];
        c = byteInitCount;
        while (--c >= 0) {
            byteInitIndex[c] = PrimitiveReader.readIntegerUnsigned(reader);
            int len = PrimitiveReader.readIntegerUnsigned(reader);
            byte[] value = new byte[len];
            PrimitiveReader.readByteData(value, 0, len, reader);
            byteInitValue[c] = value;
        }
        byteInitTotalLength = PrimitiveReader.readIntegerUnsigned(reader);

    }

    public void save(PrimitiveWriter writer) {

        PrimitiveWriter.writeIntegerUnsigned(integerCount, writer);
        PrimitiveWriter.writeIntegerUnsigned(longCount, writer);
        PrimitiveWriter.writeIntegerUnsigned(charCount, writer);
        PrimitiveWriter.writeIntegerUnsigned(bytesCount, writer);

        PrimitiveWriter.writeIntegerUnsigned(integerInitCount, writer);
        int c = integerInitCount;
        while (--c >= 0) {
            PrimitiveWriter.writeIntegerUnsigned(integerInitIndex[c], writer);
            PrimitiveWriter.writeIntegerSigned(integerInitValue[c], writer);
        }

        PrimitiveWriter.writeIntegerUnsigned(longInitCount, writer);
        c = longInitCount;
        while (--c >= 0) {
            PrimitiveWriter.writeIntegerUnsigned(longInitIndex[c], writer);
            PrimitiveWriter.writeLongSigned(longInitValue[c], writer);
        }

        PrimitiveWriter.writeIntegerUnsigned(charInitCount, writer);
        c = charInitCount;
        while (--c >= 0) {
            PrimitiveWriter.writeIntegerUnsigned(charInitIndex[c], writer);
            char[] value = charInitValue[c];
            PrimitiveWriter.writeIntegerUnsigned(value.length, writer);
            int offset = 0;
            int length = value.length;
            PrimitiveWriter.ensureSpace(value.length,writer);
            
            //convert from chars to bytes
            //writeByteArrayData()
            byte[] buffer = writer.buffer;//TODO: A, do not expose this and use byte array
            int limit = writer.limit;
                    
            while (--length >= 0) {
                
                limit = FASTRingBufferReader.encodeSingleChar((int) value[offset++], buffer, limit);
            }
            writer.limit = limit;
        }
        PrimitiveWriter.writeIntegerUnsigned(charInitTotalLength, writer);

        PrimitiveWriter.writeIntegerUnsigned(byteInitCount, writer);
        c = byteInitCount;
        while (--c >= 0) {
            PrimitiveWriter.writeIntegerUnsigned(byteInitIndex[c], writer);
            byte[] value = byteInitValue[c];
            PrimitiveWriter.writeIntegerUnsigned(value.length, writer);
            PrimitiveWriter.writeByteArrayData(value, 0, value.length, writer);
        }
        PrimitiveWriter.writeIntegerUnsigned(byteInitTotalLength, writer);

        /*
         * Fastest searialize deserialize however its more verbose and there is
         * no object dectection and construction.
         * 
         * These files can be deleted and modified but those changes are only
         * refelected on startup. New templates can be added but an explicit
         * call must be made to load them. The new templates will be loaded
         * dynamicaly on first use but this is not recommended.
         */

    }

    public void addInitInteger(int idx, int value) {

        integerInitIndex[integerInitCount] = idx;
        integerInitValue[integerInitCount] = value;
        if (++integerInitCount >= integerInitValue.length) {
            int newLength = integerInitValue.length + INIT_GROW_STEP;
            int[] temp1 = new int[newLength];
            int[] temp2 = new int[newLength];
            System.arraycopy(integerInitIndex, 0, temp1, 0, integerInitValue.length);
            System.arraycopy(integerInitValue, 0, temp2, 0, integerInitValue.length);
            integerInitIndex = temp1;
            integerInitValue = temp2;
        }

    }

    public void addInitLong(int idx, long value) {

        longInitIndex[longInitCount] = idx;
        longInitValue[longInitCount] = value;
        if (++longInitCount >= longInitValue.length) {
            int newLength = longInitValue.length + INIT_GROW_STEP;
            int[] temp1 = new int[newLength];
            long[] temp2 = new long[newLength];
            System.arraycopy(longInitIndex, 0, temp1, 0, longInitIndex.length);
            System.arraycopy(longInitValue, 0, temp2, 0, longInitIndex.length);
            longInitIndex = temp1;
            longInitValue = temp2;
        }

    }

    public void addInit(int idx, char[] value) {

        charInitIndex[charInitCount] = idx;
        charInitValue[charInitCount] = value;
        charInitTotalLength += value.length;
        if (++charInitCount >= charInitValue.length) {
            int newLength = charInitValue.length + INIT_GROW_STEP;
            int[] temp1 = new int[newLength];
            char[][] temp2 = new char[newLength][];
            System.arraycopy(charInitIndex, 0, temp1, 0, charInitValue.length);
            System.arraycopy(charInitValue, 0, temp2, 0, charInitValue.length);
            charInitIndex = temp1;
            charInitValue = temp2;
        }
        // System.err.println("default   "+idx+" is "+new String(value));
    }

    public void addInit(int idx, byte[] value) {

        byteInitIndex[byteInitCount] = idx;
        byteInitValue[byteInitCount] = value;
        byteInitTotalLength += value.length;
        if (++byteInitCount >= byteInitValue.length) {
            int newLength = byteInitValue.length + INIT_GROW_STEP;
            int[] temp1 = new int[newLength];
            byte[][] temp2 = new byte[newLength][];
            System.arraycopy(byteInitIndex, 0, temp1, 0, byteInitValue.length);
            System.arraycopy(byteInitValue, 0, temp2, 0, byteInitValue.length);
            byteInitIndex = temp1;
            byteInitValue = temp2;
        }

    }

    public static int nextPowerOfTwo(int value) {
        int temp = value;
        int result = 0;
        while (0 != temp) {
            temp = temp >> 1;
            result++;
        }
        // System.err.println(value+" -> "+(1<<result));

        return 1 << result;
    }

    public int[] integerDictionary() {
        int[] array = new int[nextPowerOfTwo(integerCount)];
        int i = integerInitCount;
        while (--i >= 0) {
            array[integerInitIndex[i]] = integerInitValue[i];
        }
        return array;
    }

    public long[] longDictionary() {
        long[] array = new long[nextPowerOfTwo(longCount)];
        int i = longInitCount;
        while (--i >= 0) {
            array[longInitIndex[i]] = longInitValue[i];
        }
        return array;
    }

    TextHeap textHeap;
    LocalHeap byteHeap;    
    
    public TextHeap charDictionary() {
        if (charCount == 0) {
            return null;
        }
        if (null==textHeap) {
            textHeap = new TextHeap(singleTextSize, gapTextSize, nextPowerOfTwo(charCount), charInitTotalLength,
                    charInitIndex, charInitValue);
            textHeap.reset();
        }
        return textHeap;
    }

    public LocalHeap byteDictionary() {
        if (bytesCount == 0) {
            return null;
        }
        if (null==byteHeap) {
            byteHeap = new LocalHeap(singleBytesSize, gapBytesSize, nextPowerOfTwo(bytesCount), byteInitTotalLength,
                    byteInitIndex, byteInitValue);
            byteHeap.reset();
        }
        
        return byteHeap;
    }

    public void reset(int[] values) {
        int i = integerCount;
        while (--i >= 0) {
            values[i] = 0;
        }
        i = integerInitCount;
        while (--i >= 0) {
            values[integerInitIndex[i]] = integerInitValue[i];
        }
    }

    public void reset(long[] values) {
        int i = longCount;
        while (--i >= 0) {
            values[i] = 0;
        }
        i = longInitCount;
        while (--i >= 0) {
            values[longInitIndex[i]] = longInitValue[i];
        }
    }

    public void reset(TextHeap heap) {
        if (null != heap) {
            heap.reset();
        }
    }

    public void reset(LocalHeap heap) {
        if (null != heap) {
            heap.reset();
        }
    }

}
