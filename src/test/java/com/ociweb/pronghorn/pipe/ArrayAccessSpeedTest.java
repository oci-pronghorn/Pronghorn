package com.ociweb.pronghorn.pipe;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import com.ociweb.pronghorn.pipe.util.ArrayBacked;

/**
 * This class is not a test and does not run with the coverage tests.
 * 
 * This is here just to capture the relative performance of the solutions.
 * @author Nathan Tippy
 *
 */
public class ArrayAccessSpeedTest {

    private static final int bitSize = 19;
    private static final int size = 1<<bitSize;
    private static final int mask = size-1;
    private static final int iterations = 50;
 
    
    private static final byte[] byteArray = new byte[size];    
    private static final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[size]);
    private static final int[] arrayBackedBytes = new int[size>>2];
    private static final int   arrayBackedBytesMask = (size>>2)-1;
    
    
    private static final short[] shortArray = new short[size];    
    private static final ByteBuffer byteBufferForShort = ByteBuffer.wrap(new byte[size*2]);
    private static final ShortBuffer shortBuffer = byteBufferForShort.asShortBuffer();
    private static final int[] arrayBackedShorts = new int[size>>1];
    private static final int   arrayBackedShortsMask = (size>>1)-1;
    
    
    private static final int[] intArray = new int[size];    
    private static final ByteBuffer byteBufferForInt = ByteBuffer.wrap(new byte[size*4]);
    private static final IntBuffer intBuffer = byteBufferForInt.asIntBuffer();
    private static final int[] arrayBackedInts = new int[size];
    private static final int arrayBackedIntsMask = (size-1);
    
    
    private static final long[] longArray = new long[size];    
    private static final ByteBuffer byteBufferForLong = ByteBuffer.wrap(new byte[size*8]);
    private static final LongBuffer longBuffer = byteBufferForLong.asLongBuffer();
    private static final int[] arrayBackedLongs = new int[size*2];
    private static final int   arrayBackedLongsMask = (size*2)-1;
    
    
    ArrayAccessSpeedTest() {        
    }
    
    public static void main(String[] args) {
        ArrayAccessSpeedTest instance = new ArrayAccessSpeedTest();
        
        runByteTest(instance);
        runShortTest(instance);
        runIntTest(instance);        
        runLongTest(instance);
                
    }

    private static void runByteTest(ArrayAccessSpeedTest instance) {
        System.out.println("running array test");
        long arrayStart = System.currentTimeMillis();
        long arrayResult = instance.byteArrayLoop();
        long arrayDuration = System.currentTimeMillis()-arrayStart;
        
        System.out.println("running byte buffer test");
        long byteBufferStart = System.currentTimeMillis();
        long byteBufferResult = instance.byteBufferLoop();
        long byteBufferDuration = System.currentTimeMillis()-byteBufferStart;
        
        long byteBackedStart = System.currentTimeMillis();
        long byteBackedResult = instance.byteArrayBackedLoop();
        long byteBackedDuration = System.currentTimeMillis()-byteBackedStart;
        
        
        System.out.println("ByteArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("ByteBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
        System.out.println("ByteBacked Time: "+byteBackedDuration+" Value: "+byteBackedResult);
        
    }
    
    private static void runShortTest(ArrayAccessSpeedTest instance) {
        System.out.println("running array test");
        long arrayStart = System.currentTimeMillis();
        long arrayResult = instance.shortArrayLoop();
        long arrayDuration = System.currentTimeMillis()-arrayStart;
        
        System.out.println("running short buffer test");
        long byteBufferStart = System.currentTimeMillis();
        long byteBufferResult = instance.shortBufferLoop();
        long byteBufferDuration = System.currentTimeMillis()-byteBufferStart;
        
        long backedArrayStart = System.currentTimeMillis();
        long backedArrayResult = instance.shortArrayBackedLoop();
        long backedArrayDuration = System.currentTimeMillis()-backedArrayStart;
        
        System.out.println("ShortArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("ShortBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
        System.out.println("ShortBacked Time: "+backedArrayDuration+" Value: "+backedArrayResult);
    }
    
    private static void runIntTest(ArrayAccessSpeedTest instance) {
        System.out.println("running array test");
        long arrayStart = System.currentTimeMillis();
        long arrayResult = instance.intArrayLoop();
        long arrayDuration = System.currentTimeMillis()-arrayStart;
        
        System.out.println("running int buffer test");
        long byteBufferStart = System.currentTimeMillis();
        long byteBufferResult = instance.intBufferLoop(); //swap with inBuffer2Loop and get the same performance
        long byteBufferDuration = System.currentTimeMillis()-byteBufferStart;
                
        long backedArrayStart = System.currentTimeMillis();
        long backedArrayResult = instance.intArrayBackedLoop();
        long backedArrayDuration = System.currentTimeMillis()-backedArrayStart;
        
        
        System.out.println("IntArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("IntBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
        System.out.println("IntBacked Time: "+backedArrayDuration+" Value: "+backedArrayResult);
        
    }

    private static void runLongTest(ArrayAccessSpeedTest instance) {
        System.out.println("running array test");
        long arrayStart = System.currentTimeMillis();
        long arrayResult = instance.longArrayLoop();
        long arrayDuration = System.currentTimeMillis()-arrayStart;
        
        System.out.println("running long buffer test");
        long byteBufferStart = System.currentTimeMillis();
        long byteBufferResult = instance.longBufferLoop();
        long byteBufferDuration = System.currentTimeMillis()-byteBufferStart;
        
        long backedArrayStart = System.currentTimeMillis();
        long backedArrayResult = instance.longArrayBackedLoop();
        long backedArrayDuration = System.currentTimeMillis()-backedArrayStart;        
        
        
        System.out.println("LongArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("LongBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
        System.out.println("LongBacked Time: "+backedArrayDuration+" Value: "+backedArrayResult);
        
    }

    private long byteArrayLoop() {
        long pos = 1;
        byteArray[0] = 1;
        byteArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            byteArray[(int)pos&mask] = (byte)(byteArray[(int)(pos-2)&mask] + byteArray[(int)(pos-1)&mask]);
        }
        return byteArray[(int)(pos-2)&mask] + byteArray[(int)(pos-1)&mask];
    }
    
    private long byteArrayBackedLoop() {
        long pos = 1;
        
        ArrayBacked.writeByte(arrayBackedBytes, arrayBackedBytesMask, 0, (byte)1);
        ArrayBacked.writeByte(arrayBackedBytes, arrayBackedBytesMask, 1, (byte)2);
                
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            
            ArrayBacked.writeByte(arrayBackedBytes, arrayBackedBytesMask, pos, 
                    (byte)(ArrayBacked.readByte(arrayBackedBytes, arrayBackedBytesMask, pos-2) +
                          ArrayBacked.readByte(arrayBackedBytes, arrayBackedBytesMask, pos-1)));
        }
        return ArrayBacked.readByte(arrayBackedBytes, arrayBackedBytesMask, pos-2) +
                ArrayBacked.readByte(arrayBackedBytes, arrayBackedBytesMask, pos-1);
    }

    private long byteBufferLoop() {
        long pos = 1;
        byteBuffer.put(0,(byte)1);
        byteBuffer.put(1,(byte)2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            byteBuffer.put((int)pos&mask, (byte) (byteBuffer.get((int)(pos-2)&mask) + byteBuffer.get((int)(pos-1)&mask)));
        }
        return byteBuffer.get((int)(pos-2)&mask) + byteBuffer.get((int)(pos-1)&mask);
    }
    
    private long shortArrayLoop() {
        long pos = 1;
        shortArray[0] = 1;
        shortArray[1] = 1;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            shortArray[(int)pos&mask] = (short)(shortArray[(int)(pos-2)&mask] + shortArray[(int)(pos-1)&mask]);
        }
        return shortArray[(int)(pos-2)&mask] + shortArray[(int)(pos-1)&mask];
    }
    
    private long shortBufferLoop() {
        long pos = 1;
        shortBuffer.put(0,(short)1);
        shortBuffer.put(1,(short)1);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            shortBuffer.put((int)pos&mask, (short) (shortBuffer.get((int)(pos-2)&mask) + shortBuffer.get((int)(pos-1)&mask)));
        }
        return shortBuffer.get((int)(pos-2)&mask) + shortBuffer.get((int)(pos-1)&mask);
    }

    private long shortArrayBackedLoop() {
        long pos = 1;
        
        ArrayBacked.writeShort(arrayBackedShorts, arrayBackedShortsMask, 0, (short)1);
        ArrayBacked.writeShort(arrayBackedShorts, arrayBackedShortsMask, 2, (short)1);
                
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            long idx = pos<<1;
            
            ArrayBacked.writeShort(arrayBackedShorts, arrayBackedShortsMask, idx, 
                    (short)(ArrayBacked.readShort(arrayBackedShorts, arrayBackedShortsMask, idx-4) +
                          ArrayBacked.readShort(arrayBackedShorts, arrayBackedShortsMask, idx-2)));
        }
        return ArrayBacked.readShort(arrayBackedShorts, arrayBackedShortsMask, (pos<<1)-4) +
                ArrayBacked.readShort(arrayBackedShorts, arrayBackedShortsMask, (pos<<1)-2);
    }

    private long intArrayLoop() {
        long pos = 1;
        intArray[0] = 1;
        intArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            intArray[(int)pos&mask] = intArray[(int)(pos-2)&mask] + intArray[(int)(pos-1)&mask];
        }
        return intArray[(int)(pos-2)&mask] + intArray[(int)(pos-1)&mask];
    }

    private long intBufferLoop() {
        long pos = 1;
        intBuffer.put(0,1);
        intBuffer.put(1,2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            intBuffer.put((int)pos&mask, intBuffer.get((int)(pos-2)&mask) + intBuffer.get((int)(pos-1)&mask));
        }
        return intBuffer.get((int)(pos-2)&mask) + intBuffer.get((int)(pos-1)&mask);
    }
    
    private long intBuffer2Loop() {
        long pos = 1;
        
        byteBufferForInt.putInt(0, 1);
        byteBufferForInt.putInt(4, 2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            int value = 
            byteBufferForInt.getInt((int)(4*(pos-2))&mask)+
            byteBufferForInt.getInt((int)(4*(pos-1))&mask);
            
            byteBufferForInt.putInt((int)(4*pos)&mask, value);
        }
        return byteBufferForInt.getInt((int)(4*(pos-2))&mask)+
                byteBufferForInt.getInt((int)(4*(pos-1))&mask);
    }
    
    private long intArrayBackedLoop() {
        long pos = 1;
        
        ArrayBacked.writeInt(arrayBackedInts, arrayBackedIntsMask, 0, (int)1);
        ArrayBacked.writeInt(arrayBackedInts, arrayBackedIntsMask, 4, (int)1);
                
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            long idx = pos<<2;
            
            ArrayBacked.writeInt(arrayBackedInts, arrayBackedIntsMask, idx, 
                         (ArrayBacked.readInt(arrayBackedInts, arrayBackedIntsMask, idx-8) +
                          ArrayBacked.readInt(arrayBackedInts, arrayBackedIntsMask, idx-4)));
        }
        return ArrayBacked.readInt(arrayBackedInts, arrayBackedIntsMask, (pos<<2)-8) +
                ArrayBacked.readInt(arrayBackedInts, arrayBackedIntsMask, (pos<<2)-4);
    }
    
    private long longArrayLoop() {
        long pos = 1;
        longArray[0] = 1;
        longArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            longArray[(int)pos&mask] = longArray[(int)(pos-2)&mask] + longArray[(int)(pos-1)&mask];
        }
        return longArray[(int)(pos-2)&mask] + longArray[(int)(pos-1)&mask];
    }

    private long longBufferLoop() {
        long pos = 1;
        longBuffer.put(0,1);
        longBuffer.put(1,2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            longBuffer.put((int)pos&mask, longBuffer.get((int)(pos-2)&mask) + longBuffer.get((int)(pos-1)&mask));
        }
        return longBuffer.get((int)(pos-2)&mask) + longBuffer.get((int)(pos-1)&mask);
    }
    
    private long longArrayBackedLoop() {
        long pos = 1;
        
        ArrayBacked.writeLong(arrayBackedLongs, arrayBackedLongsMask, 0, 1);
        ArrayBacked.writeLong(arrayBackedLongs, arrayBackedLongsMask, 8, 1);
                
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            long idx = pos<<3;
            
            ArrayBacked.writeLong(arrayBackedLongs, arrayBackedLongsMask, idx, 
                    (ArrayBacked.readLong(arrayBackedLongs, arrayBackedLongsMask, idx-16) +
                     ArrayBacked.readLong(arrayBackedLongs, arrayBackedLongsMask, idx-8)));
        }
        return ArrayBacked.readLong(arrayBackedLongs, arrayBackedLongsMask, (pos<<3)-16) +
                ArrayBacked.readLong(arrayBackedLongs, arrayBackedLongsMask, (pos<<3)-8);
    }
    
    
    
    
}
