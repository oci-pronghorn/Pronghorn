package com.ociweb.pronghorn.pipe;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

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
        
    private static final short[] shortArray = new short[size];    
    private static final ByteBuffer byteBufferForShort = ByteBuffer.wrap(new byte[size*2]);
    private static final ShortBuffer shortBuffer = byteBufferForShort.asShortBuffer();
    
    private static final int[] intArray = new int[size];    
    private static final ByteBuffer byteBufferForInt = ByteBuffer.wrap(new byte[size*4]);
    private static final IntBuffer intBuffer = byteBufferForInt.asIntBuffer();
    
    private static final long[] longArray = new long[size];    
    private static final ByteBuffer byteBufferForLong = ByteBuffer.wrap(new byte[size*8]);
    private static final LongBuffer longBuffer = byteBufferForLong.asLongBuffer();
     
    /**
     * Test showing that the ByteBuffer wrappers are not slow.
     * 
     * As of my last run on both JDK 7 and 8 the ByteWrappers of specific type outperformed
     * the array of the same primitive type.  The gap between the two grows with 
     * the primitive data size.
     * 
     */
    
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
        
        System.out.println("ByteArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("ByteBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
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
        
        System.out.println("ShortArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("ShortBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
    }
    
    private static void runIntTest(ArrayAccessSpeedTest instance) {
        System.out.println("running array test");
        long arrayStart = System.currentTimeMillis();
        long arrayResult = instance.intArrayLoop();
        long arrayDuration = System.currentTimeMillis()-arrayStart;
        
        System.out.println("running int buffer test");
        long byteBufferStart = System.currentTimeMillis();
        long byteBufferResult = instance.intBufferLoop();
        long byteBufferDuration = System.currentTimeMillis()-byteBufferStart;
        
        System.out.println("IntArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("IntBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
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
        
        System.out.println("LongArray Time:  "+arrayDuration+"  Value: "+arrayResult);
        System.out.println("LongBuffer Time: "+byteBufferDuration+" Value: "+byteBufferResult);
    }
   
    private long byteBufferLoop() {
        long pos = 1;
        byteArray[0] = 1;
        byteArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            byteArray[(int)pos&mask] = (byte)(byteArray[(int)(pos-2)&mask] + byteArray[(int)(pos-1)&mask]);
        }
        return byteArray[(int)(pos-2)&mask] + byteArray[(int)(pos-1)&mask];
    }

    private long byteArrayLoop() {
        long pos = 1;
        byteBuffer.put(0,(byte)1);
        byteBuffer.put(1,(byte)2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            byteBuffer.put((int)pos&mask, (byte) (byteBuffer.get((int)(pos-2)&mask) + byteBuffer.get((int)(pos-1)&mask)));
        }
        return byteBuffer.get((int)(pos-2)&mask) + byteBuffer.get((int)(pos-1)&mask);
    }
    
    private long shortBufferLoop() {
        long pos = 1;
        shortArray[0] = 1;
        shortArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            shortArray[(int)pos&mask] = (short)(shortArray[(int)(pos-2)&mask] + shortArray[(int)(pos-1)&mask]);
        }
        return shortArray[(int)(pos-2)&mask] + shortArray[(int)(pos-1)&mask];
    }

    private long shortArrayLoop() {
        long pos = 1;
        shortBuffer.put(0,(short)1);
        shortBuffer.put(1,(short)2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            shortBuffer.put((int)pos&mask, (short) (shortBuffer.get((int)(pos-2)&mask) + shortBuffer.get((int)(pos-1)&mask)));
        }
        return shortBuffer.get((int)(pos-2)&mask) + shortBuffer.get((int)(pos-1)&mask);
    }
    
    
    private long intBufferLoop() {
        long pos = 1;
        intArray[0] = 1;
        intArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            intArray[(int)pos&mask] = intArray[(int)(pos-2)&mask] + intArray[(int)(pos-1)&mask];
        }
        return intArray[(int)(pos-2)&mask] + intArray[(int)(pos-1)&mask];
    }

    private long intArrayLoop() {
        long pos = 1;
        intBuffer.put(0,1);
        intBuffer.put(1,2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            intBuffer.put((int)pos&mask, intBuffer.get((int)(pos-2)&mask) + intBuffer.get((int)(pos-1)&mask));
        }
        return intBuffer.get((int)(pos-2)&mask) + intBuffer.get((int)(pos-1)&mask);
    }
    
    private long longBufferLoop() {
        long pos = 1;
        longArray[0] = 1;
        longArray[1] = 2;
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            longArray[(int)pos&mask] = longArray[(int)(pos-2)&mask] + longArray[(int)(pos-1)&mask];
        }
        return longArray[(int)(pos-2)&mask] + longArray[(int)(pos-1)&mask];
    }

    private long longArrayLoop() {
        long pos = 1;
        longBuffer.put(0,1);
        longBuffer.put(1,2);
        
        long limit = ((long)size) * (long)iterations;
        while (++pos<limit) {
            longBuffer.put((int)pos&mask, longBuffer.get((int)(pos-2)&mask) + longBuffer.get((int)(pos-1)&mask));
        }
        return longBuffer.get((int)(pos-2)&mask) + longBuffer.get((int)(pos-1)&mask);
    }
    
    
    
    
}
