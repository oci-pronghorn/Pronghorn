package com.ociweb.jfast.stream;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ociweb.jfast.error.FASTException;

/**
 * Public interface for applications desiring to consume data from a FAST feed.
 * @author Nathan Tippy
 *
 */
public class FASTRingBufferReader {//TODO: B, build another static reader that does auto convert to the requested type.

    static double[] powd = new double[] {1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
        1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
        1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
        1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
        1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};
    
    static float[] powf = new float[] {Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,
        1.0E-45f,1.0E-44f,1.0E-43f,1.0E-42f,1.0E-41f,1.0E-40f,1.0E-39f,1.0E-38f,1.0E-37f,1.0E-36f,1.0E-35f,1.0E-34f,1.0E-33f,1.0E-32f,1.0E-31f,1.0E-30f,1.0E-29f,1.0E-28f,1.0E-27f,1.0E-26f,1.0E-25f,1.0E-24f,1.0E-23f,1.0E-22f,
        1.0E-21f,1.0E-20f,1.0E-19f,1.0E-18f,1.0E-17f,1.0E-16f,1.0E-15f,1.0E-14f,1.0E-13f,1.0E-12f,1.0E-11f,1.0E-10f,1.0E-9f,1.0E-8f,1.0E-7f,1.0E-6f,1.0E-5f,1.0E-4f,0.001f,0.01f,0.1f,1.0f,10.0f,100.0f,1000.0f,10000.0f,100000.0f,1000000.0f,
        1.0E7f,1.0E8f,1.0E9f,1.0E10f,1.0E11f,1.0E12f,1.0E13f,1.0E14f,1.0E15f,1.0E16f,1.0E17f,1.0E18f,1.0E19f,1.0E20f,1.0E21f,1.0E22f,1.0E23f,1.0E24f,1.0E25f,1.0E26f,1.0E27f,1.0E28f,1.0E29f,1.0E30f,1.0E31f,1.0E32f,1.0E33f,1.0E34f,1.0E35f,
        1.0E36f,1.0E37f,1.0E38f,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN};
    
    public static int readInt(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
    }

    public static long readLong(FASTRingBuffer ring, int idx) {
        long i = ring.remPos.value + idx;
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }

    public static double readDouble(FASTRingBuffer ring, int idx) {
        return ((double)readDecimalMantissa(ring,idx))*powd[64+readDecimalExponent(ring,idx)];
    }

    public static float readFloat(FASTRingBuffer ring, int idx) {
        return ((float)readDecimalMantissa(ring,idx))*powf[64*readDecimalExponent(ring,idx)];
    }
    
    public static int readDecimalExponent(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
    }
    
    public static long readDecimalMantissa(FASTRingBuffer ring, int idx) {
        long i = ring.remPos.value + idx + 1; //plus one to skip over exponent
        return (((long) ring.buffer[ring.mask & (int)i]) << 32) | (((long) ring.buffer[ring.mask & (int)(i + 1)]) & 0xFFFFFFFFl);
    }
    

    public static int readTextLength(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.remPos.value + idx + 1)];// second int is always the length
    }

    public static Appendable readText(FASTRingBuffer ring, int idx, Appendable target) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readTextLength(ring, idx);
     //   System.err.println("** read text pos:"+(ring.mask & (ring.remPos + idx))+" pos "+ (0x7FFFFFFF&pos) +" len "+len);
        if (pos < 0) {
            return readTextConst(ring,len,target,0x7FFFFFFF & pos);
        } else {
            return readTextRing(ring,len,target,pos);
        }
    }
    
    private static Appendable readTextConst(FASTRingBuffer ring, int len, Appendable target, int pos) {
        try {
            char[] buffer = ring.constTextBuffer;
            while (--len >= 0) {
                target.append(buffer[pos++]);
            }
        } catch (IOException e) {
           throw new FASTException(e);
        }
        return target;
    }

    private static Appendable readTextRing(FASTRingBuffer ring, int len, Appendable target, int pos) {
        try {
            char[] buffer = ring.charBuffer;
            int mask = ring.charMask;
            while (--len >= 0) {
                target.append(buffer[mask & pos++]);
            }
        } catch (IOException e) {
           throw new FASTException(e);
        }
        return target;
    }
    
    public static void readText(FASTRingBuffer ring, int idx, char[] target, int targetOffset) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readTextLength(ring, idx);
        if (pos < 0) {
            readTextConst(ring,len,target, targetOffset,0x7FFFFFFF & pos);
        } else {
            readTextRing(ring,len,target, targetOffset,pos);
        }
    }
    
    private static void readTextConst(FASTRingBuffer ring, int len, char[] target, int targetIdx, int pos) {
            char[] buffer = ring.constTextBuffer;
            while (--len >= 0) {
                target[targetIdx++]=buffer[pos++];
            };
    }

    private static void readTextRing(FASTRingBuffer ring, int len, char[] target, int targetIdx, int pos) {
            char[] buffer = ring.charBuffer;
            int mask = ring.charMask;
            while (--len >= 0) {
                target[targetIdx]=buffer[mask & pos++];
            }
    }
    
    public static void readText(FASTRingBuffer ring, int idx, char[] target, int targetOffset, int targetMask) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readTextLength(ring, idx);
        if (pos < 0) {
            readTextConst(ring,len,target, targetOffset,targetMask, 0x7FFFFFFF & pos);
        } else {
            readTextRing(ring,len,target, targetOffset,targetMask, pos);
        }
    }
    
    private static void readTextConst(FASTRingBuffer ring, int len, char[] target, int targetIdx, int targetMask, int pos) {
            char[] buffer = ring.constTextBuffer;
            while (--len >= 0) {
                target[targetMask & targetIdx++]=buffer[pos++];
            };
    }

    private static void readTextRing(FASTRingBuffer ring, int len, char[] target, int targetIdx, int targetMask, int pos) {
            char[] buffer = ring.charBuffer;
            int mask = ring.charMask;
            while (--len >= 0) {
                target[targetMask & targetIdx]=buffer[mask & pos++];
            }
    }
    
    
    public static boolean eqText(FASTRingBuffer ring, int idx, CharSequence seq) {
        int len = FASTRingBufferReader.readTextLength(ring, idx);
        if (len!=seq.length()) {
            return false;
        }
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        if (pos < 0) {
            return eqTextConst(ring,len,seq,0x7FFFFFFF & pos);
        } else {
            return eqTextRing(ring,len,seq,pos);
        }
    }
    
    private static boolean eqTextConst(FASTRingBuffer ring, int len, CharSequence seq, int pos) {
            char[] buffer = ring.constTextBuffer;
            int i = 0;
            while (--len >= 0) {
                if (seq.charAt(i++)!=buffer[pos++]) {
                    return false;
                }
            }
            return true;
    }

    private static boolean eqTextRing(FASTRingBuffer ring, int len, CharSequence seq, int pos) {
            char[] buffer = ring.charBuffer;
            int mask = ring.charMask;
            int i = 0;
            while (--len >= 0) {
                if (seq.charAt(i++)!=buffer[mask & pos++]) {
                    System.err.println("text match failure on:"+seq.charAt(i-1)+" pos "+pos+" mask "+mask);
                    return false;
                }
            }
            return true;
    }
    
    //Bytes
    
    public static int readBytesLength(FASTRingBuffer ring, int idx) {
        return ring.buffer[ring.mask & (int)(ring.remPos.value + idx + 1)];// second int is always the length
    }

    public static ByteBuffer readBytes(FASTRingBuffer ring, int idx, ByteBuffer target) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            return readBytesConst(ring,len,target,0x7FFFFFFF & pos);
        } else {
            return readBytesRing(ring,len,target,pos);
        }
    }
    
    private static ByteBuffer readBytesConst(FASTRingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.constByteBuffer;
        while (--len >= 0) {
            target.put(buffer[pos++]);
        }
        return target;
    }

    private static ByteBuffer readBytesRing(FASTRingBuffer ring, int len, ByteBuffer target, int pos) {
        byte[] buffer = ring.byteBuffer;
        int mask = ring.charMask;
        while (--len >= 0) {
            target.put(buffer[mask & pos++]);
        }
        return target;
    }
    
    public static void readBytes(FASTRingBuffer ring, int idx, byte[] target, int targetOffset) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,0x7FFFFFFF & pos);
        } else {
            readBytesRing(ring,len,target, targetOffset,pos);
        }
    }
    
    private static void readBytesConst(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetIdx++]=buffer[pos++];
            };
    }

    private static void readBytesRing(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.charMask;
            while (--len >= 0) {
                target[targetIdx]=buffer[mask & pos++];
            }
    }
    
    public static void readBytes(FASTRingBuffer ring, int idx, byte[] target, int targetOffset, int targetMask) {
        int pos = ring.buffer[ring.mask & (int)(ring.remPos.value + idx)];
        int len = FASTRingBufferReader.readBytesLength(ring, idx);
        if (pos < 0) {
            readBytesConst(ring,len,target, targetOffset,targetMask, 0x7FFFFFFF & pos);
        } else {
            readBytesRing(ring,len,target, targetOffset,targetMask, pos);
        }
    }
    
    private static void readBytesConst(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int targetMask, int pos) {
            byte[] buffer = ring.constByteBuffer;
            while (--len >= 0) {
                target[targetMask & targetIdx++]=buffer[pos++];
            };
    }

    private static void readBytesRing(FASTRingBuffer ring, int len, byte[] target, int targetIdx, int targetMask, int pos) {
            byte[] buffer = ring.byteBuffer;
            int mask = ring.charMask;
            while (--len >= 0) {
                target[targetMask & targetIdx]=buffer[mask & pos++];
            }
    }

    public static void dump(FASTRingBuffer queue) {
        //dump everything up to where it it is still writing new fragments.
        queue.removeCount.lazySet(queue.remPos.value = queue.addCount.get());
        
    }

    public static void nextMessage(FASTRingBuffer rb) {
        // TODO Auto-generated method stub
        
    }

    public static void nextFragment(FASTRingBuffer rb) {
        // TODO Auto-generated method stub
        
    }

}
