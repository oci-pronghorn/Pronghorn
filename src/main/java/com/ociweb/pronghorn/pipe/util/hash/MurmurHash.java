package com.ociweb.pronghorn.pipe.util.hash;

import java.nio.ByteBuffer;

public class MurmurHash {
    // Based on Murmurhash 2.0 Java port at http://dmy999.com/article/50/murmurhash-2-java-port
    // 2011-12-05: Modified by Hiroshi Nakamura <nahi@ruby-lang.org>
    // - signature change to use offset
    //   hash(byte[] data, int seed) to hash(byte[] src, int offset, int length, int seed)
    // - extract 'm' and 'r' as murmurhash2.0 constants

    // Ported by Derek Young from the C version (specifically the endian-neutral
    // version) from:
    //   http://murmurhash.googlepages.com/
    //
    // released to the public domain - dmy999@gmail.com

    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    private static final int MURMUR2_MAGIC = 0x5bd1e995;
    // CRuby 1.9 uses 16 but original C++ implementation uses 24 with above Magic.
    private static final int MURMUR2_R = 24;

    @SuppressWarnings("fallthrough")
    public static int hash32(byte[] src, int offset, int length, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = src[i + 0] & 0xFF;
            k |= (src[i + 1] & 0xFF) << 8;
            k |= (src[i + 2] & 0xFF) << 16;
            k |= (src[i + 3] & 0xFF) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
            case 3:
                h ^= (src[i + 2] & 0xFF) << 16;
            case 2:
                h ^= (src[i + 1] & 0xFF) << 8;
            case 1:
                h ^= (src[i + 0] & 0xFF);
                h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    public static int hash32(byte[] byteInput, int byteOffset, int byteLength, int byteMask, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ byteLength;

        int i = byteOffset;
        int len = byteLength;
        while (len >= 4) {
            int k = byteInput[byteMask & (i + 0)] & 0xFF;
            k |= (byteInput[byteMask & (i + 1)] & 0xFF) << 8;
            k |= (byteInput[byteMask & (i + 2)] & 0xFF) << 16;
            k |= (byteInput[byteMask & (i + 3)] & 0xFF) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
            case 3:
                h ^= (byteInput[byteMask & (i + 2)] & 0xFF) << 16;
            case 2:
                h ^= (byteInput[byteMask & (i + 1)] & 0xFF) << 8;
            case 1:
                h ^= (byteInput[byteMask & (i + 0)] & 0xFF);
                h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    private static int getByte(long[] array, int byteIdx) {
        return 0xFF & (int)(array[byteIdx>>3] >> ((byteIdx&0x7)<<3));
    }
    private static int getByte(int[] array, int byteIdx) {
        return 0xFF & (int)(array[byteIdx>>2] >> ((byteIdx&0x3)<<3));
    }
    private static int getByte(short[] array, int byteIdx) {
        return 0xFF & (int)(array[byteIdx>>1] >> ((byteIdx&0x1)<<3));
    }
    private static int getByte(CharSequence csq, int byteIdx) {
        return 0xFF & (int)(csq.charAt(byteIdx>>1) >> ((byteIdx&0x1)<<3));
    }
    
    
    public static int hash32(long[] inputArray, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*8;
        int length = inputLength*8;
                
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(inputArray,i + 0);
            k |= (getByte(inputArray,i + 1)) << 8;
            k |= (getByte(inputArray,i + 2)) << 16;
            k |= (getByte(inputArray,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    public static int hash32(int[] inputArray, int inputOffset, int inputLength, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {
                        
            int k = inputArray[i++];
            
            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    public static int hash32(int[] inputArray, int inputOffset, int inputLength, int inputMask, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {
                        
            int k = inputArray[inputMask & i++];
            
            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }  
    
    public static int hash32(int k, int j, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (2*4);

            //first
            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            //second
            j *= MURMUR2_MAGIC;
            j ^= j >>> MURMUR2_R;
            j *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= j;
            

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }  
    
    
    public static <C extends CharSequence> int hash32(C[] inputArray, int seed) {
        return hash32(inputArray, 0, inputArray.length, seed);
    }
    
    public static <C extends CharSequence> int hash32(C[] inputArray, int inputOffset, int inputLength, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {
                        
            int k = hash32(inputArray[i++], seed);
                        
            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    } 
    
    
    public static int hash32(short[] inputArray, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*2;
        int length = inputLength*2;
                
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(inputArray,i + 0);
            k |= (getByte(inputArray,i + 1)) << 8;
            k |= (getByte(inputArray,i + 2)) << 16;
            k |= (getByte(inputArray,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (getByte(inputArray,i + 2)) << 16;
        case 2:
            h ^= (getByte(inputArray,i + 1)) << 8;
        case 1:
            h ^= (getByte(inputArray,i + 0));
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    public static int hash32(CharSequence charSequence, int seed) {
        return null==charSequence? seed : hash32(charSequence, 0, charSequence.length(), seed);
    }
    
    public static int hash32(CharSequence charSequence, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*2;
        int length = inputLength*2;
                
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(charSequence,i + 0);
            k |= (getByte(charSequence,i + 1)) << 8;
            k |= (getByte(charSequence,i + 2)) << 16;
            k |= (getByte(charSequence,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (getByte(charSequence,i + 2)) << 16;
        case 2:
            h ^= (getByte(charSequence,i + 1)) << 8;
        case 1:
            h ^= (getByte(charSequence,i + 0));
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    
    //ByteBuffer
    @SuppressWarnings("fallthrough")
    public static int hash32(ByteBuffer src, int offset, int length, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = src.get(i + 0) & 0xFF;
            k |= (src.get(i + 1) & 0xFF) << 8;
            k |= (src.get(i + 2) & 0xFF) << 16;
            k |= (src.get(i + 3) & 0xFF) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (src.get(i + 2) & 0xFF) << 16;
        case 2:
            h ^= (src.get(i + 1) & 0xFF) << 8;
        case 1:
            h ^= (src.get(i + 0) & 0xFF);
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }
    
    public static int hash64finalizer(long value) {
    	return hash32finalizer( ((int)(value>>32)) | (int)value );
    }
    
    
    public static int hash32finalizer(int value)
    {
    	value ^= value >> 16;
        value *= 0x85ebca6b;
        value ^= value >> 13;
        value *= 0xc2b2ae35;
        value ^= value >> 16;
        return value;
    }
    
    
}
