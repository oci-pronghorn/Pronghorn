//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.ociweb.jfast.error.FASTError;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;

/**
 * PrimitiveReader
 * 
 * Must be final and not implement any interface or be abstract. In-lining the
 * methods of this class provides much of the performance needed by
 * this library.
 * 
 * This class also has no member methods, all the methods are static.  This allows 
 * the Java and Julia implementations to have the same design.  It also removes any vtable
 * lookups that may have been required by a more traditional approach.
 * 
 * @author Nathan Tippy
 * 
 */

public final class PrimitiveReader {

    // Note: only the type/opp combos used will get in-lined, this small
    // footprint will fit in execution cache.
    // if we in-line too much the block will be to large and may spill.

    private long totalReader;

    private final FASTInput input;
    private final byte[] buffer;
    
    private byte[] invPmapStack;
    private int invPmapStackDepth;

    private int position;
    private int limit;

    // both bytes but class def likes int much better for alignment
    private byte pmapIdx = -1;
    private byte bitBlock = 0;
    private final int resetLimit;  

    //Needed to be this large to pass unit tests.
    private long nanoBlockingTimeout = 5000000; //maximum wait time for more incoming content in nanoseconds.

    
    /**
     * 
     * Making the bufferSize large will decrease the number of copies but may increase latency.
     * Making the bufferSize small will decrease latency but may reduce overall throughput.
     * 
     * @param bufferSizeInBytes must be large enough to hold a single group
     * @param input
     * @param maxPMapCountInBytes must be large enough to hold deepest possible nesting of pmaps
     */
    public PrimitiveReader(int bufferSizeInBytes, FASTInput input, int maxPMapCountInBytes) { 
        this.input = input;
        this.buffer = new byte[bufferSizeInBytes];
        this.resetLimit = 0;
        this.position = 0;
        this.limit = 0;
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;

        input.init(this.buffer);
    }
    //TODO: C, validate valid template switch over can only happen when PmapStack is empty!
    
    public PrimitiveReader(byte[] buffer) {
        this.input = null; //TODO: C, may want dummy impl for this.
        this.buffer = buffer;
        this.resetLimit = buffer.length;
        
        this.position = 0;
        this.limit = buffer.length;
        //in this case where the full data is provided then we know it can not be larger than the buffer.
        int maxPMapCountInBytes = buffer.length;
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;

    }
    
    public PrimitiveReader(byte[] buffer, int maxPMapCountInBytes) {
        this.input = null; //TODO: C, may want dummy impl for this.
        this.buffer = buffer;
        this.resetLimit = buffer.length;
        
        this.position = 0;
        this.limit = buffer.length;
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;

    }
    
    public static final void reset(PrimitiveReader reader) {
        reader.totalReader = 0;
        reader.position = 0;
        reader.limit = reader.resetLimit;
        reader.pmapIdx = -1;
        reader.invPmapStackDepth = reader.invPmapStack.length - 2;

    }

    public static final void setTimeout(long nanos, PrimitiveReader reader) {
        reader.nanoBlockingTimeout = nanos;
    }
    
    public static final long getTimeout(PrimitiveReader reader) {
        return reader.nanoBlockingTimeout;
    }
    
    public static final long totalRead(PrimitiveReader reader) {
        return reader.totalReader;
    }

    public static final int bytesReadyToParse(PrimitiveReader reader) {
        return reader.limit - reader.position;
    }

    public static final void fetch(PrimitiveReader reader) {
        fetch(0, reader);
    }
    
    // Will not return until the need is met because the parser has
    // determined that we can not continue until this data is provided.
    // this call may however read in more than the need because its ready
    // and convenient to reduce future calls.
    private static void fetch(int need, PrimitiveReader reader) {
        int count = 0;
        need = fetchAvail(need, reader);
        long timeout = 0;
        while (need > 0) { 
            if (0 == count++) {
                timeout = System.nanoTime()+reader.nanoBlockingTimeout;
                
                //TODO: X, compact buffer in prep for data spike

            } else {
                if (System.nanoTime()>timeout) {
                    
                    //TODO: B, must roll back to previous position to decode can try again later on this steam.
                    
                    throw new FASTException(FASTError.TIMEOUT);
                }
            }

            need = fetchAvail(need, reader);
        }

    }

    private static int fetchAvail(int need, PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            reader.position = reader.limit = 0;
        }
        int remainingSpace = reader.buffer.length - reader.limit;
        if (need <= remainingSpace) {
            // fill remaining space if possible to reduce fetch later

            int filled = reader.input.fill(reader.limit, remainingSpace);

            //
            reader.totalReader += filled;
            reader.limit += filled;
            //
            return need - filled;
        } else {
            return noRoomOnFetch(need, reader);
        }
    }

    private static int noRoomOnFetch(int need, PrimitiveReader reader) {
        // not enough room at end of buffer for the need
        int populated = reader.limit - reader.position;
        int reqiredSize = need + populated;

        assert (reader.buffer.length >= reqiredSize) : "internal buffer is not large enough, requres " + reqiredSize
                + " bytes";

        System.arraycopy(reader.buffer, reader.position, reader.buffer, 0, populated);
        // fill and return

        int filled = reader.input.fill(populated, reader.buffer.length - populated);

        reader.position = 0;
        reader.totalReader += filled;
        reader.limit = populated + filled;

        return need - filled;

    }

    public static final void readByteData(byte[] target, int offset, int length, PrimitiveReader reader) {
        // ensure all the bytes are in the buffer before calling visitor
        if (reader.limit - reader.position < length) {
            fetch(length, reader);
        }
        System.arraycopy(reader.buffer, reader.position, target, offset, length);
        reader.position += length;
    }

    // ///////////////
    // pmapStructure
    // 1 2 3 4 5 D ? I 2 3 4 X X
    // 0 0 0 0 1 D ? I 0 0 1 X X
    //
    // D delta to last position
    // I pmapIdx of last stack frame
    // //
    // called at the start of each group unless group knows it has no pmap
    public static final void openPMap(final int pmapMaxSize, PrimitiveReader reader) {
        //TODO: X, pmapMaxSize is a constant for many templates and can be injected.
        
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        // push the old index for resume
        reader.invPmapStack[reader.invPmapStackDepth - 1] = (byte) reader.pmapIdx;

        int k = reader.invPmapStackDepth -= (pmapMaxSize + 2);
        reader.bitBlock = reader.buffer[reader.position];
        k = walkPMapLength(pmapMaxSize, k, reader.invPmapStack, reader);
        reader.invPmapStack[k] = (byte) (3 + pmapMaxSize + (reader.invPmapStackDepth - k));

        // set next bit to read
        reader.pmapIdx = 6;
    }

    private static int walkPMapLength(final int pmapMaxSize, int k, byte[] pmapStack, PrimitiveReader reader) {
        if (reader.limit - reader.position > pmapMaxSize) {
            if ((pmapStack[k++] = reader.buffer[reader.position++]) >= 0) {
                if ((pmapStack[k++] = reader.buffer[reader.position++]) >= 0) {
                    do {
                    } while ((pmapStack[k++] = reader.buffer[reader.position++]) >= 0);
                }
            }
        } else {
            k = openPMapSlow(k,reader);
        }
        return k;
    }

    private static int openPMapSlow(int k, PrimitiveReader reader) {
        // must use slow path because we are near the end of the buffer.
        do {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            // System.err.println("*pmap:"+Integer.toBinaryString(0xFF&buffer[position]));
        } while ((reader.invPmapStack[k++] = reader.buffer[reader.position++]) >= 0);
        return k;
    }

    public static byte popPMapBit(PrimitiveReader reader) {//Invoked 100's of millions of times, must be tight.
        byte pidx = reader.pmapIdx; 
        if (pidx > 0 || (pidx == 0 && reader.bitBlock < 0)) {
            // Frequent, 6 out of every 7 plus the last bit block
            reader.pmapIdx = (byte) (pidx - 1);
            return (byte) (1 & (reader.bitBlock >>> pidx));
        } else {
            return (pidx >= 0 ? popPMapBitLow(reader.bitBlock, reader) : 0); //detect next byte or continue with zeros.
        }
    }

    private static byte popPMapBitLow(byte bb, PrimitiveReader reader) {
        // SOMETIMES one of 7 we need to move up to the next byte
        // System.err.println(invPmapStackDepth);
        reader.pmapIdx = 6;
        reader.bitBlock = reader.invPmapStack[++reader.invPmapStackDepth]; //TODO: X, Set both bytes togheter? may speed up
        return (byte) (1 & bb);
    }

    // called at the end of each group
    public static final void closePMap(PrimitiveReader reader) {
        // assert(bitBlock<0);
        assert (reader.invPmapStack[reader.invPmapStackDepth + 1] >= 0);
        reader.bitBlock = reader.invPmapStack[reader.invPmapStackDepth += (reader.invPmapStack[reader.invPmapStackDepth + 1])];
        reader.pmapIdx = reader.invPmapStack[reader.invPmapStackDepth - 1];

    }

    // ///////////////////////////////////
    // ///////////////////////////////////
    // ///////////////////////////////////

    public static long readLongSigned(PrimitiveReader reader) {//Invoked 100's of millions of times, must be tight.
        if (reader.limit - reader.position <= 10) {
            return readLongSignedSlow(reader);
        }

        long v = reader.buffer[reader.position++];
        long accumulator = ((v & 0x40) == 0) ? 0l : 0xFFFFFFFFFFFFFF80l;

        while (v >= 0) {
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[reader.position++];
        }

        return accumulator | (v & 0x7Fl);
    }

    private static long readLongSignedSlow(PrimitiveReader reader) {
        // slow path
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        int v = reader.buffer[reader.position++];
        long accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFFFFFFFFFF80l;

        while (v >= 0) { // (v & 0x80)==0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static long readLongUnsigned(PrimitiveReader reader) {
        if (reader.position > reader.limit - 10) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            byte v = reader.buffer[reader.position++];
            long accumulator;
            if (v >= 0) { // (v & 0x80)==0) {
                accumulator = v << 7;
            } else {
                return (v & 0x7F);
            }

            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            v = reader.buffer[reader.position++];

            while (v >= 0) { // (v & 0x80)==0) {
                accumulator = (accumulator | v) << 7;

                if (reader.position >= reader.limit) {
                    fetch(1, reader);
                }
                v = reader.buffer[reader.position++];

            }
            return accumulator | (v & 0x7F);
        }
        byte[] buf = reader.buffer;

        byte v = buf[reader.position++];
        long accumulator;
        if (v >= 0) {// (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        v = buf[reader.position++];
        while (v >= 0) {// (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            v = buf[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static int readIntegerSigned(PrimitiveReader reader) {
        if (reader.limit - reader.position <= 5) {
            return readIntegerSignedSlow(reader);
        }
        int p = reader.position;
        byte v = reader.buffer[p++];
        int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[p++];
        }
        reader.position = p;
        return accumulator | (v & 0x7F);
    }

    private static int readIntegerSignedSlow(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        byte v = reader.buffer[reader.position++];
        int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;

        while (v >= 0) { // (v & 0x80)==0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static int readIntegerUnsigned(PrimitiveReader reader) {//Invoked 100's of millions of times, must be tight.
        if (reader.limit - reader.position >= 5) {// not near end so go fast.
            byte v;
            return ((v = reader.buffer[reader.position++]) < 0) ? (v & 0x7F) : readIntegerUnsignedLarger(v, reader);
        } else {
            return readIntegerUnsignedSlow(reader);
        }
    }

    private static int readIntegerUnsignedLarger(byte t, PrimitiveReader reader) {
        byte v = reader.buffer[reader.position++];
        if (v < 0) {
            return (t << 7) | (v & 0x7F);
        } else {
            int accumulator = ((t << 7) | v) << 7;
            while ((v = reader.buffer[reader.position++]) >= 0) {
                accumulator = (accumulator | v) << 7;
            }
            return accumulator | (v & 0x7F);
        }
    }

    private static int readIntegerUnsignedSlow(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        byte v = reader.buffer[reader.position++];
        int accumulator;
        if (v >= 0) { // (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        v = reader.buffer[reader.position++];

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static Appendable readTextASCII(Appendable target, PrimitiveReader reader) {
        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        byte v = reader.buffer[reader.position];

        if (0 == v) {
            v = reader.buffer[reader.position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            reader.position += 2;
        } else {
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.

            while (reader.buffer[reader.position] >= 0) {
                try {
                    target.append((char) (reader.buffer[reader.position]));
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                reader.position++;
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
            }
            try {
                target.append((char) (0x7F & reader.buffer[reader.position]));
            } catch (IOException e) {
                throw new FASTException(e);
            }

            reader.position++;

        }
        return target;
    }

    public static final int readTextASCII(char[] target, int targetOffset, int targetLimit, PrimitiveReader reader) {

        // TODO: Z, speed up textASCII, by add fast copy by fetch of limit, then
        // return error when limit is reached? Do not call fetch on limit we do
        // not know that we need them.

        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        byte v = reader.buffer[reader.position];

        if (0 == v) {
            v = reader.buffer[reader.position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            reader.position += 2;
            return 0; // zero length string
        } else {
            int countDown = targetLimit - targetOffset;
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.
            int idx = targetOffset;
            while (reader.buffer[reader.position] >= 0 && --countDown >= 0) {
                target[idx++] = (char) (reader.buffer[reader.position++]);
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
            }
            if (--countDown >= 0) {
                target[idx++] = (char) (0x7F & reader.buffer[reader.position++]);
                return idx - targetOffset;// length of string
            } else {
                return targetOffset - idx;// neg length of string if hit max
            }
        }
    }

    public static final int readTextASCII2(char[] target, int targetOffset, int targetLimit, PrimitiveReader reader) {

        int countDown = targetLimit - targetOffset;
        if (reader.limit - reader.position >= countDown) {
            // System.err.println("fast");
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.
            int idx = targetOffset;
            while (reader.buffer[reader.position] >= 0 && --countDown >= 0) {
                target[idx++] = (char) (reader.buffer[reader.position++]);
            }
            if (--countDown >= 0) {
                target[idx++] = (char) (0x7F & reader.buffer[reader.position++]);
                return idx - targetOffset;// length of string
            } else {
                return targetOffset - idx;// neg length of string if hit max
            }
        } else {
            return readAsciiText2Slow(target, targetOffset, countDown, reader);
        }
    }

    private static int readAsciiText2Slow(char[] target, int targetOffset, int countDown, PrimitiveReader reader) {
        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        // must use count because the base of position will be in motion.
        // however the position can not be incremented or fetch may drop data.
        int idx = targetOffset;
        while (reader.buffer[reader.position] >= 0 && --countDown >= 0) {
            target[idx++] = (char) (reader.buffer[reader.position++]);
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
        }
        if (--countDown >= 0) {
            target[idx++] = (char) (0x7F & reader.buffer[reader.position++]);
            return idx - targetOffset;// length of string
        } else {
            return targetOffset - idx;// neg length of string if hit max
        }
    }

    // keep calling while byte is >=0
    public static final byte readTextASCIIByte(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader); // CAUTION: may change value of position
        }
        return reader.buffer[reader.position++];
    }

    public static Appendable readTextUTF8(int charCount, Appendable target, PrimitiveReader reader) {

        while (--charCount >= 0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            byte b = reader.buffer[reader.position++];
            if (b >= 0) {
                // code point 7
                try {
                    target.append((char) b);
                } catch (IOException e) {
                    throw new FASTException(e);
                }
            } else {
                decodeUTF8(target, b, reader);
            }
        }
        return target;
    }

    public static final void readSkipByStop(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        while (reader.buffer[reader.position++] >= 0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
        }
    }

    public static final void readSkipByLengthByt(int len, PrimitiveReader reader) {
        if (reader.limit - reader.position < len) {
            fetch(len, reader);
        }
        reader.position += len;
    }

    public static final void readSkipByLengthUTF(int len, PrimitiveReader reader) {
        // len is units of utf-8 chars so we must check the
        // code points for each before fetching and jumping.
        // no validation at all because we are not building a string.
        while (--len >= 0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            byte b = reader.buffer[reader.position++];
            if (b < 0) {
                // longer pattern than 1 byte
                if (0 != (b & 0x20)) {
                    // longer pattern than 2 bytes
                    if (0 != (b & 0x10)) {
                        // longer pattern than 3 bytes
                        if (0 != (b & 0x08)) {
                            // longer pattern than 4 bytes
                            if (0 != (b & 0x04)) {
                                // longer pattern than 5 bytes
                                if (reader.position >= reader.limit) {
                                    fetch(5, reader);
                                }
                                reader.position += 5;
                            } else {
                                if (reader.position >= reader.limit) {
                                    fetch(4, reader);
                                }
                                reader.position += 4;
                            }
                        } else {
                            if (reader.position >= reader.limit) {
                                fetch(3, reader);
                            }
                            reader.position += 3;
                        }
                    } else {
                        if (reader.position >= reader.limit) {
                            fetch(2, reader);
                        }
                        reader.position += 2;
                    }
                } else {
                    if (reader.position >= reader.limit) {
                        fetch(1, reader);
                    }
                    reader.position++;
                }
            }
        }
    }

    public static final void readTextUTF8(char[] target, int offset, int charCount, PrimitiveReader reader) {
        byte b;
        if (reader.limit - reader.position >= charCount << 3) { // if bigger than the text
                                                  // could be then use this
                                                  // shortcut
            // fast
            while (--charCount >= 0) {
                if ((b = reader.buffer[reader.position++]) >= 0) {
                    // code point 7
                    target[offset++] = (char) b;
                } else {
                    decodeUTF8Fast(target, offset++, b, reader);// untested?? why
                }
            }
        } else {
            while (--charCount >= 0) {
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                if ((b = reader.buffer[reader.position++]) >= 0) {
                    // code point 7
                    target[offset++] = (char) b;
                } else {
                    decodeUTF8(target, offset++, b, reader);
                }
            }
        }
    }

    // convert single char that is not the simple case
    private static void decodeUTF8(Appendable target, byte b, PrimitiveReader reader) {
        byte[] source = reader.buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                try {
                    target.append((char) 0xFFFD); // Bad data replacement char
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                ++reader.position;
                return;
            }
            // code point 11
            result = (b & 0x1F);
        } else {
            /*
             * //longer pattern than 1 byte if (0!=(b&0x20)) { //longer pattern
             * than 2 bytes if (0!=(b&0x10)) { //longer pattern than 3 bytes if
             * (0!=(b&0x08)) { //longer pattern than 4 bytes if (0!=(b&0x04)) {
             */

            if (0 != (b & 0x20)) {
                // if (((byte) (0xFF&(b<<3)) )>=0) { //TODO: T, Need UTF8 test
                // and then these would be faster/simpler by factoring out the
                // constant in this comparison.
                // code point 16
                result = (b & 0x0F);
            } else {
                if (0 != (b & 0x10)) {
                    // if (((byte)(0xFF&(b<<4)))>=0) {
                    // code point 21
                    if (true)
                        throw new UnsupportedOperationException("this is not getting tested!");
                    result = (b & 0x07);
                } else {
                    if (((byte) (0xFF & (b << 5))) >= 0) {
                        // code point 26
                        result = (b & 0x03);
                    } else {
                        if (((byte) (0xFF & (b << 6))) >= 0) {
                            // code point 31
                            result = (b & 0x01);
                        } else {
                            // System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
                            // the high bit should never be set
                            try {
                                target.append((char) 0xFFFD); // Bad data
                                                              // replacement
                                                              // char
                            } catch (IOException e) {
                                throw new FASTException(e);
                            }
                            if (reader.limit - reader.position < 5) {
                                fetch(5, reader);
                            }
                            reader.position += 5;
                            return;
                        }

                        if ((source[reader.position] & 0xC0) != 0x80) {
                            try {
                                target.append((char) 0xFFFD); // Bad data
                                                              // replacement
                                                              // char
                            } catch (IOException e) {
                                throw new FASTException(e);
                            }
                            if (reader.limit - reader.position < 5) {
                                fetch(5, reader);
                            }
                            reader.position += 5;
                            return;
                        }
                        if (reader.position >= reader.limit) {
                            fetch(1, reader); // CAUTION: may change value of position
                        }
                        result = (result << 6) | (source[reader.position++] & 0x3F);
                    }
                    if ((source[reader.position] & 0xC0) != 0x80) {
                        try {
                            target.append((char) 0xFFFD); // Bad data
                                                          // replacement char
                        } catch (IOException e) {
                            throw new FASTException(e);
                        }
                        if (reader.limit - reader.position < 4) {
                            fetch(4, reader);
                        }
                        reader.position += 4;
                        return;
                    }
                    if (reader.position >= reader.limit) {
                        fetch(1, reader); // CAUTION: may change value of position
                    }
                    result = (result << 6) | (source[reader.position++] & 0x3F);
                }
                if ((source[reader.position] & 0xC0) != 0x80) {
                    try {
                        target.append((char) 0xFFFD); // Bad data replacement
                                                      // char
                    } catch (IOException e) {
                        throw new FASTException(e);
                    }
                    if (reader.limit - reader.position < 3) {
                        fetch(3, reader);
                    }
                    reader.position += 3;
                    return;
                }
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                result = (result << 6) | (source[reader.position++] & 0x3F);
            }
            if ((source[reader.position] & 0xC0) != 0x80) {
                try {
                    target.append((char) 0xFFFD); // Bad data replacement char
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                if (reader.limit - reader.position < 2) {
                    fetch(2, reader);
                }
                reader.position += 2;
                return;
            }
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            result = (result << 6) | (source[reader.position++] & 0x3F);
        }
        if ((source[reader.position] & 0xC0) != 0x80) {
            try {
                target.append((char) 0xFFFD); // Bad data replacement char
            } catch (IOException e) {
                throw new FASTException(e);
            }
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            reader.position += 1;
            return;
        }
        try {
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            target.append((char) ((result << 6) | (source[reader.position++] & 0x3F)));
        } catch (IOException e) {
            throw new FASTException(e);
        }
    }

    // convert single char that is not the simple case
    private static void decodeUTF8(char[] target, int targetIdx, byte b, PrimitiveReader reader) {

        byte[] source = reader.buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                ++reader.position;
                return;
            }
            // code point 11
            result = (b & 0x1F);
        } else {
            if (((byte) (0xFF & (b << 3))) >= 0) {
                // code point 16
                result = (b & 0x0F);
            } else {
                if (((byte) (0xFF & (b << 4))) >= 0) {
                    // code point 21
                    result = (b & 0x07);
                } else {
                    if (((byte) (0xFF & (b << 5))) >= 0) {
                        // code point 26
                        result = (b & 0x03);
                    } else {
                        if (((byte) (0xFF & (b << 6))) >= 0) {
                            // code point 31
                            result = (b & 0x01);
                        } else {
                            // System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
                            // the high bit should never be set
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            if (reader.limit - reader.position < 5) {
                                fetch(5, reader);
                            }
                            reader.position += 5;
                            return;
                        }

                        if ((source[reader.position] & 0xC0) != 0x80) {
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            if (reader.limit - reader.position < 5) {
                                fetch(5, reader);
                            }
                            reader.position += 5;
                            return;
                        }
                        if (reader.position >= reader.limit) {
                            fetch(1, reader); // CAUTION: may change value of position
                        }
                        result = (result << 6) | (source[reader.position++] & 0x3F);
                    }
                    if ((source[reader.position] & 0xC0) != 0x80) {
                        target[targetIdx] = 0xFFFD; // Bad data replacement char
                        if (reader.limit - reader.position < 4) {
                            fetch(4, reader);
                        }
                        reader.position += 4;
                        return;
                    }
                    if (reader.position >= reader.limit) {
                        fetch(1, reader); // CAUTION: may change value of position
                    }
                    result = (result << 6) | (source[reader.position++] & 0x3F);
                }
                if ((source[reader.position] & 0xC0) != 0x80) {
                    target[targetIdx] = 0xFFFD; // Bad data replacement char
                    if (reader.limit - reader.position < 3) {
                        fetch(3, reader);
                    }
                    reader.position += 3;
                    return;
                }
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                result = (result << 6) | (source[reader.position++] & 0x3F);
            }
            if ((source[reader.position] & 0xC0) != 0x80) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                if (reader.limit - reader.position < 2) {
                    fetch(2, reader);
                }
                reader.position += 2;
                return;
            }
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            result = (result << 6) | (source[reader.position++] & 0x3F);
        }
        if ((source[reader.position] & 0xC0) != 0x80) {
            target[targetIdx] = 0xFFFD; // Bad data replacement char
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
            reader.position += 1;
            return;
        }
        if (reader.position >= reader.limit) {
            fetch(1, reader); // CAUTION: may change value of position
        }
        target[targetIdx] = (char) ((result << 6) | (source[reader.position++] & 0x3F));
    }

    private static void decodeUTF8Fast(char[] target, int targetIdx, byte b, PrimitiveReader reader) {

        byte[] source = reader.buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                ++reader.position;
                return;
            }
            // code point 11
            result = (b & 0x1F);
        } else {
            if (((byte) (0xFF & (b << 3))) >= 0) {
                // code point 16
                result = (b & 0x0F);
            } else {
                if (((byte) (0xFF & (b << 4))) >= 0) {
                    // code point 21
                    result = (b & 0x07);
                } else {
                    if (((byte) (0xFF & (b << 5))) >= 0) {
                        // code point 26
                        result = (b & 0x03);
                    } else {
                        if (((byte) (0xFF & (b << 6))) >= 0) {
                            // code point 31
                            result = (b & 0x01);
                        } else {
                            // System.err.println("odd byte :"+Integer.toBinaryString(b)+" at pos "+(offset-1));
                            // the high bit should never be set
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            reader.position += 5;
                            return;
                        }

                        if ((source[reader.position] & 0xC0) != 0x80) {
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            reader.position += 5;
                            return;
                        }
                        result = (result << 6) | (source[reader.position++] & 0x3F);
                    }
                    if ((source[reader.position] & 0xC0) != 0x80) {
                        target[targetIdx] = 0xFFFD; // Bad data replacement char
                        reader.position += 4;
                        return;
                    }
                    result = (result << 6) | (source[reader.position++] & 0x3F);
                }
                if ((source[reader.position] & 0xC0) != 0x80) {
                    target[targetIdx] = 0xFFFD; // Bad data replacement char
                    reader.position += 3;
                    return;
                }
                result = (result << 6) | (source[reader.position++] & 0x3F);
            }
            if ((source[reader.position] & 0xC0) != 0x80) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                reader.position += 2;
                return;
            }
            result = (result << 6) | (source[reader.position++] & 0x3F);
        }
        if ((source[reader.position] & 0xC0) != 0x80) {
            target[targetIdx] = 0xFFFD; // Bad data replacement char
            reader.position += 1;
            return;
        }
        target[targetIdx] = (char) ((result << 6) | (source[reader.position++] & 0x3F));
    }

    public static final boolean isEOF(PrimitiveReader reader) {
        if (reader.limit != reader.position) {
            return false;
        }
        if (null==reader.input) {
            return true;
        }
        fetch(0, reader);
        return reader.limit != reader.position ? false : reader.input.isEOF();
    }

    // ///////////////////////////////
    // Dictionary specific operations
    // ///////////////////////////////

    public static final int readIntegerUnsignedCopy(int target, int source, int[] dictionary, PrimitiveReader reader) {
        return dictionary[target] = (popPMapBit(reader) == 0 ? dictionary[source] : readIntegerUnsigned(reader));
    }

    public static final int readIntegerUnsignedDefault(int constDefault, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? constDefault : readIntegerUnsigned(reader));
    }

    public static final int readIntegerUnsignedDefaultOptional(int constDefault, int constAbsent, PrimitiveReader reader) {
        int value;
        return (popPMapBit(reader) == 0) ? constDefault
                : (value = readIntegerUnsigned(reader)) == 0 ? constAbsent : value - 1;
    }

    public static final int readIntegerUnsignedIncrement(int target, int source, int[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readIntegerUnsigned(reader)));
    }

    public static final int readIntegerUnsignedIncrementOptional(int target, int source, int[] dictionary, int constAbsent, PrimitiveReader reader) {

        if (popPMapBit(reader) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            int value;
            if ((value = readIntegerUnsigned(reader)) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    public static final int readIntegerSignedCopy(int target, int source, int[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? dictionary[source]
                : (dictionary[target] = readIntegerSigned(reader)));
    }

    public static final int readIntegerSignedDefault(int constDefault, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? constDefault : readIntegerSigned(reader));
    }

    public static final int readIntegerSignedIncrement(int target, int source, int[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readIntegerSigned(reader)));
    }

    

    // For the Long values

    public static final long readLongUnsignedCopy(int target, int source, long[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? dictionary[source]
                : (dictionary[target] = readLongUnsigned(reader)));
    }

    public static final long readLongUnsignedDefault(long constDefault, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? constDefault : readLongUnsigned(reader));
    }

    public static final long readLongUnsignedDefaultOptional(long constDefault, long constAbsent, PrimitiveReader reader) {
        if (popPMapBit(reader) == 0) {
            return constDefault;
        } else {
            long value = readLongUnsigned(reader);
            return value == 0 ? constAbsent : value - 1;
        }
    }

    public static final long readLongUnsignedIncrement(int target, int source, long[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readLongUnsigned(reader)));
    }

    public static final long readLongUnsignedIncrementOptional(int target, int source, long[] dictionary, long constAbsent, PrimitiveReader reader) {

        if (popPMapBit(reader) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            long value;
            if ((value = readLongUnsigned(reader)) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    //TODO: B, can duplicate this to make a more effecient version when source==target
    public static final long readLongSignedCopy(int target, int source, long[] dictionary, PrimitiveReader reader) {
        return dictionary[target] = (popPMapBit(reader) == 0 ? dictionary[source] : readLongSigned(reader));
    }

    public static final long readLongSignedDefaultOptional(long constDefault, long constAbsent, PrimitiveReader reader) {
        if (popPMapBit(reader) == 0) {
            return constDefault;
        } else {
            long value = readLongSigned(reader);
            return value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        }
    }

    public static final long readLongSignedIncrement(int target, int source, long[] dictionary, PrimitiveReader reader) {
        return (popPMapBit(reader) == 0 ? (dictionary[target] = dictionary[source] + 1) : (dictionary[target] = readLongSigned(reader)));
    }

    public static final long readLongSignedIncrementOptional(int target, int source, long[] dictionary, long constAbsent, PrimitiveReader reader) {

        if (popPMapBit(reader) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            long value;
            if ((value = readLongSigned(reader)) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    // //////////////
    // /////////

    public static final int openMessage(int pmapMaxSize, PrimitiveReader reader) {
        openPMap(pmapMaxSize, reader);
        // return template id or unknown
        return (0 != popPMapBit(reader)) ? readIntegerUnsigned(reader) : -1;// template Id

    }

    public static int readRawInt(PrimitiveReader reader) {
        if (reader.limit-reader.position <4) {
            fetch(4, reader);
        }
        
        return (((0xFF & reader.buffer[reader.position++]) << 0) | 
                ((0xFF & reader.buffer[reader.position++]) << 8) |
                ((0xFF & reader.buffer[reader.position++]) << 16) | 
                ((0xFF & reader.buffer[reader.position++]) << 24));

    }

}
