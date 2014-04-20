//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.io.IOException;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;

/**
 * PrimitiveReader
 * 
 * Must be final and not implement any interface or be abstract. In-lining the
 * primitive methods of this class provides much of the performance needed by
 * this library.
 * 
 * 
 * @author Nathan Tippy
 * 
 */

public final class PrimitiveReader {

    // Note: only the type/opp combos used will get in-lined, this small
    // footprint will fit in execution cache.
    // if we in-line too much the block will be to large and may spill.

    private final FASTInput input;
    private long totalReader;
    private final byte[] buffer;

    private final byte[] invPmapStack;
    private int invPmapStackDepth;

    private int position;
    private int limit;

    // both bytes but class def likes int much better for alignment
    private byte pmapIdx = -1;
    private byte bitBlock = 0;

    public final void reset() {
        totalReader = 0;
        position = 0;
        limit = 0;
        pmapIdx = -1;
        invPmapStackDepth = invPmapStack.length - 2;

    }

    public PrimitiveReader(FASTInput input) {
        this(2048, input, 32);
    }

    public PrimitiveReader(int initBufferSize, FASTInput input, int maxPMapCount) {
        this.input = input;
        this.buffer = new byte[initBufferSize];

        this.position = 0;
        this.limit = 0;
        this.invPmapStack = new byte[maxPMapCount];
        this.invPmapStackDepth = invPmapStack.length - 2;

        input.init(this.buffer);
    }

    public final long totalRead() {
        return totalReader;
    }

    public final int bytesReadyToParse() {
        return limit - position;
    }

    public final void fetch() {
        fetch(0);
    }

    // Will not return until the need is met because the parser has
    // determined that we can not continue until this data is provided.
    // this call may however read in more than the need because its ready
    // and convenient to reduce future calls.
    private void fetch(int need) {
        int count = 0;
        need = fetchAvail(need);
        while (need > 0) { // TODO: C, if orignial need was zero should also
                           // compact?
            if (0 == count++) {

                // compact and prep for data spike

            } else {
                if (count < 10) {
                    Thread.yield();
                    // TODO: C, if we are in the middle of parsing a field this
                    // becomes a blocking read and requires a timeout and throw.

                } else {
                    try {
                        Thread.sleep(0, 100);
                    } catch (InterruptedException e) {
                    }
                }
            }

            need = fetchAvail(need);
        }

    }

    private int fetchAvail(int need) {
        if (position >= limit) {
            position = limit = 0;
        }
        int remainingSpace = buffer.length - limit;
        if (need <= remainingSpace) {
            // fill remaining space if possible to reduce fetch later

            int filled = input.fill(limit, remainingSpace);

            //
            totalReader += filled;
            limit += filled;
            //
            return need - filled;
        } else {
            return noRoomOnFetch(need);
        }
    }

    private int noRoomOnFetch(int need) {
        // not enough room at end of buffer for the need
        int populated = limit - position;
        int reqiredSize = need + populated;

        assert (buffer.length >= reqiredSize) : "internal buffer is not large enough, requres " + reqiredSize
                + " bytes";

        System.arraycopy(buffer, position, buffer, 0, populated);
        // fill and return

        int filled = input.fill(populated, buffer.length - populated);

        position = 0;
        totalReader += filled;
        limit = populated + filled;

        return need - filled;

    }

    public final void readByteData(byte[] target, int offset, int length) {
        // ensure all the bytes are in the buffer before calling visitor
        if (limit - position < length) {
            fetch(length);
        }
        System.arraycopy(buffer, position, target, offset, length);
        position += length;
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
    public final void openPMap(final int pmapMaxSize) {
        if (position >= limit) {
            fetch(1);
        }
        // push the old index for resume
        invPmapStack[invPmapStackDepth - 1] = (byte) pmapIdx;

        int k = invPmapStackDepth -= (pmapMaxSize + 2);
        bitBlock = buffer[position];
        k = walkPMapLength(pmapMaxSize, k);
        invPmapStack[k] = (byte) (3 + pmapMaxSize + (invPmapStackDepth - k));

        // set next bit to read
        pmapIdx = 6;
    }

    private int walkPMapLength(final int pmapMaxSize, int k) {
        if (limit - position > pmapMaxSize) {
            do {
            } while ((invPmapStack[k++] = buffer[position++]) >= 0);
        } else {
            k = openPMapSlow(k);
        }
        return k;
    }

    private int openPMapSlow(int k) {
        // must use slow path because we are near the end of the buffer.
        do {
            if (position >= limit) {
                fetch(1);
            }
            // System.err.println("*pmap:"+Integer.toBinaryString(0xFF&buffer[position]));
        } while ((invPmapStack[k++] = buffer[position++]) >= 0);
        return k;
    }

    // called at every field to determine operation
    public final byte popPMapBit() {
        return popPMapBit(pmapIdx, bitBlock);
    }

    private byte popPMapBit(byte tmp, byte bb) {
        if (tmp > 0 || (tmp == 0 && bb < 0)) {
            // Frequent, 6 out of every 7 plus the last bit block
            pmapIdx = (byte) (tmp - 1); // TODO: Z, What if caller keeps the
                                        // pmapIdx instead of keeping state
                                        // here?
            return (byte) (1 & (bb >>> tmp));
        } else {
            return (tmp >= 0 ? popPMapBitLow(tmp, bb) : 0);
        }
    }

    private byte popPMapBitLow(byte tmp, byte bb) {
        // SOMETIMES one of 7 we need to move up to the next byte
        // System.err.println(invPmapStackDepth);
        // The order of these lines should not be changed without profile
        pmapIdx = 6;
        bb = (byte) (1 & bb);
        bitBlock = invPmapStack[++invPmapStackDepth];
        return bb;
    }

    // called at the end of each group
    public final void closePMap() {
        // assert(bitBlock<0);
        assert (invPmapStack[invPmapStackDepth + 1] >= 0);
        bitBlock = invPmapStack[invPmapStackDepth += (invPmapStack[invPmapStackDepth + 1])];
        pmapIdx = invPmapStack[invPmapStackDepth - 1];

    }

    // ///////////////////////////////////
    // ///////////////////////////////////
    // ///////////////////////////////////

    public final long readLongSigned() {
        return readLongSignedPrivate();
    }

    private long readLongSignedPrivate() {
        if (limit - position <= 10) {
            return readLongSignedSlow();
        }

        long v = buffer[position++];
        long accumulator = ((v & 0x40) == 0) ? 0l : 0xFFFFFFFFFFFFFF80l;

        while (v >= 0) {
            accumulator = (accumulator | v) << 7;
            v = buffer[position++];
        }

        return accumulator | (v & 0x7Fl);
    }

    private long readLongSignedSlow() {
        // slow path
        if (position >= limit) {
            fetch(1);
        }
        int v = buffer[position++];
        long accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFFFFFFFFFF80l;

        while (v >= 0) { // (v & 0x80)==0) {
            if (position >= limit) {
                fetch(1);
            }
            accumulator = (accumulator | v) << 7;
            v = buffer[position++];
        }
        return accumulator | (v & 0x7F);
    }

    public final long readLongUnsigned() {
        return readLongUnsignedPrivate();
    }

    private long readLongUnsignedPrivate() {
        if (position > limit - 10) {
            if (position >= limit) {
                fetch(1);
            }
            byte v = buffer[position++];
            long accumulator;
            if (v >= 0) { // (v & 0x80)==0) {
                accumulator = v << 7;
            } else {
                return (v & 0x7F);
            }

            if (position >= limit) {
                fetch(1);
            }
            v = buffer[position++];

            while (v >= 0) { // (v & 0x80)==0) {
                accumulator = (accumulator | v) << 7;

                if (position >= limit) {
                    fetch(1);
                }
                v = buffer[position++];

            }
            return accumulator | (v & 0x7F);
        }
        byte[] buf = buffer;

        byte v = buf[position++];
        long accumulator;
        if (v >= 0) {// (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        v = buf[position++];
        while (v >= 0) {// (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            v = buf[position++];
        }
        return accumulator | (v & 0x7F);
    }

    public final int readIntegerSigned() {
        return readIntegerSignedPrivate();
    }

    private int readIntegerSignedPrivate() {
        if (limit - position <= 5) {
            return readIntegerSignedSlow();
        }
        int p = position;
        byte v = buffer[p++];
        int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            v = buffer[p++];
        }
        position = p;
        return accumulator | (v & 0x7F);
    }

    private int readIntegerSignedSlow() {
        if (position >= limit) {
            fetch(1);
        }
        byte v = buffer[position++];
        int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;

        while (v >= 0) { // (v & 0x80)==0) {
            if (position >= limit) {
                fetch(1);
            }
            accumulator = (accumulator | v) << 7;
            v = buffer[position++];
        }
        return accumulator | (v & 0x7F);
    }

    public final int readIntegerUnsigned() {

        return readIntegerUnsignedPrivate();
    }

    private int readIntegerUnsignedPrivate() {
        if (limit - position >= 5) {// not near end so go fast.
            byte v;
            return ((v = buffer[position++]) < 0) ? (v & 0x7F) : readIntegerUnsignedLarger(v);
        } else {
            return readIntegerUnsignedSlow();
        }
    }

    private int readIntegerUnsignedLarger(byte t) {
        byte v = buffer[position++];
        if (v < 0) {
            return (t << 7) | (v & 0x7F);
        } else {
            int accumulator = ((t << 7) | v) << 7;
            while ((v = buffer[position++]) >= 0) {
                accumulator = (accumulator | v) << 7;
            }
            return accumulator | (v & 0x7F);
        }
    }

    private int readIntegerUnsignedSlow() {
        if (position >= limit) {
            fetch(1);
        }
        byte v = buffer[position++];
        int accumulator;
        if (v >= 0) { // (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        if (position >= limit) {
            fetch(1);
        }
        v = buffer[position++];

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            if (position >= limit) {
                fetch(1);
            }
            v = buffer[position++];
        }
        return accumulator | (v & 0x7F);
    }

    public Appendable readTextASCII(Appendable target) {
        if (limit - position < 2) {
            fetch(2);
        }

        byte v = buffer[position];

        if (0 == v) {
            v = buffer[position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            position += 2;
        } else {
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.

            while (buffer[position] >= 0) {
                try {
                    target.append((char) (buffer[position]));
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                position++;
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
            }
            try {
                target.append((char) (0x7F & buffer[position]));
            } catch (IOException e) {
                throw new FASTException(e);
            }

            position++;

        }
        return target;
    }

    public final int readTextASCII(char[] target, int targetOffset, int targetLimit) {

        // TODO: Z, speed up textASCII, by add fast copy by fetch of limit, then
        // return error when limit is reached? Do not call fetch on limit we do
        // not know that we need them.

        if (limit - position < 2) {
            fetch(2);
        }

        byte v = buffer[position];

        if (0 == v) {
            v = buffer[position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            position += 2;
            return 0; // zero length string
        } else {
            int countDown = targetLimit - targetOffset;
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.
            int idx = targetOffset;
            while (buffer[position] >= 0 && --countDown >= 0) {
                target[idx++] = (char) (buffer[position++]);
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
            }
            if (--countDown >= 0) {
                target[idx++] = (char) (0x7F & buffer[position++]);
                return idx - targetOffset;// length of string
            } else {
                return targetOffset - idx;// neg length of string if hit max
            }
        }
    }

    public final int readTextASCII2(char[] target, int targetOffset, int targetLimit) {

        int countDown = targetLimit - targetOffset;
        if (limit - position >= countDown) {
            // System.err.println("fast");
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.
            int idx = targetOffset;
            while (buffer[position] >= 0 && --countDown >= 0) {
                target[idx++] = (char) (buffer[position++]);
            }
            if (--countDown >= 0) {
                target[idx++] = (char) (0x7F & buffer[position++]);
                return idx - targetOffset;// length of string
            } else {
                return targetOffset - idx;// neg length of string if hit max
            }
        } else {
            return readAsciiText2Slow(target, targetOffset, countDown);
        }
    }

    private int readAsciiText2Slow(char[] target, int targetOffset, int countDown) {
        if (limit - position < 2) {
            fetch(2);
        }

        // must use count because the base of position will be in motion.
        // however the position can not be incremented or fetch may drop data.
        int idx = targetOffset;
        while (buffer[position] >= 0 && --countDown >= 0) {
            target[idx++] = (char) (buffer[position++]);
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
        }
        if (--countDown >= 0) {
            target[idx++] = (char) (0x7F & buffer[position++]);
            return idx - targetOffset;// length of string
        } else {
            return targetOffset - idx;// neg length of string if hit max
        }
    }

    // keep calling while byte is >=0
    public byte readTextASCIIByte() {
        if (position >= limit) {
            fetch(1); // CAUTION: may change value of position
        }
        return buffer[position++];
    }

    public Appendable readTextUTF8(int charCount, Appendable target) {

        while (--charCount >= 0) {
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            byte b = buffer[position++];
            if (b >= 0) {
                // code point 7
                try {
                    target.append((char) b);
                } catch (IOException e) {
                    throw new FASTException(e);
                }
            } else {
                decodeUTF8(target, b);
            }
        }
        return target;
    }

    public final void readSkipByStop() {
        if (position >= limit) {
            fetch(1);
        }
        while (buffer[position++] >= 0) {
            if (position >= limit) {
                fetch(1);
            }
        }
    }

    public final void readSkipByLengthByt(int len) {
        if (limit - position < len) {
            fetch(len);
        }
        position += len;
    }

    public final void readSkipByLengthUTF(int len) {
        // len is units of utf-8 chars so we must check the
        // code points for each before fetching and jumping.
        // no validation at all because we are not building a string.
        while (--len >= 0) {
            if (position >= limit) {
                fetch(1);
            }
            byte b = buffer[position++];
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
                                if (position >= limit) {
                                    fetch(5);
                                }
                                position += 5;
                            } else {
                                if (position >= limit) {
                                    fetch(4);
                                }
                                position += 4;
                            }
                        } else {
                            if (position >= limit) {
                                fetch(3);
                            }
                            position += 3;
                        }
                    } else {
                        if (position >= limit) {
                            fetch(2);
                        }
                        position += 2;
                    }
                } else {
                    if (position >= limit) {
                        fetch(1);
                    }
                    position++;
                }
            }
        }
    }

    public final void readTextUTF8(char[] target, int offset, int charCount) {
        // System.err.println("B");
        byte b;
        if (limit - position >= charCount << 3) { // if bigger than the text
                                                  // could be then use this
                                                  // shortcut
            // fast
            while (--charCount >= 0) {
                if ((b = buffer[position++]) >= 0) {
                    // code point 7
                    target[offset++] = (char) b;
                } else {
                    decodeUTF8Fast(target, offset++, b);// untested?? why
                }
            }
        } else {
            while (--charCount >= 0) {
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
                if ((b = buffer[position++]) >= 0) {
                    // code point 7
                    target[offset++] = (char) b;
                } else {
                    decodeUTF8(target, offset++, b);
                }
            }
        }
    }

    // convert single char that is not the simple case
    private void decodeUTF8(Appendable target, byte b) {
        byte[] source = buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                try {
                    target.append((char) 0xFFFD); // Bad data replacement char
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
                ++position;
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
                            if (limit - position < 5) {
                                fetch(5);
                            }
                            position += 5;
                            return;
                        }

                        if ((source[position] & 0xC0) != 0x80) {
                            try {
                                target.append((char) 0xFFFD); // Bad data
                                                              // replacement
                                                              // char
                            } catch (IOException e) {
                                throw new FASTException(e);
                            }
                            if (limit - position < 5) {
                                fetch(5);
                            }
                            position += 5;
                            return;
                        }
                        if (position >= limit) {
                            fetch(1); // CAUTION: may change value of position
                        }
                        result = (result << 6) | (source[position++] & 0x3F);
                    }
                    if ((source[position] & 0xC0) != 0x80) {
                        try {
                            target.append((char) 0xFFFD); // Bad data
                                                          // replacement char
                        } catch (IOException e) {
                            throw new FASTException(e);
                        }
                        if (limit - position < 4) {
                            fetch(4);
                        }
                        position += 4;
                        return;
                    }
                    if (position >= limit) {
                        fetch(1); // CAUTION: may change value of position
                    }
                    result = (result << 6) | (source[position++] & 0x3F);
                }
                if ((source[position] & 0xC0) != 0x80) {
                    try {
                        target.append((char) 0xFFFD); // Bad data replacement
                                                      // char
                    } catch (IOException e) {
                        throw new FASTException(e);
                    }
                    if (limit - position < 3) {
                        fetch(3);
                    }
                    position += 3;
                    return;
                }
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
                result = (result << 6) | (source[position++] & 0x3F);
            }
            if ((source[position] & 0xC0) != 0x80) {
                try {
                    target.append((char) 0xFFFD); // Bad data replacement char
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                if (limit - position < 2) {
                    fetch(2);
                }
                position += 2;
                return;
            }
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            result = (result << 6) | (source[position++] & 0x3F);
        }
        if ((source[position] & 0xC0) != 0x80) {
            try {
                target.append((char) 0xFFFD); // Bad data replacement char
            } catch (IOException e) {
                throw new FASTException(e);
            }
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            position += 1;
            return;
        }
        try {
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            target.append((char) ((result << 6) | (source[position++] & 0x3F)));
        } catch (IOException e) {
            throw new FASTException(e);
        }
    }

    // convert single char that is not the simple case
    private void decodeUTF8(char[] target, int targetIdx, byte b) {

        byte[] source = buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
                ++position;
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
                            if (limit - position < 5) {
                                fetch(5);
                            }
                            position += 5;
                            return;
                        }

                        if ((source[position] & 0xC0) != 0x80) {
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            if (limit - position < 5) {
                                fetch(5);
                            }
                            position += 5;
                            return;
                        }
                        if (position >= limit) {
                            fetch(1); // CAUTION: may change value of position
                        }
                        result = (result << 6) | (source[position++] & 0x3F);
                    }
                    if ((source[position] & 0xC0) != 0x80) {
                        target[targetIdx] = 0xFFFD; // Bad data replacement char
                        if (limit - position < 4) {
                            fetch(4);
                        }
                        position += 4;
                        return;
                    }
                    if (position >= limit) {
                        fetch(1); // CAUTION: may change value of position
                    }
                    result = (result << 6) | (source[position++] & 0x3F);
                }
                if ((source[position] & 0xC0) != 0x80) {
                    target[targetIdx] = 0xFFFD; // Bad data replacement char
                    if (limit - position < 3) {
                        fetch(3);
                    }
                    position += 3;
                    return;
                }
                if (position >= limit) {
                    fetch(1); // CAUTION: may change value of position
                }
                result = (result << 6) | (source[position++] & 0x3F);
            }
            if ((source[position] & 0xC0) != 0x80) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                if (limit - position < 2) {
                    fetch(2);
                }
                position += 2;
                return;
            }
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            result = (result << 6) | (source[position++] & 0x3F);
        }
        if ((source[position] & 0xC0) != 0x80) {
            target[targetIdx] = 0xFFFD; // Bad data replacement char
            if (position >= limit) {
                fetch(1); // CAUTION: may change value of position
            }
            position += 1;
            return;
        }
        if (position >= limit) {
            fetch(1); // CAUTION: may change value of position
        }
        target[targetIdx] = (char) ((result << 6) | (source[position++] & 0x3F));
    }

    private void decodeUTF8Fast(char[] target, int targetIdx, byte b) {

        byte[] source = buffer;

        int result;
        if (((byte) (0xFF & (b << 2))) >= 0) {
            if ((b & 0x40) == 0) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                ++position;
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
                            position += 5;
                            return;
                        }

                        if ((source[position] & 0xC0) != 0x80) {
                            target[targetIdx] = 0xFFFD; // Bad data replacement
                                                        // char
                            position += 5;
                            return;
                        }
                        result = (result << 6) | (source[position++] & 0x3F);
                    }
                    if ((source[position] & 0xC0) != 0x80) {
                        target[targetIdx] = 0xFFFD; // Bad data replacement char
                        position += 4;
                        return;
                    }
                    result = (result << 6) | (source[position++] & 0x3F);
                }
                if ((source[position] & 0xC0) != 0x80) {
                    target[targetIdx] = 0xFFFD; // Bad data replacement char
                    position += 3;
                    return;
                }
                result = (result << 6) | (source[position++] & 0x3F);
            }
            if ((source[position] & 0xC0) != 0x80) {
                target[targetIdx] = 0xFFFD; // Bad data replacement char
                position += 2;
                return;
            }
            result = (result << 6) | (source[position++] & 0x3F);
        }
        if ((source[position] & 0xC0) != 0x80) {
            target[targetIdx] = 0xFFFD; // Bad data replacement char
            position += 1;
            return;
        }
        target[targetIdx] = (char) ((result << 6) | (source[position++] & 0x3F));
    }

    public boolean isEOF() {
        if (limit != position) {
            return false;
        }
        fetch(0);
        return limit != position ? false : input.isEOF();
    }

    // ///////////////////////////////
    // Dictionary specific operations
    // ///////////////////////////////

    public final int readIntegerUnsignedOptional(int constAbsent) {
        int value = readIntegerUnsignedPrivate();
        return value == 0 ? constAbsent : value - 1;
    }

    public final int readIntegerSignedConstantOptional(int constAbsent, int constConst) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constAbsent : constConst);
    }

    public final int readIntegerUnsignedConstantOptional(int constAbsent, int constConst) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constAbsent : constConst);
    }

    public final int readIntegerUnsignedCopy(int target, int source, int[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? dictionary[source]
                : (dictionary[target] = readIntegerUnsignedPrivate()));
    }

    public final int readIntegerUnsignedDelta(int target, int source, int[] dictionary) {
        // Delta opp never uses PMAP
        return (dictionary[target] = (int) (dictionary[source] + readLongSignedPrivate()));
    }

    public final int readIntegerUnsignedDeltaOptional(int target, int source, int[] dictionary, int constAbsent) {
        // Delta opp never uses PMAP
        long value = readLongSignedPrivate();
        if (0 == value) {
            dictionary[target] = 0;// set to absent
            return constAbsent;
        } else {
            return dictionary[target] = (int) (dictionary[source] + (value > 0 ? value - 1 : value));

        }
    }

    public final int readIntegerUnsignedDefault(int constDefault) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constDefault : readIntegerUnsignedPrivate());
    }

    public final int readIntegerUnsignedDefaultOptional(int constDefault, int constAbsent) {
        int value;
        return (popPMapBit(pmapIdx, bitBlock) == 0) ? constDefault
                : (value = readIntegerUnsignedPrivate()) == 0 ? constAbsent : value - 1;
    }

    public final int readIntegerUnsignedIncrement(int target, int source, int[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readIntegerUnsignedPrivate()));
    }

    public final int readIntegerUnsignedIncrementOptional(int target, int source, int[] dictionary, int constAbsent) {

        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            int value;
            if ((value = readIntegerUnsignedPrivate()) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    public final int readIntegerSignedOptional(int constAbsent) {
        int value = readIntegerSignedPrivate();
        return value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
    }

    public final int readIntegerSignedCopy(int target, int source, int[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? dictionary[source]
                : (dictionary[target] = readIntegerSignedPrivate()));
    }

    public final int readIntegerSignedDelta(int target, int source, int[] dictionary) {
        // Delta opp never uses PMAP
        return (dictionary[target] = (int) (dictionary[source] + readLongSignedPrivate()));
    }

    public final int readIntegerSignedDeltaOptional(int target, int source, int[] dictionary, int constAbsent) {
        // Delta opp never uses PMAP
        long value = readLongSignedPrivate();
        if (0 == value) {
            dictionary[target] = 0;// set to absent
            return constAbsent;
        } else {
            return dictionary[target] = (int) (dictionary[source] + (value > 0 ? value - 1 : value));

        }
    }

    public final int readIntegerSignedDefault(int constDefault) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constDefault : readIntegerSignedPrivate());
    }

    public final int readIntegerSignedDefaultOptional(int constDefault, int constAbsent) {
        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return constDefault;
        } else {
            int value = readIntegerSignedPrivate();
            return value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        }
    }

    public final int readIntegerSignedIncrement(int target, int source, int[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readIntegerSignedPrivate()));
    }

    public final int readIntegerSignedIncrementOptional(int target, int source, int[] dictionary, int constAbsent) {

        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            int value;
            if ((value = readIntegerSignedPrivate()) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    // For the Long values

    public final long readLongUnsigned(int target, long[] dictionary) {
        // no need to set initValueFlags for field that can never be null
        return dictionary[target] = readLongUnsignedPrivate();
    }

    public final long readLongUnsignedOptional(long constAbsent) {
        long value = readLongUnsignedPrivate();
        return value == 0 ? constAbsent : value - 1;
    }

    public final long readLongSignedConstantOptional(long constAbsent, long constConst) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constAbsent : constConst);
    }

    public final long readLongUnsignedConstantOptional(long constAbsent, long constConst) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constAbsent : constConst);
    }

    public final long readLongUnsignedCopy(int target, int source, long[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? dictionary[source]
                : (dictionary[target] = readLongUnsignedPrivate()));
    }

    public final long readLongUnsignedDelta(int target, int source, long[] dictionary) {
        // Delta opp never uses PMAP
        return (dictionary[target] = (dictionary[source] + readLongSignedPrivate()));
    }

    public final long readLongUnsignedDeltaOptional(int target, int source, long[] dictionary, long constAbsent) {
        // Delta opp never uses PMAP
        long value = readLongSignedPrivate();
        if (0 == value) {
            dictionary[target] = 0;// set to absent
            return constAbsent;
        } else {
            return dictionary[target] = (dictionary[source] + (value > 0 ? value - 1 : value));

        }
    }

    public final long readLongUnsignedDefault(long constDefault) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constDefault : readLongUnsignedPrivate());
    }

    public final long readLongUnsignedDefaultOptional(long constDefault, long constAbsent) {
        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return constDefault;
        } else {
            long value = readLongUnsignedPrivate();
            return value == 0 ? constAbsent : value - 1;
        }
    }

    public final long readLongUnsignedIncrement(int target, int source, long[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readLongUnsignedPrivate()));
    }

    public final long readLongUnsignedIncrementOptional(int target, int source, long[] dictionary, long constAbsent) {

        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            long value;
            if ((value = readLongUnsignedPrivate()) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    public final long readLongSigned(int target, long[] dictionary) {
        // no need to set initValueFlags for field that can never be null
        return dictionary[target] = readLongSignedPrivate();
    }

    public final long readLongSignedOptional(long constAbsent) {
        long value = readLongSignedPrivate();
        return value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
    }

    public final long readLongSignedCopy(int target, int source, long[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? dictionary[source]
                : (dictionary[target] = readLongSignedPrivate()));
    }

    public final long readLongSignedDelta(int target, int source, long[] dictionary) {
        // Delta opp never uses PMAP
        return (dictionary[target] = (dictionary[source] + readLongSignedPrivate()));
    }

    public final long readLongSignedDeltaOptional(int target, int source, long[] dictionary, long constAbsent) {
        // Delta opp never uses PMAP
        long value = readLongSignedPrivate();
        if (0 == value) {
            dictionary[target] = 0;// set to absent
            return constAbsent;
        } else {
            return dictionary[target] = (dictionary[source] + (value > 0 ? value - 1 : value));

        }
    }

    public final long readLongSignedDefault(long constDefault) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? constDefault : readLongSignedPrivate());
    }

    public final long readLongSignedDefaultOptional(long constDefault, long constAbsent) {
        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return constDefault;
        } else {
            long value = readLongSignedPrivate();
            return value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        }
    }

    public final long readLongSignedIncrement(int target, int source, long[] dictionary) {
        return (popPMapBit(pmapIdx, bitBlock) == 0 ? (dictionary[target] = dictionary[source] + 1)
                : (dictionary[target] = readLongSignedPrivate()));
    }

    public final long readLongSignedIncrementOptional(int target, int source, long[] dictionary, long constAbsent) {

        if (popPMapBit(pmapIdx, bitBlock) == 0) {
            return (dictionary[target] == 0 ? constAbsent : (dictionary[target] = dictionary[source] + 1));
        } else {
            long value;
            if ((value = readLongSignedPrivate()) == 0) {
                dictionary[target] = 0;
                return constAbsent;
            } else {
                return (dictionary[target] = value) - 1;
            }
        }
    }

    // //////////////
    // /////////

    public final int openMessage(int pmapMaxSize) {
        openPMap(pmapMaxSize);
        // return template id or unknown
        return (0 != popPMapBit()) ? readIntegerUnsigned() : -1;// template Id

    }

}
