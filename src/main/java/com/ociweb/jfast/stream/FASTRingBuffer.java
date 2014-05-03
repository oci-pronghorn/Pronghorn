package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;

/**
 * Specialized ring buffer for holding decoded values from a FAST stream. Ring
 * buffer has blocks written which correspond to whole messages or sequence
 * items. Within these blocks the consumer is provided random (eg. direct)
 * access capabilities.
 * 
 * 
 * 
 * @author Nathan Tippy
 * 
 * 
 * Storage:
 *  int - 1 slot
 *  long - 2 slots, high then low 
 *  text - 2 slots, index then length  (if index is negative use constant array)
 * 
 */
public final class FASTRingBuffer {

    final int[] buffer;
    final int mask;

    public final int maxSize;

    final int maxCharSize;
    final int charMask;
    final char[] charBuffer;
    int addCharPos = 0;

    final int maxByteSize;
    final int byteMask;
    final byte[] byteBuffer;
    int addBytePos = 0;
    
    final char[] constTextBuffer; //defined externally and never changes
    final byte[] constByteBuffer;

    //TODO: A, Filter is a mistake instead we need Ring buffers per messages and a drop message one as well. Then it is all hardcoded per instance setup. need hash for those options.
    FASTFilter filter = FASTFilter.none;

    final AtomicInteger removeCount = new AtomicInteger();
    final AtomicInteger addCount = new AtomicInteger();
    public int addPos = 0;
    public int remPos = 0;

    public FASTRingBuffer(byte primaryBits, byte charBits, char[] constTextBuffer, byte[] constByteBuffer) {
        assert (primaryBits >= 1);

        this.constTextBuffer = constTextBuffer;
        this.constByteBuffer = constByteBuffer;
        
        this.maxSize = 1 << primaryBits;
        this.mask = maxSize - 1;
        this.buffer = new int[maxSize];

        this.maxCharSize = 1 << charBits;
        this.charMask = maxCharSize - 1;
        this.charBuffer = new char[maxCharSize];

        this.maxByteSize = maxCharSize;// TODO: add value for max bits size
        this.byteMask = maxByteSize - 1;
        this.byteBuffer = new byte[maxByteSize];
    }

    /**
     * Empty and restore to original values.
     */
    public void reset() {
        addCharPos = 0;
        addPos = 0;
        remPos = 0;
        removeCount.set(0);
        addCount.set(0);
    }

    // adjust these from the offset of the biginning of the message.

    public void release() {
        // step forward and allow write to previous location.
    }

    public void lockMessage() {
        // step forward to next message or sequence?
        // provide offset to the beginning of this message.

    }

    // TODO: A, add map method which can take data from one ring buffer and
    // populate another.

    // TODO: A, Promises/Futures/Listeners as possible better fit to stream
    // processing?
    // TODO: A, look at adding reduce method in addition to filter.

    public final int availableCapacity() {
        return maxSize - (addPos - remPos);
    }

    public static int peek(int[] buf, int pos, int mask) {
        return buf[mask & pos];
    }

    public static long peekLong(int[] buf, int pos, int mask) {
        return (((long) buf[mask & pos]) << 32) | (((long) buf[mask & (pos + 1)]) & 0xFFFFFFFFl);

    }

    // TODO: add mappers to go from one buffer to the next
    // TODO: add consumer/Iterator to go from ring buffer to Object stream
    // TODO: Map templates to methods for RMI of void methods(eg. one direction).
    // TODO: Z, add map toIterator method for consuming ring buffer by java8 streams.

    public final int appendInt1(int value) {
        buffer[mask & addPos++] = value;
        return value;
    }

    public int writeTextToRingBuffer(int heapId, int len, TextHeap textHeap) {//Invoked 100's of millions of times, must be tight.
        final int p = addCharPos;
        if (len > 0) {
            addCharPos = TextHeap.copyToRingBuffer(heapId, charBuffer, p, charMask,textHeap);
        }
        return p;
    }

    public int writeBytesToRingBuffer(int heapId, int len, ByteHeap byteHeap) {
        final int p = addBytePos;
        if (len > 0) {
            addBytePos = byteHeap.copyToRingBuffer(heapId, byteBuffer, p, byteMask);
        }
        return p;
    }

    // TODO: A, Use static method to access fields by offset based on
    // templateId.
    // TODO: A, At end of group check filter of that record and jump back if
    // need to skip.

    // next sequence is ready for consumption.
    public final void unBlockSequence() {
        // TODO: A, only filter on the message level. sequence will be  difficult because they are nested.

        // if filtered out the addPos will be rolled back to newGroupPos
        byte f = filter.go(addCount.get(), this);
        
        if (f > 0) {// consumer is allowed to read up to addCount
            // normal
            addCount.lazySet(addPos);
        } else if (f < 0) {
            // skip
            addPos = addCount.get();
        } // else hold
    }

    //
    public void unBlockMessage() {
        // if filtered out the addPos will be rolled back to newGroupPos
        byte f = filter.go(addCount.get(), this);// TODO: B, may call at end of
                                                 // message, can not change
                                                 // mind!
        if (0 == f) {
            // do not hold use default instead
            f = filter.defaultBehavior();
        }

        if (f > 0) {// consumer is allowed to read up to addCount
            // normal
            addCount.lazySet(addPos);
        } else {
            // skip
            addPos = addCount.get();
        }
    }

    public void removeForward(int step) {
        remPos = removeCount.get() + step;
        assert (remPos <= addPos);
        removeCount.lazySet(remPos);
    }

    public void dump() {
        // move the removePosition up to the addPosition
        // System.err.println("resetup to "+addPos);
        remPos = addPos;
        removeCount.lazySet(addPos);
    }

    // TODO: A, Given templateId, and FieldId return offset for RingBuffer to
    // get value, must keep in client code

    // this is for fast direct WRITE TO target
    public void readChars(int idx, char[] target, int targetIdx, TextHeap textHeap) {
        int ref1 = buffer[mask & (remPos + idx)];
        if (ref1 < 0) {
            textHeap.get(ref1, target, targetIdx);
        } else {
            int len = buffer[mask & (remPos + idx + 1)];
            // copy into target but may need to loop from text buffer
            while (--len >= 0) {
                target[targetIdx + len] = charBuffer[(ref1 + len) & charMask];
            }
        }
    }

    // WARNING: consumer of these may need to loop around end of buffer !!
    // these are needed for fast direct READ FROM here
    public int readRingCharPos(int fieldPos) {
        // constant from heap or dynamic from char ringBuffer
        int ref1 = buffer[mask & (remPos + fieldPos)];
        return ref1 < 0 ? ref1&0x7FFFFFFF : ref1;
    }

    public char[] readRingCharBuffer(int fieldPos) {
        // constant from heap or dynamic from char ringBuffer
        return buffer[mask & (remPos + fieldPos)] < 0 ? constTextBuffer : this.charBuffer;
    }

    public int readRingCharMask() {
        return charMask;
    }



    public boolean hasContent() {
        return addPos > remPos;
    }

    public int contentRemaining() {
        return addPos - remPos;
    }

}
