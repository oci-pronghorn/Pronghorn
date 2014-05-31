package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;

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

    public final int[] buffer;
    public final int mask;

    public final int maxSize;

    public final int maxCharSize;
    public final int charMask;
    public final char[] charBuffer;
    public int addCharPos = 0;

    final int maxByteSize;
    final int byteMask;
    final byte[] byteBuffer;
    int addBytePos = 0;
    
    final char[] constTextBuffer; //defined externally and never changes
    final byte[] constByteBuffer;

    FASTFilter filter = FASTFilter.none;

    final AtomicInteger removeCount;
    private final AtomicInteger addCount;
    public int addPos;
    public int remPos;
    
    //TODO: A, use stack of offsets for each fragment until full message is completed.
    private int[] fragStack; //TODO: B, first offset 0 points to the constants after the ring buffer.

    public FASTRingBuffer(byte primaryBits, byte charBits, char[] constTextBuffer, byte[] constByteBuffer) {
        assert (primaryBits >= 1);       
        
        int maxFragDepth = 10;//TODO: A, must compute max frag depth in template parser.        
        this.fragStack = new int[maxFragDepth];
        
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.maxSize = 1 << primaryBits;
        this.mask = maxSize - 1;
        

        this.buffer = new int[maxSize];      
        
        //TODO: A, jump size along with fields are stored as constants relative to script postion (keep as much as possible in ring buffer)

        //TODO: A, use callback upon new class load to reset field offsets.
        
       
        this.removeCount = new AtomicInteger(); //reader reads from this position.
        this.addCount = new AtomicInteger(); // consumer is allowed to read up to addCount
        this.addPos = 0; //assigned to addCount when the full record is read.
        this.remPos = 0; //reads from this postion and assigned from removeCount;
        


        //constant data will never change and is populated externally.
        
        this.constTextBuffer = constTextBuffer;
        this.constByteBuffer = constByteBuffer;
        
        //single text and byte buffers because this is where the variable length data will go.
        
        this.maxCharSize = 1 << charBits;
        this.charMask = maxCharSize - 1;
        this.charBuffer = new char[maxCharSize];

        this.maxByteSize = maxCharSize;
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

    // TODO: C, add map method which can take data from one ring buffer and
    // populate another.

    // TODO: C, Promises/Futures/Listeners as possible better fit to stream
    // processing?
    // TODO: C, look at adding reduce method in addition to filter.

    public final int availableCapacity() {
        return maxSize - (addPos - remPos);
    }

    public static int peek(int[] buf, int pos, int mask) {
        return buf[mask & pos];
    }

    public static long peekLong(int[] buf, int pos, int mask) {
        return (((long) buf[mask & pos]) << 32) | (((long) buf[mask & (pos + 1)]) & 0xFFFFFFFFl);

    }

    // TODO: Z, add consumer/Iterator to go from ring buffer to Object stream
    // TODO: Z, Map templates to methods for RMI of void methods(eg. one direction).
    // TODO: Z, add map toIterator method for consuming ring buffer by java8 streams.


    public int writeTextToRingBuffer(int heapId, int len, TextHeap textHeap) {//Invoked 100's of millions of times, must be tight.
        final int p = addCharPos;
        if (len > 0) {
            addCharPos = TextHeap.copyToRingBuffer(heapId, charBuffer, p, charMask,textHeap);
        }
        return p;
    }
    
    public int writeTextToRingBuffer(int heapId, int len, PrimitiveReader reader) {//Invoked 100's of millions of times, must be tight.
        final int p = addCharPos;
        if (len > 0) {
            
            int lenTemp = PrimitiveReader.readTextASCIIIntoRing(charBuffer, p, charBuffer.length, reader);
            addCharPos+=lenTemp;// = TextHeap.copyToRingBuffer(heapId, charBuffer, p, charMask,textHeap);
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

    // TODO: A, Callback interface for setting the offsets used by the clients, Generate list of FieldId static offsets for use by static reader based on templateId.
    
    //TODO: A, build multi target ring buffers per message and null ring buffer to drop messages.

    // next sequence is ready for consumption.
    public static final void unBlockSequence(FASTRingBuffer ringBuffer) {
        //can not change mind after first decision.
        // if filtered out the addPos will be rolled back to newGroupPos
        byte f = ringBuffer.filter.go(ringBuffer.addCount.get(), ringBuffer);
        
        if (f > 0) {// consumer is allowed to read up to addCount
            // normal
            ringBuffer.addCount.lazySet(ringBuffer.addPos);
        } else if (f < 0) {
            // skip
            ringBuffer.addPos = ringBuffer.addCount.get();
        } // else hold
    }

    //
    public static final void unBlockMessage(FASTRingBuffer ringBuffer) {
        // TODO: B, add filter rules against leading fragment of message including message id. Can not change mind later!      
        
        // if filtered out the addPos will be rolled back to newGroupPos
        byte f = ringBuffer.filter.go(ringBuffer.addCount.get(), ringBuffer);
        if (0 == f) {
            // do not hold use default instead
            f = ringBuffer.filter.defaultBehavior();
        }

        if (f > 0) {// consumer is allowed to read up to addCount
            // normal
            ringBuffer.addCount.lazySet(ringBuffer.addPos);
        } else {
            // skip
            ringBuffer.addPos = ringBuffer.addCount.get();
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
