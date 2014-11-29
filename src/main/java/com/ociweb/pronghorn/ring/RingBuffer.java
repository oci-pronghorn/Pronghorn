package com.ociweb.pronghorn.ring;

import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.pronghorn.ring.util.PaddedAtomicInteger;
import com.ociweb.pronghorn.ring.util.PaddedAtomicLong;



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



// TODO: X, (optimization) need to get messageId when its the only message and so not written to the ring buffer.
// TODO: C, add map method which can take data from one ring buffer and populate another.
// TODO: C, look at adding reduce method in addition to filter.
// TODO: X, dev ops tool to empty (drain) buffers and record the loss.
// TODO: B, must add way of selecting what field to skip writing for the consumer.

//TODO: B, build  null ring buffer to drop messages.

public final class RingBuffer {
   

    public static class PaddedLong {
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    public static class PaddedInt {
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    
    public final int maxSize;
    public final int[] buffer;
    public final int mask;
    public final PaddedLong workingHeadPos = new PaddedLong();
    public final PaddedLong workingTailPos = new PaddedLong();

    public final AtomicLong tailPos = new PaddedAtomicLong(); // producer is allowed to write up to tailPos
    public final AtomicLong headPos = new PaddedAtomicLong(); // consumer is allowed to read up to headPos
    
    public final int maxByteSize;
    public final byte[] byteBuffer;
    public final int byteMask;
    public final PaddedInt byteWorkingHeadPos = new PaddedInt();
    public final PaddedInt byteWorkingTailPos = new PaddedInt();
    
    public final PaddedAtomicInteger bytesHeadPos = new PaddedAtomicInteger();
    public final PaddedAtomicInteger bytesTailPos = new PaddedAtomicInteger();
    
    //defined externally and never changes
    final byte[] constByteBuffer;
    private final byte[][] bufferLookup;

//TODO: check the length and hash to lower the probably of crash.
    


    // end of moveNextFields

    static final int JUMP_MASK = 0xFFFFF;
    public final WalkingConsumerState consumerData;
    
    
    /**
     * Construct simple ring buffer without any assumed data structures
     * @param primaryBits
     * @param byteBits
     */
    public RingBuffer(byte primaryBits, byte byteBits) {
    	this(primaryBits, byteBits, null,  FieldReferenceOffsetManager.RAW_BYTES);
    	if ((primaryBits>>1)>byteBits) {
    		throw new UnsupportedOperationException("The byteBits value must be at least "+(primaryBits>>1)+" and should be even bigger but it was set to "+byteBits+" alternatively primaryBits could be set to a value less than "+(byteBits<<1));
    	}    	
    }
    
    /**
     * Construct ring buffer with re-usable constants and fragment structures
     * 
     * @param primaryBits
     * @param byteBits
     * @param byteConstants
     * @param from
     */
    public RingBuffer(byte primaryBits, byte byteBits, byte[] byteConstants, FieldReferenceOffsetManager from) {
        //constant data will never change and is populated externally.
        
        assert (primaryBits >= 1);       
                
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.maxSize = 1 << primaryBits;
        this.mask = maxSize - 1;
        
        this.buffer = new int[maxSize];    
  
        //single text and byte buffers because this is where the variable length data will go.

        this.maxByteSize =  1 << byteBits;
        this.byteMask = maxByteSize - 1;
        this.byteBuffer = new byte[maxByteSize];

        this.constByteBuffer = byteConstants;
        this.bufferLookup = new byte[][] {byteBuffer,constByteBuffer};
                
        this.consumerData = new WalkingConsumerState(mask, from);
    }

    

    
    /**
     * Empty and restore to original values.
     */
    public void reset() {

    	workingHeadPos.value = 0;
        workingTailPos.value = 0;
        tailPos.set(0);
        headPos.set(0); 
        byteWorkingHeadPos.value = 0;
        bytesHeadPos.set(0);
        byteWorkingTailPos.value = 0;
        bytesTailPos.set(0);
        
        WalkingConsumerState.reset(consumerData);
        

    }
    
    


    


    public static int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static long peekLong(int[] buf, long pos, int mask) {
        
        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    public static void addByteArray(byte[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
    	    	
        final int p = rbRingBuffer.byteWorkingHeadPos.value;
        if (sourceLen > 0) {
        	int targetMask = rbRingBuffer.byteMask;
        	int proposedEnd = p + sourceLen;
			byte[] target = rbRingBuffer.byteBuffer;        	
			
			//NOTE: we are not checking for overflow but if we did it would be here
			
            int tStop = (p + sourceLen) & targetMask;
			int tStart = p & targetMask;
			if (tStop > tStart) {
			    System.arraycopy(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    int firstLen = 1+ targetMask - tStart;
			    System.arraycopy(source, sourceIdx, target, tStart, firstLen);
			    System.arraycopy(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
			}
            rbRingBuffer.byteWorkingHeadPos.value = proposedEnd;
        }        
        
        addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
        addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
    }
    

	public static void addValue(RingBuffer rb, int value) {
		 addValue(rb.buffer, rb.mask, rb.workingHeadPos, value);		
	}
    
   
    //we are only allowed 12% of the time or so for doing this write.
    //this pushes only ~5gbs but if we had 100% it would scale to 45gbs
    //so this is not the real bottleneck and given the compression ratio of the test data
    //we can push 1gbs more of compressed data for each 10% of cpu freed up.
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value) {
        buffer[rbMask & (int)headCache.value++] = value;
    } 
    
    public static void setValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] = value;
    } 
    
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p] = value1; //TODO: X, code gen replace rbMask with constant may help remove check
        buffer[rbMask & (int)(p+1)] = value2; //TODO: X, code gen replace rbMask with constant may help remove check
        headCache.value = p+2;
        
    } 
    
       
    
    public static void dump(RingBuffer rb) {
                       
        // move the removePosition up to the addPosition
        // new Exception("WARNING THIS IS NO LONGER COMPATIBLE WITH PUMP CALLS").printStackTrace();
        rb.tailPos.lazySet(rb.workingTailPos.value = rb.workingHeadPos.value);
    }

    // WARNING: consumer of these may need to loop around end of buffer !!
    // these are needed for fast direct READ FROM here

    public static int readRingByteLen(int fieldPos, int[] rbB, int rbMask, PaddedLong rbPos) {
    //	System.err.println("read len:"+rbB[rbMask & (int)(rbPos.value + fieldPos + 1)]+" from "+rbPos.value+" field "+fieldPos);
        return rbB[rbMask & (int)(rbPos.value + fieldPos + 1)];// second int is always the length
    }

	public static int readRingByteLen(int idx, RingBuffer ring) {
		return readRingByteLen(idx,ring.buffer,ring.mask,ring.workingTailPos);       
	}
	
	public static int takeRingByteLen(RingBuffer ring) {		
		return ring.buffer[(int)(ring.mask & (ring.workingTailPos.value++))];// second int is always the length     
	}
    
    public static int bytePosition(int meta, RingBuffer ring, int len) {
    	
    	int pos = meta&0x7FFFFFFF;//may be negative when it is a constant but lower bits are always position
    	
    	int end = pos + len; //need this in order to find the tail to detect overlap 	
    	if (end > ring.byteWorkingTailPos.value) {
    		ring.byteWorkingTailPos.value = end;
    	}
    	
        return pos; 
    }    

    public static byte[] byteBackingArray(int meta, RingBuffer rbRingBuffer) {
        return rbRingBuffer.bufferLookup[meta>>>31];
    }
    
	public static int readRingByteMetaData(int pos, RingBuffer rb) {
		return readValue(pos,rb.buffer,rb.mask,rb.workingTailPos.value);
	}
			
	public static int takeRingByteMetaData(RingBuffer ring) {
		return readValue(0,ring.buffer,ring.mask,ring.workingTailPos.value++);
	}
	
    public static int readValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }
   
    public static int readValue(int idx, RingBuffer ring) {    	
    	return readValue(idx, ring.buffer,ring.mask,ring.workingTailPos.value);
    }
    
    public static int takeValue(RingBuffer ring) {    	
    	return readValue(0, ring.buffer,ring.mask,ring.workingTailPos.value++);
    }
    
    public static int contentRemaining(RingBuffer rb) {
        return (int)(rb.headPos.longValue() - rb.tailPos.longValue()); //must not go past add count because it is not release yet.
    }

    public static void setWorkingTailPosition(RingBuffer ring, long position) {
    	ring.workingTailPos.value = position;
    }
    
    public static long getWorkingTailPosition(RingBuffer ring) {
    	return ring.workingTailPos.value;
    }
    
    public static void releaseReadLock(RingBuffer ring) {
    	ring.tailPos.lazySet(ring.workingTailPos.value);
    	ring.bytesTailPos.lazySet(ring.byteWorkingTailPos.value);
    }
    
    public static void publishWrites(RingBuffer ring) {
    	
    	//prevent long running arrays from rolling over in second byte ring
    	ring.byteWorkingHeadPos.value = ring.byteMask & ring.byteWorkingHeadPos.value;
    	ring.byteWorkingTailPos.value = ring.byteMask & ring.byteWorkingTailPos.value;
    	if (ring.byteWorkingHeadPos.value < ring.byteWorkingTailPos.value ) {
    		ring.byteWorkingHeadPos.value += (ring.byteMask + 1);
    	}

    	//publish writes
    	ring.headPos.lazySet(ring.workingHeadPos.value);
    	ring.bytesHeadPos.lazySet(ring.byteWorkingHeadPos.value);
    }
    
    public static void abandonWrites(RingBuffer ring) {    
        //ignore the fact that any of this was written to the ring buffer
    	ring.workingHeadPos.value = ring.headPos.longValue();
    	ring.byteWorkingHeadPos.value = ring.bytesHeadPos.intValue();
    }


    //All the spin lock methods share the same implementation. Unfortunately these can not call 
    //a common implementation because the extra method jump degrades the performance in tight loops
    //where these spin locks are commonly used.
    
    public static long spinBlockOnTailTillMatchesHead(long lastCheckedValue, RingBuffer ringBuffer) {
    	long targetValue = ringBuffer.headPos.longValue();
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield(); //needed for now but re-evaluate performance impact
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		} 
		return lastCheckedValue;
    }
    
    public static long spinBlockOnTail(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHeadTillMatchesTail(long lastCheckedValue, RingBuffer ringBuffer) {
    	long targetValue = ringBuffer.tailPos.longValue();    	
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
		    lastCheckedValue = ringBuffer.headPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHead(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
		    lastCheckedValue = ringBuffer.headPos.longValue();
		}
		return lastCheckedValue;
    }
    
	public static int byteMask(RingBuffer ring) {
		return ring.byteMask;
	}

	public static long headPosition(RingBuffer ring) {
		 return ring.headPos.get();
	}

	public static long tailPosition(RingBuffer ring) {
		return ring.tailPos.get();
	}

	public static int primarySize(RingBuffer ring) {
		return ring.maxSize;
	}
	
}
