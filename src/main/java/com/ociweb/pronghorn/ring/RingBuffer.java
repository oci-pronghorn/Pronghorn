package com.ociweb.pronghorn.ring;

import java.util.concurrent.atomic.AtomicBoolean;
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

// TODO: C, add map method which can take data from one ring buffer and populate another.
// TODO: C, look at adding reduce method in addition to filter.
// TODO: X, dev ops tool to empty (drain) buffers and record the loss.
// TODO: B, must add way of selecting what field to skip writing for the consumer.

//TODO: B, build  null ring buffer to drop messages.

//TODO: AA, check if low level api will update stats
//TODO: AA, auto optimized publish rate based on ring buffer status and full frequency

public final class RingBuffer {
   
    public static class PaddedLong {
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    public static class PaddedInt {
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    //TODO: AA, note original disrupter allows for multiple threads to each visit the same spot and each do mutation
    //          there is no problem with doing this upgrade to the ring buffer support.
    
    
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
    
    public final int maxAvgVarLen; 
    private int varLenMovingAverage = 0;//this is an exponential moving average

    // end of moveNextFields

    static final int JUMP_MASK = 0xFFFFF;
    public final RingWalker consumerData;
    
    public final byte pBits;
    public final byte bBits;
    
    private final AtomicBoolean shutDown = new AtomicBoolean(false);//TODO: A, create unit test examples for using this.
    
    
    public RingBuffer(RingBufferConfig config) {
    	this(config.primaryBits, config.byteBits, config.byteConst, config.from);
    }
    
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
        
    	this.pBits = primaryBits;
    	this.bBits = byteBits;
    	
        assert (primaryBits >= 0); //zero is a special case for a mock ring       
                
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
                
        this.consumerData = new RingWalker(mask, from);
        
        if (0 == from.maxVarFieldPerUnit || 0==primaryBits) { //zero bits is for the dummy mock case
        	maxAvgVarLen = 0; //no fragments had any variable length fields so we never allow any
        } else {
        	//given outer ring buffer this is the maximum number of var fields that can exist at the same time.
        	int maxVarCount = (int)Math.ceil(maxSize*from.maxVarFieldPerUnit);
        	//we require at least 2 fields to ensure that the average approach works in all cases
        	if (maxVarCount < 2) {
        		// 2 = size * perUnit
        		int minSize = (int)Math.ceil(2f/from.maxVarFieldPerUnit);
        		int minBits = 32 - Integer.numberOfLeadingZeros(minSize - 1);
        		throw new UnsupportedOperationException("primary buffer is too small it must be at least "+minBits+" bits"); 
        	}
        	//to allow more almost 2x more flexibility in variable length bytes we track pairs of writes and ensure the 
        	//two together are below the threshold rather than each alone
        	maxAvgVarLen = maxByteSize/maxVarCount;
        }
        
             
        
    }
    
	public void validateVarLength(int length) {
		
	//	System.err.println("write len:"+length+" max is  "+maxAvgVarLen);
		
		int newAvg = (length+varLenMovingAverage)>>1;
        if (newAvg>maxAvgVarLen)	{
        	
        	int bytesPerInt = (int)Math.ceil(length*RingBuffer.from(this).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);
        	
        	throw new UnsupportedOperationException("Can not write byte array of length "+length+". The dif between primary and byte bits should be at least "+bitsDif+". "+pBits+","+bBits);
        }
        varLenMovingAverage = newAvg;
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
        
        RingWalker.reset(consumerData);
        

    }
        

    public static void addByteArrayWithMask(final RingBuffer outputRing, int mask, int len, byte[] data, int offset) {
		if ((offset&mask) <= ((offset+len-1) & mask)) {
			
			//simple add bytes
			addByteArray(data, offset&mask, len, outputRing);
			 
		} else {						
			
			//rolled over the end of the buffer
			int len1 = 1+mask-(offset&mask);
			appendPartialBytesArray(data, offset&mask, len1, outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask);        
			appendPartialBytesArray(data, 0, len-len1, outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask);        
			
			addBytePosAndLen(outputRing.buffer, outputRing.mask, outputRing.workingHeadPos, outputRing.bytesHeadPos.get(), outputRing.byteMask& outputRing.byteWorkingHeadPos.value, len);
			outputRing.byteWorkingHeadPos.value = outputRing.byteWorkingHeadPos.value + len;
		}
	}

	public static int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static long peekLong(int[] buf, long pos, int mask) {
        
        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }
    
    public static boolean isShutDown(RingBuffer ring) {
    	return ring.shutDown.get();
    }
    
    public static void shutDown(RingBuffer ring) {
    	ring.shutDown.set(true);
    }    

    public static void addByteArray(byte[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {
    	
    	assert(sourceLen>=0);
        appendPartialBytesArray(source, sourceIdx, sourceLen, rbRingBuffer.byteBuffer, rbRingBuffer.byteWorkingHeadPos.value, rbRingBuffer.byteMask);   
        addBytePosAndLen(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, rbRingBuffer.bytesHeadPos.get(), rbRingBuffer.byteWorkingHeadPos.value, sourceLen);
        rbRingBuffer.byteWorkingHeadPos.value = rbRingBuffer.byteWorkingHeadPos.value + sourceLen;		
		
    }
    
    public static void addNullByteArray(RingBuffer rbRingBuffer) {
        addBytePosAndLen(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, rbRingBuffer.bytesHeadPos.get(), rbRingBuffer.byteWorkingHeadPos.value, -1);
    }
    

	public static void appendPartialBytesArray(byte[] source, int sourceIdx, int sourceLen,
			                                   byte[] target, final int targetBytePos, int targetMask) {
		int tStop = (targetBytePos + sourceLen) & targetMask;
		int tStart = targetBytePos & targetMask;
		if (tStop >= tStart) {
		    System.arraycopy(source, sourceIdx, target, tStart, sourceLen);
		} else {
			// done as two copies
		    int firstLen = (1+ targetMask) - tStart;
		    System.arraycopy(source, sourceIdx, target, tStart, firstLen);
		    System.arraycopy(source, sourceIdx + firstLen, target, 0, sourceLen - firstLen);
		}
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
    
    public static void addBytePosAndLen(int[] buffer, int rbMask, PaddedLong headCache, int bytesHeadPos, int position, int length) {
        //TODO: AA, at this point we can modify the pos that is set.
    	//negative position is written as is because the internal array does not have any offset (but it could some day)
    	//positive position is written after subtracting the rbRingBuffer.bytesHeadPos.longValue()
    	int tmp = position;
//    	if (position>=0) {
//    		tmp = (int)(position-bytesHeadPos);
//    		if (tmp<0) {
//    			throw new UnsupportedOperationException("bad value "+tmp+"  "+position+" "+bytesHeadPos);
//    		}
//    	}
    	
        long p = headCache.value; 
        buffer[rbMask & (int)p] = tmp;
        buffer[rbMask & (int)(p+1)] = length;
        headCache.value = p+2;
        
    } 
    
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2, int value3) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p++] = value1;
        buffer[rbMask & (int)p++] = value2;
        buffer[rbMask & (int)p++] = value3;
        headCache.value = p;
        
    }    
    
    public static void addLongValue(int[] buffer, int rbMask, PaddedLong headCache, long value) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p] = (int)(value >>> 32);
        buffer[rbMask & (int)(p+1)] = (int)(value & 0xFFFFFFFF);
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
        return rbB[(int) (rbMask & (rbPos.value + fieldPos + 1))];// second int is always the length
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
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		} 
		return lastCheckedValue;
    }
    
    public static long spinBlockOnTail(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.tailPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHeadTillMatchesTail(long lastCheckedValue, RingBuffer ringBuffer) {
    	long targetValue = ringBuffer.tailPos.longValue();    	
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
		    lastCheckedValue = ringBuffer.headPos.longValue();
		}
		return lastCheckedValue;
    }
    
    public static long spinBlockOnHead(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		Thread.yield();//needed for now but re-evaluate performance impact
    		if (isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
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

	public static FieldReferenceOffsetManager from(RingBuffer ring) {
		return ring.consumerData.from;
	}
	
}
