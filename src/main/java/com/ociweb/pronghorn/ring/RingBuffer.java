package com.ociweb.pronghorn.ring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.pronghorn.ring.util.PaddedAtomicLong;


//cas: comment -- general for full file.
//     -- It would be worthwhile running this through a code formatter to bring it in line with what the
//        typical Java dev would expect.  A couple of things to consider would be max line length (one of the asserts
//        makes it all the way to col. 244).   A truly curious thing is the lack of spaces around the
//        less-than/greater-than operators.  All the other ops seem to get a nice padding, but not these.  Unequal
//        treatment, it would seem.  More or less.
//     --  JavaDoc!  (obviously) I think I can do this for you.  I'm not sure when I can do it, but the class seems
//         mature enough that the public API should be well-documented.  (Although I'm not sure all the public API
//         really is public.)  There will be some "jdoc" comments below as hints for whoever does it.

/**
 * Schema aware data pipe implemented as an internal pair of ring buffers.  One ring holds all the fixed length 
 * fields and the fixed length meta data relating to the variable length (unstructured firelds).  The other ring
 * holds only bytes which back the variable length fields like Strings, or Images.
 * 
 * The supported Schema is defined in the FieldReferenceOffsetManager passed in upon construction.  The Schema is 
 * make up of Messages and Messages are are made up of one or more fixed length fragments.  
 * 
 * These fragments enable direct lookup of fields within sequences and enable the consumptino of larger messages than 
 * would fit within the defined limits of the buffers. 
 *
 *
 * @author Nathan Tippy
 *
 *
 * //cas: this needs expanded explanation of what a slot is. (best if done above.)
 * Storage:
 *  int     - 1 slot
 *  long    - 2 slots, high then low
 *  text    - 2 slots, index then length  (if index is negative use constant array)
 *  decimal - 3 slots, exponent then long mantissa
 *
 *
 * StructuredLayoutRing  (these do in fact have strong type definition per field, in addition to being fixed length)
 * UnstructuredLayoutRing  (these are just bytes that are commonly UTF-8 encoded strings but may be image data or even video/audio)
 */

public final class RingBuffer {

    /**
     * Holds the active head position information
     */
    static class StructuredLayoutRingHead {
        final PaddedLong workingHeadPos;
        final AtomicLong headPos;

        StructuredLayoutRingHead() {
            this.workingHeadPos = new PaddedLong();
            this.headPos = new PaddedAtomicLong();
        }
    }
    
    static class StructuredLayoutRingTail {
        
        /**
         * The workingTailPosition is only to be used by the consuming thread. As values are read the tail is moved forward.
         * Eventually the consumer finishes the read of the fragment and will use this working position as the value to be published
         * in order to inform the writer of this new free space.
         */
        final PaddedLong workingTailPos; //no need for CAS since only one thread will ever use this.
        
        /**
         * This is the official published tail position. It is written to by the consuming thread and frequently polled by the producing thread.
         * Making use of the built in CAS features of AtomicLong forms a memory gate that enables this lock free implementation to function.
         */
        final AtomicLong tailPos;

        /**
         * Holds the active tail position information
         */
        StructuredLayoutRingTail() {
            this.workingTailPos = new PaddedLong();
            this.tailPos = new PaddedAtomicLong();
        }

        /**
         * Switch the working back to the published tail position.
         * Only used by replay feature, not for general use.
         */
		long rollBackWorking() {
			return workingTailPos.value = tailPos.get();
		}
    }


/**
 * Spinning on a CAS AtomicLong leads to a lot of contention which will decrease performance.
 * Once we know that the producer can write up to a given position there  is no need to keep polling until we have written up to that point.
 * This class hold the head value until that position is reached.
 */
    static class LowLevelAPIWritePositionCache {
        /**
         * This is the position the producer is allowed to write up to before having to ask the CAS AtomicLong again for a new value. 
         */
        long llwHeadPosCache;
 
        /**
         * This holds the last position that has been officially written.  The Low Level API uses the size of the next fragment
         * added to this value to determine if the next write will need to go past the cached head position above.
         * 
         * Once we know that the write will fit this value is incremented by the size to confirm the write.  This is independent 
         * of the workingHeadPosition by design so we have two accounting mechanisms to help detected errors.
         * 
         * TODO: AA add asserts that implemented the above claim.
         */
        long llwConfirmedWrittenPosition;

        LowLevelAPIWritePositionCache() {
        }
    }
    
//cas: see above ;-\
    static class LowLevelAPIReadPositionCache {
        long llrTailPosCache;
        long llwConfirmedReadPosition;

        LowLevelAPIReadPositionCache() {
        }
    }

    static class UnstructuredLayoutRingHead {
        final PaddedInt byteWorkingHeadPos;
        final PaddedInt bytesHeadPos;

        UnstructuredLayoutRingHead() {
            this.byteWorkingHeadPos = new PaddedInt();
            this.bytesHeadPos = new PaddedInt();
        }
    }

    static class UnstructuredLayoutRingTail {
        final PaddedInt byteWorkingTailPos;
        final PaddedInt bytesTailPos;

        UnstructuredLayoutRingTail() {
            this.byteWorkingTailPos = new PaddedInt();
            this.bytesTailPos = new PaddedInt();
        }

        /**
         * Switch the working back to the published tail position.
         * Only used by replay feature, not for general use.
         */
		int rollBackWorking() {
			return byteWorkingTailPos.value = bytesTailPos.value;
		}
    }

    public static class PaddedLong {
        //provided that there are no other members of this object all these primitives will be next to one another in memory.        
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;

        //small static method will be frequently in-lined allowing direct access to the member without method overhead
        public static long get(PaddedLong pi) {
            return pi.value;
        }

        public static void set(PaddedLong pi, long value) {
            pi.value = value;
        }

        public static long add(PaddedLong pi, long inc) {
                return pi.value += inc;
        }

        public String toString() {
            return Long.toString(value);
        }

    }

    public static class PaddedInt {
        //most platforms have 64 byte cache lines so this is padded to consume 16 4 byte ints
        //if a platform has smaller cache lines this will use a little more memory than required but the performance will still be preserved.
        //modern Intel and AMD chips commonly have 64 byte cache lines
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7, padding8, padding9, padding10, padding11, padding13, padding14, padding15, padding16;

		public static int get(PaddedInt pi) {
	            return pi.value;
	    }

		public static void set(PaddedInt pi, int value) {
		    pi.value = value;
		}

	    public static int add(PaddedInt pi, int inc) {
	            return pi.value += inc;
	    }

	    public static int maskedAdd(PaddedInt pi, int inc, int wrapMask) {
               return pi.value = wrapMask & (inc + pi.value);
        }

		public String toString() {
		    return Integer.toString(value);
		}
    }

    private static final Logger log = LoggerFactory.getLogger(RingBuffer.class);
    
    //I would like to follow the convention where all caps constants are used to indicate static final values which are resolved at compile time.
    //This is distinct from other static finals which hold run time instances and values computed from runtime input.
    //The reason for this distinction is that these members have special properties.
    //    A) the literal value replaces the variable by the compiler so..   a change of value requires a recompile of all dependent jars.
    //    B) these are the only variables which are allowed as case values in switch statements.
   
    //This mask is used to filter the meta value used for variable length fields.
    //after applying this mask to meta the result is always the relative offset within the byte buffer of where the variable length data starts.
    //NOTE: when the high bit is set we will not pull the value from the ring buffer but instead use the constants array (these are pronouns)
    public static final int RELATIVE_POS_MASK = 0x7FFFFFFF; //removes high bit which indicates this is a constant
    
    private static final AtomicInteger ringCounter = new AtomicInteger();

    //This mask is here to support the fact that variable length fields will run out of space because the head/tail are 32 bit ints instead of
    //longs that are used for the structured layout data.  This mask enables the int to wrap back down to zero instead of going negative.
    //this will only happen once for every 2GB written.
    public static final int BYTES_WRAP_MASK = 0x7FFFFFFF;//NOTE: this trick only works because its 1 bit less than the roll-over sign bit
        
    //A few corner use cases require a poison pill EOF message to be sent down the pipes to ensure each consumer knows when to shut down.
    //This is here for compatibility with legacy APIs,  This constant is the size of the EOF message.
    public static final int EOF_SIZE = 2;

    //these public fields are fine because they are all final
    public final int ringId;
    public final int sizeOfStructuredLayoutRingBuffer;
    public final int sizeOfUntructuredLayoutRingBuffer;
    public final int mask;
    public final int byteMask;
    public final byte bitsOfStructuredLayoutRingBuffer;
    public final byte bitsOfUntructuredLayoutRingBuffer;
    public final int maxAvgVarLen;


    //TODO: AAAA, need to add constant for gap always kept after head and before tail, this is for debug mode to store old state upon error. NEW FEATURE.
    //            the time slices of the graph will need to be kept for all rings to reconstruct history later.


    private final StructuredLayoutRingHead structuredLayoutRingBufferHead = new StructuredLayoutRingHead();
    private final UnstructuredLayoutRingHead unstructuredLayoutRingBufferHead = new UnstructuredLayoutRingHead();

    LowLevelAPIWritePositionCache llWrite; //low level write head pos cache and target
    LowLevelAPIReadPositionCache llRead; //low level read tail pos cache and target

    final RingWalker ringWalker;

    private final StructuredLayoutRingTail primaryBufferTail = new StructuredLayoutRingTail(); //primary working and public
    private final UnstructuredLayoutRingTail byteBufferTail = new UnstructuredLayoutRingTail(); //primary working and public

    //these values are only modified and used when replay is NOT in use
    //hold the publish position when batching so the batch can be flushed upon shutdown and thread context switches
    private int lastReleasedBytesTail;
    private long lastReleasedTail;

    private int bytesWriteLastConsumedBytePos = 0;

    //All references found in the messages/fragments to variable length content are relative.  These members hold the current
    //base offset to which the relative value is added to find the absolute position in the ring.
    //These values are only updated as each fragment is consumed or produced.
    private int unstructuredLayoutWriteBase = 0;
    private int unstructuredLayoutReadBase = 0;

    //Non Uniform Memory Architectures (NUMA) are supported by the Java Virtual Machine(JVM)
    //However there are some limitations.
    //   A) NUMA support must be enabled with the command line argument
    //   B) The heap space must be allocated by the same thread which expects to use it long term.
    //
    // As a result of the above the construction of the buffers is postponed and done with an initBuffers() method.
    // The initBuffers() method will be called by the consuming thread before the pipe is used. (see Pronghorn)
    private byte[] unstructuredLayoutRingBuffer;
    private int[] structuredLayoutRingBuffer;
    //defined externally and never changes
    protected final byte[] unstructuredLayoutConstBuffer;
    private byte[][] bufferLookup;
    //NOTE:
    //     This is the future direction of the ring buffer which is not yet complete
    //     By migrating all array index usages to these the backing ring can be moved outside the Java heap
    //     By moving the ring outside the Java heap other applications have have direct access
    //     The Overhead of the poly method call is what has prevented this change

    private IntBuffer wrappedStructuredLayoutRingBuffer;
    private ByteBuffer wrappedUnstructuredLayoutRingBufferA;
    private ByteBuffer wrappedUnstructuredLayoutRingBufferB;
    private ByteBuffer wrappedUnstructuredLayoutConstBuffer;

    //for writes validates that bytes of var length field is within the expected bounds.
    private int varLenMovingAverage = 0;//this is an exponential moving average

    static final int JUMP_MASK = 0xFFFFF;

    //Exceptions must not occur within consumers/producers of rings however when they do we no longer have 
    //a clean understanding of state. To resolve the problem all producers and consumers must also shutdown.
    //This flag passes the signal so any producer/consumer that sees it on knows to shut down and pass on the flag.
    private final AtomicBoolean imperativeShutDown = new AtomicBoolean(false);
    private RingBufferException firstShutdownCaller = null;


	//hold the batch positions, when the number reaches zero the records are send or released
	private int batchReleaseCountDown = 0;
	private int batchReleaseCountDownInit = 0;
	private int batchPublishCountDown = 0;
	private int batchPublishCountDownInit = 0;
	//cas: jdoc -- This is the first mention of batch(ing).  It would really help the maintainer's comprehension of what
	// you mean if you would explain this hugely overloaded word somewhere prior to use -- probably in the class's javadoc.
	    //hold the publish position when batching so the batch can be flushed upon shutdown and thread context switches
    private int lastPublishedUnstructuredLayoutRingBufferHead;
    private long lastPublishedStructuredLayoutRingBufferHead;
	    
	private final int debugFlags;

	private long holdingPrimaryWorkingTail;
	private int  holdingBytesWorkingTail;
	private int holdingBytesReadBase;


	public static void replayUnReleased(RingBuffer ringBuffer) {
	    
//We must enforce this but we have a few unit tests that are in violation which need to be fixed first	    
//	    if (!RingBuffer.from(ringBuffer).hasSimpleMessagesOnly) {
//	        throw new UnsupportedOperationException("replay of unreleased messages is not supported unless every message is also a single fragment.");
//	    }
	    
		if (!isReplaying(ringBuffer)) {
			//save all working values only once if we re-enter replaying multiple times.			
			
		    ringBuffer.holdingPrimaryWorkingTail = RingBuffer.getWorkingTailPosition(ringBuffer);			
			ringBuffer.holdingBytesWorkingTail = RingBuffer.bytesWorkingTailPosition(ringBuffer);			
						
			//NOTE: we must never adjust the ringWalker.nextWorkingHead because this is replay and must not modify write position!
			ringBuffer.ringWalker.holdingNextWorkingTail = ringBuffer.ringWalker.nextWorkingTail; 
			ringBuffer.ringWalker.holdingNextWorkingHead = ringBuffer.ringWalker.nextWorkingHead; //Should not change, this is saved for validation that it did not change during replay. 
			
			ringBuffer.holdingBytesReadBase = ringBuffer.unstructuredLayoutReadBase;
			
		}
		
		//clears the stack and cursor position back to -1 so we assume that the next read will begin a new message
		RingWalker.resetCursorState(ringBuffer.ringWalker);
		
		//set new position values for high and low api
		ringBuffer.ringWalker.nextWorkingTail = ringBuffer.primaryBufferTail.rollBackWorking();
		ringBuffer.unstructuredLayoutReadBase = ringBuffer.byteBufferTail.rollBackWorking(); //this byte position is used by both high and low api
		
		
	}

	public static boolean isReplaying(RingBuffer ringBuffer) {
		return RingBuffer.getWorkingTailPosition(ringBuffer)<ringBuffer.holdingPrimaryWorkingTail;
	}

	public static void cancelReplay(RingBuffer ringBuffer) {
		ringBuffer.primaryBufferTail.workingTailPos.value = ringBuffer.holdingPrimaryWorkingTail;
		ringBuffer.byteBufferTail.byteWorkingTailPos.value = ringBuffer.holdingBytesWorkingTail;
		
		ringBuffer.unstructuredLayoutReadBase = ringBuffer.holdingBytesReadBase;
		
		ringBuffer.ringWalker.nextWorkingTail = ringBuffer.ringWalker.holdingNextWorkingTail ;
		assert(ringBuffer.ringWalker.holdingNextWorkingHead == ringBuffer.ringWalker.nextWorkingHead);
	}

	////
	////
	public static void batchAllReleases(RingBuffer rb) {
	       rb.batchReleaseCountDownInit = Integer.MAX_VALUE;
	       rb.batchReleaseCountDown = Integer.MAX_VALUE;
	}
	

    public static void setReleaseBatchSize(RingBuffer rb, int size) {

    	validateBatchSize(rb, size);

    	rb.batchReleaseCountDownInit = size;
    	rb.batchReleaseCountDown = size;
    }

    public static void setPublishBatchSize(RingBuffer rb, int size) {

    	validateBatchSize(rb, size);

    	rb.batchPublishCountDownInit = size;
    	rb.batchPublishCountDown = size;
    }

    public static void setMaxPublishBatchSize(RingBuffer rb) {

    	int size = computeMaxBatchSize(rb, 3);

    	rb.batchPublishCountDownInit = size;
    	rb.batchPublishCountDown = size;

    }

    public static void setMaxReleaseBatchSize(RingBuffer rb) {

    	int size = computeMaxBatchSize(rb, 3);
    	rb.batchReleaseCountDownInit = size;
    	rb.batchReleaseCountDown = size;

    }


//cas: naming -- a couple of things, neither new.  Obviously the name of the buffer, bytes.  Also the use of base in
// the variable buffer, but not in the fixed.  Otoh, by now, maybe the interested reader would already understand.
    public static int bytesWriteBase(RingBuffer rb) {
    	return rb.unstructuredLayoutWriteBase;
    }

    public static void markBytesWriteBase(RingBuffer rb) {
    	rb.unstructuredLayoutWriteBase = rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
    }

    public static int bytesReadBase(RingBuffer rb) {
    	return rb.unstructuredLayoutReadBase;
    }

    public static void markBytesReadBase(RingBuffer rb) {
    	//this assert is not quite right because we may have string fields of zero length, TODO: add check for this before restoring the assert.
     	//assert(0==from(rb).maxVarFieldPerUnit || rb.byteWorkingTailPos.value != rb.bytesReadBase) : "byteWorkingTailPos should have moved forward";
    	rb.unstructuredLayoutReadBase = rb.byteBufferTail.byteWorkingTailPos.value;
    }

    /**
     * Helpful user readable summary of the ring buffer.
     * Shows where the head and tail positions are along with how full the ring is at the time of call.
     */
    public String toString() {

    	StringBuilder result = new StringBuilder();
    	result.append("RingId:").append(ringId);
    	result.append(" tailPos ").append(primaryBufferTail.tailPos.get());
    	result.append(" wrkTailPos ").append(primaryBufferTail.workingTailPos.value);
    	result.append(" headPos ").append(structuredLayoutRingBufferHead.headPos.get());
    	result.append(" wrkHeadPos ").append(structuredLayoutRingBufferHead.workingHeadPos.value);
    	result.append("  ").append(structuredLayoutRingBufferHead.headPos.get()-primaryBufferTail.tailPos.get()).append("/").append(sizeOfStructuredLayoutRingBuffer);
    	result.append("  bytesTailPos ").append(PaddedInt.get(byteBufferTail.bytesTailPos));
    	result.append(" bytesWrkTailPos ").append(byteBufferTail.byteWorkingTailPos.value);
    	result.append(" bytesHeadPos ").append(PaddedInt.get(unstructuredLayoutRingBufferHead.bytesHeadPos));
    	result.append(" bytesWrkHeadPos ").append(unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);

    	return result.toString();
    }


    /**
     * Return the configuration used for this ring buffer, Helpful when we need to make clones of the ring which will hold same message types.
     * @return
     */
    public RingBufferConfig config() { //TODO: AAA, this creates garbage and we should just hold the config object instead of copying the values out.  Then return the same instance here.
        return new RingBufferConfig(bitsOfStructuredLayoutRingBuffer,bitsOfUntructuredLayoutRingBuffer,unstructuredLayoutConstBuffer,ringWalker.from);
    }


// cas: comment -- sort of curious to have a constructor way down here.
    public RingBuffer(RingBufferConfig config) {

    	byte primaryBits = config.primaryBits;
    	byte byteBits = config.byteBits;
    	byte[] byteConstants = config.byteConst;


    	debugFlags = config.debugFlags;

        //Assign the immutable universal id value for this specific instance
    	//these values are required to keep track of all ring buffers when graphs are built
        this.ringId = ringCounter.getAndIncrement();

    	this.bitsOfStructuredLayoutRingBuffer = primaryBits;
    	this.bitsOfUntructuredLayoutRingBuffer = byteBits;

        assert (primaryBits >= 0); //zero is a special case for a mock ring

//cas: naming.  This should be consistent with the maxByteSize, i.e., maxFixedSize or whatever.
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.sizeOfStructuredLayoutRingBuffer = 1 << primaryBits;
        this.mask = sizeOfStructuredLayoutRingBuffer - 1;

        //single text and byte buffers because this is where the variable length data will go.

        this.sizeOfUntructuredLayoutRingBuffer =  1 << byteBits;
        this.byteMask = sizeOfUntructuredLayoutRingBuffer - 1;

        FieldReferenceOffsetManager from = config.from;
        this.ringWalker = new RingWalker(from);
        this.unstructuredLayoutConstBuffer = byteConstants;


        if (0 == from.maxVarFieldPerUnit || 0==primaryBits) { //zero bits is for the dummy mock case
        	maxAvgVarLen = 0; //no fragments had any variable length fields so we never allow any
        } else {
        	//given outer ring buffer this is the maximum number of var fields that can exist at the same time.
        	int mx = sizeOfStructuredLayoutRingBuffer;
        	int maxVarCount = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, mx);
        	//to allow more almost 2x more flexibility in variable length bytes we track pairs of writes and ensure the
        	//two together are below the threshold rather than each alone
        	maxAvgVarLen = sizeOfUntructuredLayoutRingBuffer/maxVarCount;
        }
    }

    public static int totalRings() {
        return ringCounter.get();
    }

	public RingBuffer initBuffers() {
		assert(!isInit(this)) : "RingBuffer was already initialized";
		if (!isInit(this)) {
			buildBuffers();
		} else {
			log.warn("Init was already called once already on this ring buffer");
		}
		return this;
    }

//cas: LEFT OFF HERE.
	private void buildBuffers() {

        assert(structuredLayoutRingBufferHead.workingHeadPos.value == structuredLayoutRingBufferHead.headPos.get());
        assert(primaryBufferTail.workingTailPos.value == primaryBufferTail.tailPos.get());
        assert(structuredLayoutRingBufferHead.workingHeadPos.value == primaryBufferTail.workingTailPos.value);
        assert(primaryBufferTail.tailPos.get()==structuredLayoutRingBufferHead.headPos.get());

        long toPos = structuredLayoutRingBufferHead.workingHeadPos.value;//can use this now that we have confirmed they all match.

        this.llRead = new LowLevelAPIReadPositionCache();
        this.llWrite = new LowLevelAPIWritePositionCache();

        // cas: comment.  If it really, truly must be the same, then a common routine should be
        // extracted (unless you can call reset here).
        //This init must be the same as what is done in reset()
        //This target is a counter that marks if there is room to write more data into the ring without overwriting other data.
        this.llRead.llwConfirmedReadPosition = 0-this.sizeOfStructuredLayoutRingBuffer;
        llWrite.llwHeadPosCache = toPos;
        llRead.llrTailPosCache = toPos;
        llRead.llwConfirmedReadPosition = toPos - sizeOfStructuredLayoutRingBuffer;
        llWrite.llwConfirmedWrittenPosition = toPos;

        this.unstructuredLayoutRingBuffer = new byte[sizeOfUntructuredLayoutRingBuffer];
        this.structuredLayoutRingBuffer = new int[sizeOfStructuredLayoutRingBuffer];
        this.bufferLookup = new byte[][] {unstructuredLayoutRingBuffer,unstructuredLayoutConstBuffer};

        this.wrappedStructuredLayoutRingBuffer = IntBuffer.wrap(this.structuredLayoutRingBuffer);
        this.wrappedUnstructuredLayoutRingBufferA = ByteBuffer.wrap(this.unstructuredLayoutRingBuffer);
        this.wrappedUnstructuredLayoutRingBufferB = ByteBuffer.wrap(this.unstructuredLayoutRingBuffer);
        this.wrappedUnstructuredLayoutConstBuffer = null==this.unstructuredLayoutConstBuffer?null:ByteBuffer.wrap(this.unstructuredLayoutConstBuffer);

        assert(0==wrappedUnstructuredLayoutRingBufferA.position() && wrappedUnstructuredLayoutRingBufferA.capacity()==wrappedUnstructuredLayoutRingBufferA.limit()) : "The ByteBuffer is not clear.";

	}

	public static boolean isInit(RingBuffer ring) {
	    //Due to the fact that no locks are used it becomes necessary to check
	    //every single field to ensure the full initialization of the object
	    //this is done as part of graph set up and as such is called rarely.
		return null!=ring.unstructuredLayoutRingBuffer &&
			   null!=ring.structuredLayoutRingBuffer &&
			   null!=ring.bufferLookup &&
			   null!=ring.wrappedStructuredLayoutRingBuffer &&
			   null!=ring.wrappedUnstructuredLayoutRingBufferA &&
			   null!=ring.llRead &&
			   null!=ring.llWrite;
	}

	public static void validateVarLength(RingBuffer rb, int length) {
		int newAvg = (length+rb.varLenMovingAverage)>>1;
        if (newAvg>rb.maxAvgVarLen)	{
            //compute some helpful information to add to the exception
        	int bytesPerInt = (int)Math.ceil(length*RingBuffer.from(rb).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);

        	throw new UnsupportedOperationException("Can not write byte array of length "+length+". The dif between primary and byte bits should be at least "+bitsDif+". "+rb.bitsOfStructuredLayoutRingBuffer+","+rb.bitsOfUntructuredLayoutRingBuffer);
        }
        rb.varLenMovingAverage = newAvg;
	}



    /**
     * Empty and restore to original values.
     */
    public void reset() {
    	reset(0,0);
    }

    /**
     * Rest to desired position, helpful in unit testing to force wrap off the end.
     * @param toPos
     */
    public void reset(int toPos, int bPos) {

    	structuredLayoutRingBufferHead.workingHeadPos.value = toPos;
        primaryBufferTail.workingTailPos.value = toPos;
        primaryBufferTail.tailPos.set(toPos);
        structuredLayoutRingBufferHead.headPos.set(toPos);

        if (null!=llWrite) {
            llWrite.llwHeadPosCache = toPos;
            llRead.llrTailPosCache = toPos;
            llRead.llwConfirmedReadPosition = toPos - sizeOfStructuredLayoutRingBuffer;
            llWrite.llwConfirmedWrittenPosition = toPos;
        }

        unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = bPos;
        PaddedInt.set(unstructuredLayoutRingBufferHead.bytesHeadPos,bPos);

        unstructuredLayoutWriteBase = bPos;
        unstructuredLayoutReadBase = bPos;
        bytesWriteLastConsumedBytePos = bPos;

        byteBufferTail.byteWorkingTailPos.value = bPos;
        PaddedInt.set(byteBufferTail.bytesTailPos,bPos);
        RingWalker.reset(ringWalker, toPos);
    }

    public static void appendFragment(RingBuffer input, Appendable target, int cursor) {
        try {

            FieldReferenceOffsetManager from = from(input);
            int fields = from.fragScriptSize[cursor];
            assert (cursor<from.tokensLen-1);//there are no single token messages so there is no room at the last position.


            int dataSize = from.fragDataSize[cursor];
            String msgName = from.fieldNameScript[cursor];
            long msgId = from.fieldIdScript[cursor];

            target.append(" cursor:"+cursor+
                           " fields: "+fields+" "+String.valueOf(msgName)+
                           " id: "+msgId).append("\n");

            if (0==fields && cursor==from.tokensLen-1) { //this is an odd case and should not happen
                //TODO: AA length is too long and we need to detect cursor out of bounds!
                System.err.println("total tokens:"+from.tokens.length);//Arrays.toString(from.fieldNameScript));
                System.exit(-1);
            }


            int i = 0;
            while (i<fields) {
                final int p = i+cursor;
                String name = from.fieldNameScript[p];
                long id = from.fieldIdScript[p];

                int token = from.tokens[p];
                int type = TokenBuilder.extractType(token);

                //fields not message name
                String value = "";
                if (i>0 || !input.ringWalker.isNewMessage) {
                    int pos = from.fragDataSize[i+cursor];
                    //create string values of each field so we can see them easily
                    switch (type) {
                        case TypeMask.Group:

                            int oper = TokenBuilder.extractOper(token);
                            boolean open = (0==(OperatorMask.Group_Bit_Close&oper));
                            value = "open:"+open+" pos:"+p;

                            break;
                        case TypeMask.GroupLength:
                            int len = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                            value = Integer.toHexString(len)+"("+len+")";
                            break;
                        case TypeMask.IntegerSigned:
                        case TypeMask.IntegerUnsigned:
                        case TypeMask.IntegerSignedOptional:
                        case TypeMask.IntegerUnsignedOptional:
                            int readInt = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                            value = Integer.toHexString(readInt)+"("+readInt+")";
                            break;
                        case TypeMask.LongSigned:
                        case TypeMask.LongUnsigned:
                        case TypeMask.LongSignedOptional:
                        case TypeMask.LongUnsignedOptional:
                            long readLong = readLong(primaryBuffer(input), input.mask, pos+tailPosition(input));
                            value = Long.toHexString(readLong)+"("+readLong+")";
                            break;
                        case TypeMask.Decimal:
                        case TypeMask.DecimalOptional:

                            int exp = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                            long mantissa = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input)+1);
                            value = exp+" "+mantissa;

                            break;
                        case TypeMask.TextASCII:
                        case TypeMask.TextASCIIOptional:

                            {
                                int meta = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                                int length = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input)+1);
                                readASCII(input, target, meta, length);
                                value = meta+" len:"+length;
                                // value = target.toString();
                            }
                            break;
                        case TypeMask.TextUTF8:
                        case TypeMask.TextUTF8Optional:

                            {
                                int meta = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                                int length = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input)+1);
                                readUTF8(input, target, meta, length);
                                value = meta+" len:"+length;
                               // value = target.toString();
                            }
                            break;
                        case TypeMask.ByteArray:
                        case TypeMask.ByteArrayOptional:
                            {
                                int meta = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input));
                                int length = readInt(primaryBuffer(input), input.mask, pos+tailPosition(input)+1);
                                value = meta+" len:"+length;

                            }
                            break;
                        default: target.append("unknown ").append("\n");

                    }


                    value += (" "+TypeMask.toString(type)+" "+pos);
                }

                target.append("   "+name+":"+id+"  "+value).append("\n");

                //TWEET  x+t+"xxx" is a bad idea.


                if (TypeMask.Decimal==type || TypeMask.DecimalOptional==type) {
                    i++;//skip second slot for decimals
                }

                i++;
            }
        } catch (IOException ioe) {
            RingReader.log.error("Unable to build text for fragment.",ioe);
            throw new RuntimeException(ioe);
        }
    }

    public static ByteBuffer readBytes(RingBuffer ring, ByteBuffer target, int meta, int len) {
		if (meta < 0) {
	        return readBytesConst(ring,len,target,RingReader.POS_CONST_MASK & meta);
	    } else {
	        return readBytesRing(ring,len,target,restorePosition(ring,meta));
	    }
	}

    public static void readBytes(RingBuffer ring, byte[] target, int targetIdx, int targetMask, int meta, int len) {
		if (meta < 0) {
			//NOTE: constByteBuffer does not wrap so we do not need the mask
			copyBytesFromToRing(ring.unstructuredLayoutConstBuffer, RingReader.POS_CONST_MASK & meta, 0xFFFFFFFF, target, targetIdx, targetMask, len);
	    } else {
			copyBytesFromToRing(ring.unstructuredLayoutRingBuffer,restorePosition(ring,meta),ring.byteMask,target,targetIdx,targetMask,len);
	    }
	}

	private static ByteBuffer readBytesRing(RingBuffer ring, int len, ByteBuffer target, int pos) {
		int mask = ring.byteMask;
		byte[] buffer = ring.unstructuredLayoutRingBuffer;

        int tStart = pos & mask;
        int len1 = 1+mask - tStart;

		if (len1>=len) {
			target.put(buffer, mask&pos, len);
		} else {
			target.put(buffer, mask&pos, len1);
			target.put(buffer, 0, len-len1);
		}

	    return target;
	}

	private static ByteBuffer readBytesConst(RingBuffer ring, int len, ByteBuffer target, int pos) {
	    	target.put(ring.unstructuredLayoutConstBuffer, pos, len);
	        return target;
	    }

	public static Appendable readASCII(RingBuffer ring, Appendable target,	int meta, int len) {
		if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
	        return readASCIIConst(ring,len,target,RingReader.POS_CONST_MASK & meta);
	    } else {
	        return readASCIIRing(ring,len,target,restorePosition(ring, meta));
	    }
	}

	public static boolean isEqual(RingBuffer ring, CharSequence charSeq, int meta, int len) {
		if (len!=charSeq.length()) {
			return false;
		}
		if (meta < 0) {

			int pos = RingReader.POS_CONST_MASK & meta;

	    	byte[] buffer = ring.unstructuredLayoutConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	    		if (charSeq.charAt(len)!=buffer[pos+len]) {
	    			return false;
	    		}
	        }

		} else {

			byte[] buffer = ring.unstructuredLayoutRingBuffer;
			int mask = ring.byteMask;
			int pos = restorePosition(ring, meta);

	        while (--len >= 0) {
	    		if (charSeq.charAt(len)!=buffer[mask&(pos+len)]) {
	    			return false;
	    		}
	        }

		}

		return true;
	}

	private static Appendable readASCIIRing(RingBuffer ring, int len, Appendable target, int pos) {
		byte[] buffer = ring.unstructuredLayoutRingBuffer;
		int mask = ring.byteMask;

	    try {
	        while (--len >= 0) {
	            target.append((char)buffer[mask & pos++]);
	        }
	    } catch (IOException e) {
	       throw new RuntimeException(e);
	    }
	    return target;
	}

	private static Appendable readASCIIConst(RingBuffer ring, int len, Appendable target, int pos) {
	    try {
	    	byte[] buffer = ring.unstructuredLayoutConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	            target.append((char)buffer[pos++]);
	        }
	    } catch (IOException e) {
	       throw new RuntimeException(e);
	    }
	    return target;
	}

	public static Appendable readUTF8(RingBuffer ring, Appendable target, int meta, int len) {
		if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
	        return readUTF8Const(ring,len,target,RingReader.POS_CONST_MASK & meta);
	    } else {
	        return readUTF8Ring(ring,len,target,restorePosition(ring,meta));
	    }
	}

	private static Appendable readUTF8Const(RingBuffer ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(ring.unstructuredLayoutConstBuffer, charAndPos, 0xFFFFFFFF); //constants do not wrap
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

	private static Appendable readUTF8Ring(RingBuffer ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(ring.unstructuredLayoutRingBuffer, charAndPos, ring.byteMask);
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

	public static void addDecimalAsASCII(int readDecimalExponent,	long readDecimalMantissa, RingBuffer outputRing) {
		long ones = (long)(readDecimalMantissa*RingReader.powdi[64 + readDecimalExponent]);
		validateVarLength(outputRing, 21);
		int max = 21 + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		int len = leftConvertLongToASCII(outputRing, ones, max);
		outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(len + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);

		copyASCIIToBytes(".", outputRing);

		long frac = Math.abs(readDecimalMantissa - (long)(ones/RingReader.powdi[64 + readDecimalExponent]));

		validateVarLength(outputRing, 21);
		int max1 = 21 + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		int len1 = leftConvertLongWithLeadingZerosToASCII(outputRing, readDecimalExponent, frac, max1);
		outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = RingBuffer.BYTES_WRAP_MASK&(len1 + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);

		//may require trailing zeros
		while (len1<readDecimalExponent) {
			copyASCIIToBytes("0",outputRing);
			len1++;
		}


	}

	public static void addLongAsASCII(RingBuffer outputRing, long value) {
		validateVarLength(outputRing, 21);
		int max = 21 + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		int len = leftConvertLongToASCII(outputRing, value, max);
		addBytePosAndLen(outputRing, outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, len);
		outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(len + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);
	}

	public static void addIntAsASCII(RingBuffer outputRing, int value) {
		validateVarLength(outputRing, 12);
		int max = 12 + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		int len = leftConvertIntToASCII(outputRing, value, max);
		addBytePosAndLen(outputRing, outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, len);
		outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = RingBuffer.BYTES_WRAP_MASK&(len + outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);
	}

	/**
     * All bytes even those not yet committed.
     *
     * @param ringBuffer
     * @return
     */
	public static int bytesOfContent(RingBuffer ringBuffer) {
		int dif = (ringBuffer.byteMask&ringBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value) - (ringBuffer.byteMask&PaddedInt.get(ringBuffer.byteBufferTail.bytesTailPos));
		return ((dif>>31)<<ringBuffer.bitsOfUntructuredLayoutRingBuffer)+dif;
	}

	public static void validateBatchSize(RingBuffer rb, int size) {
		int maxBatch = computeMaxBatchSize(rb);
		if (size>maxBatch) {
			throw new UnsupportedOperationException("For the configured ring buffer the batch size can be no larger than "+maxBatch);
		}
	}

	public static int computeMaxBatchSize(RingBuffer rb) {
		return computeMaxBatchSize(rb,2);//default mustFit of 2
	}

	public static int computeMaxBatchSize(RingBuffer rb, int mustFit) {
		assert(mustFit>=1);
		int maxBatchFromBytes = rb.maxAvgVarLen==0?Integer.MAX_VALUE:(rb.sizeOfUntructuredLayoutRingBuffer/rb.maxAvgVarLen)/mustFit;
		int maxBatchFromPrimary = (rb.sizeOfStructuredLayoutRingBuffer/FieldReferenceOffsetManager.maxFragmentSize(from(rb)))/mustFit;
		return Math.min(maxBatchFromBytes, maxBatchFromPrimary);
	}

	@Deprecated
	public static void publishEOF(RingBuffer ring) {

		assert(ring.primaryBufferTail.tailPos.get()+ring.sizeOfStructuredLayoutRingBuffer>=ring.structuredLayoutRingBufferHead.headPos.get()+RingBuffer.EOF_SIZE) : "Must block first to ensure we have 2 spots for the EOF marker";

		PaddedInt.set(ring.unstructuredLayoutRingBufferHead.bytesHeadPos,ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);
		ring.structuredLayoutRingBuffer[ring.mask &((int)ring.structuredLayoutRingBufferHead.workingHeadPos.value +  from(ring).templateOffset)]    = -1;
		ring.structuredLayoutRingBuffer[ring.mask &((int)ring.structuredLayoutRingBufferHead.workingHeadPos.value +1 +  from(ring).templateOffset)] = 0;

		ring.structuredLayoutRingBufferHead.headPos.lazySet(ring.structuredLayoutRingBufferHead.workingHeadPos.value = ring.structuredLayoutRingBufferHead.workingHeadPos.value + RingBuffer.EOF_SIZE);

	}

	public static void copyBytesFromToRing(byte[] source, int sourceloc, int sourceMask, byte[] target, int targetloc, int targetMask, int length) {
		copyBytesFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask,	length);
	}

	public static void copyIntsFromToRing(int[] source, int sourceloc, int sourceMask, int[] target, int targetloc, int targetMask, int length) {
		copyIntsFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask, length);
	}


	private static void copyBytesFromToRingMasked(byte[] source,
			final int rStart, final int rStop, byte[] target, final int tStart,
			final int tStop, int length) {
		if (tStop > tStart) {
			//do not accept the equals case because this can not work with data the same length as as the buffer
			doubleMaskTargetDoesNotWrap(source, rStart, rStop, target, tStart, length);
		} else {
			doubleMaskTargetWraps(source, rStart, rStop, target, tStart, tStop,	length);
		}
	}


	private static void copyIntsFromToRingMasked(int[] source,
			final int rStart, final int rStop, int[] target, final int tStart,
			final int tStop, int length) {
		if (tStop > tStart) {
			doubleMaskTargetDoesNotWrap(source, rStart, rStop, target, tStart, length);
		} else {
			doubleMaskTargetWraps(source, rStart, rStop, target, tStart, tStop,	length);
		}
	}

	private static void doubleMaskTargetDoesNotWrap(byte[] source,
			final int srcStart, final int srcStop, byte[] target, final int trgStart,	int length) {
		if (srcStop >= srcStart) {
			//the source and target do not wrap
			System.arraycopy(source, srcStart, target, trgStart, length);
		} else {
			//the source is wrapping but not the target
			System.arraycopy(source, srcStart, target, trgStart, length-srcStop);
			System.arraycopy(source, 0, target, trgStart + length - srcStop, srcStop);
		}
	}

	private static void doubleMaskTargetDoesNotWrap(int[] source,
			final int rStart, final int rStop, int[] target, final int tStart,
			int length) {
		if (rStop > rStart) {
			//the source and target do not wrap
			System.arraycopy(source, rStart, target, tStart, length);
		} else {
			//the source is wrapping but not the target
			System.arraycopy(source, rStart, target, tStart, length-rStop);
			System.arraycopy(source, 0, target, tStart + length - rStop, rStop);
		}
	}

	private static void doubleMaskTargetWraps(byte[] source, final int rStart,
			final int rStop, byte[] target, final int tStart, final int tStop,
			int length) {
		if (rStop > rStart) {
//				//the source does not wrap but the target does
//				// done as two copies
		    System.arraycopy(source, rStart, target, tStart, length-tStop);
		    System.arraycopy(source, rStart + length - tStop, target, 0, tStop);
		} else {
		    if (length>0) {
				//both the target and the source wrap
		    	doubleMaskDoubleWrap(source, target, length, tStart, rStart, length-tStop, length-rStop);
			}
		}
	}

	private static void doubleMaskTargetWraps(int[] source, final int rStart,
			final int rStop, int[] target, final int tStart, final int tStop,
			int length) {
		if (rStop > rStart) {
//				//the source does not wrap but the target does
//				// done as two copies
		    System.arraycopy(source, rStart, target, tStart, length-tStop);
		    System.arraycopy(source, rStart + length - tStop, target, 0, tStop);
		} else {
		    if (length>0) {
				//both the target and the source wrap
		    	doubleMaskDoubleWrap(source, target, length, tStart, rStart, length-tStop, length-rStop);
			}
		}
	}

	private static void doubleMaskDoubleWrap(byte[] source, byte[] target,
			int length, final int tStart, final int rStart, int targFirstLen,
			int srcFirstLen) {
		if (srcFirstLen<targFirstLen) {
			//split on src first
			System.arraycopy(source, rStart, target, tStart, srcFirstLen);
			System.arraycopy(source, 0, target, tStart+srcFirstLen, targFirstLen - srcFirstLen);
			System.arraycopy(source, targFirstLen - srcFirstLen, target, 0, length - targFirstLen);
		} else {
			//split on targ first
			System.arraycopy(source, rStart, target, tStart, targFirstLen);
			System.arraycopy(source, rStart + targFirstLen, target, 0, srcFirstLen - targFirstLen);
			System.arraycopy(source, 0, target, srcFirstLen - targFirstLen, length - srcFirstLen);
		}
	}

	private static void doubleMaskDoubleWrap(int[] source, int[] target,
			int length, final int tStart, final int rStart, int targFirstLen,
			int srcFirstLen) {
		if (srcFirstLen<targFirstLen) {
			//split on src first
			System.arraycopy(source, rStart, target, tStart, srcFirstLen);
			System.arraycopy(source, 0, target, tStart+srcFirstLen, targFirstLen - srcFirstLen);
			System.arraycopy(source, targFirstLen - srcFirstLen, target, 0, length - targFirstLen);
		} else {
			//split on targ first
			System.arraycopy(source, rStart, target, tStart, targFirstLen);
			System.arraycopy(source, rStart + targFirstLen, target, 0, srcFirstLen - targFirstLen);
			System.arraycopy(source, 0, target, srcFirstLen - targFirstLen, length - srcFirstLen);
		}
	}

	public static int leftConvertIntToASCII(RingBuffer rb, int value, int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.unstructuredLayoutRingBuffer;
		int tmp = Math.abs(value);
		int max = idx;
		do {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			int t = tmp/10;
			int r = tmp%10;
			target[rb.byteMask&--idx] = (byte)('0'+r);
			tmp = t;
		} while (0!=tmp);
		target[rb.byteMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>31));

		//shift it down to the head
		int length = max-idx;
		if (idx!=rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[rb.byteMask & (s+rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

	public static int leftConvertLongToASCII(RingBuffer rb, long value,	int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.unstructuredLayoutRingBuffer;
		long tmp = Math.abs(value);
		int max = idx;
		do {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			long t = tmp/10;
			long r = tmp%10;
			target[rb.byteMask&--idx] = (byte)('0'+r);
			tmp = t;
		} while (0!=tmp);
		target[rb.byteMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>63));

		int length = max-idx;
		//shift it down to the head
		if (idx!=rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[rb.byteMask & (s+rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

   public static int leftConvertLongWithLeadingZerosToASCII(RingBuffer rb, int chars, long value, int idx) {
        //max places is value for -2B therefore its 11 places so we start out that far and work backwards.
        //this will leave a gap but that is not a problem.
        byte[] target = rb.unstructuredLayoutRingBuffer;
        long tmp = Math.abs(value);
        int max = idx;

        do {
            //do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
            long t = tmp/10;
            long r = tmp%10;
            target[rb.byteMask&--idx] = (byte)('0'+r);
            tmp = t;
            chars--;
        } while (0!=tmp);
        while(--chars>=0) {
            target[rb.byteMask&--idx] = '0';
        }

        target[rb.byteMask& (idx-1)] = (byte)'-';
        //to make it positive we jump over the sign.
        idx -= (1&(value>>63));

        int length = max-idx;
        //shift it down to the head
        if (idx!=rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value) {
            int s = 0;
            while (s<length) {
                target[rb.byteMask & (s+rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
                s++;
            }
        }
        return length;
    }

	public static int readInt(int[] buffer, int mask, long index) {
		return buffer[mask & (int)(index)];
	}

	/**
	 * Read and return the int value at this position and clear the value with the provided clearValue.
	 * This ensures no future calls will be able to read the value once this is done.
	 * 
	 * This is primarily needed for secure data xfers when the re-use of a ring buffer may 'leak' old values.
	 * It is also useful for setting flags in conjuction with the replay feature.
	 * 
	 * @param buffer
	 * @param mask
	 * @param index
	 * @param clearValue
	 * @return
	 */
	public static int readIntSecure(int[] buffer, int mask, long index, int clearValue) {
	        int idx = mask & (int)(index);
            int result =  buffer[idx];
            buffer[idx] = clearValue;
            return result;
	}
	
	public static long readLong(int[] buffer, int mask, long index) {
		return (((long) buffer[mask & (int)index]) << 32) | (((long) buffer[mask & (int)(index + 1)]) & 0xFFFFFFFFl);
	}

	/**
	   * Convert bytes into chars using UTF-8.
	   *
	   *  High 32   BytePosition
	   *  Low  32   Char (caller can cast response to char to get the decoded value)
	   *
	   */
	  public static long decodeUTF8Fast(byte[] source, long posAndChar, int mask) { //pass in long of last position?

		  // 7  //high bit zero all others its 1
		  // 5 6
		  // 4 6 6
		  // 3 6 6 6
		  // 2 6 6 6 6
		  // 1 6 6 6 6 6

	    int sourcePos = (int)(posAndChar >> 32);

	    byte b;
	    if ((b = source[mask&sourcePos++]) >= 0) {
	        // code point 7
	        return (((long)sourcePos)<<32) | (long)b; //1 byte result of 7 bits with high zero
	    }

	    int result;
	    if (((byte) (0xFF & (b << 2))) >= 0) {
	        if ((b & 0x40) == 0) {
	            ++sourcePos;
	            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	        }
	        // code point 11
	        result = (b & 0x1F); //5 bits
	    } else {
	        if (((byte) (0xFF & (b << 3))) >= 0) {
	            // code point 16
	            result = (b & 0x0F); //4 bits
	        } else {
	            if (((byte) (0xFF & (b << 4))) >= 0) {
	                // code point 21
	                result = (b & 0x07); //3 bits
	            } else {
	                if (((byte) (0xFF & (b << 5))) >= 0) {
	                    // code point 26
	                    result = (b & 0x03); // 2 bits
	                } else {
	                    if (((byte) (0xFF & (b << 6))) >= 0) {
	                        // code point 31
	                        result = (b & 0x01); // 1 bit
	                    } else {
	                        // the high bit should never be set
	                        sourcePos += 5;
	                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                    }

	                    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                        sourcePos += 5;
	                        return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                    }
	                    result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	                }
	                if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                    sourcePos += 4;
	                    return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	                }
	                result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	            }
	            if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	                sourcePos += 3;
	                return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	            }
	            result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	        }
	        if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	            sourcePos += 2;
	            return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	        }
	        result = (result << 6) | (int)(source[mask&sourcePos++] & 0x3F);
	    }
	    if ((source[mask&sourcePos] & 0xC0) != 0x80) {
	       System.err.println("Invalid encoding, low byte must have bits of 10xxxxxx but we find "+Integer.toBinaryString(source[mask&sourcePos]));
	       sourcePos += 1;
	       return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	    }
	    long chr = ((result << 6) | (int)(source[mask&sourcePos++] & 0x3F)); //6 bits
	    return (((long)sourcePos)<<32) | chr;
	  }

	public static int copyASCIIToBytes(CharSequence source, RingBuffer rbRingBuffer) {
		return copyASCIIToBytes(source, 0, source.length(), rbRingBuffer);
	}

	public static void addASCII(CharSequence source, RingBuffer rb) {
	    addASCII(source, 0, null==source ? -1 : source.length(), rb);
	}

	public static void addASCII(CharSequence source, int sourceIdx, int sourceCharCount, RingBuffer rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static void addASCII(char[] source, int sourceIdx, int sourceCharCount, RingBuffer rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static int copyASCIIToBytes(CharSequence source, int sourceIdx, final int sourceLen, RingBuffer rbRingBuffer) {
		final int p = rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		//TODO: revisit this not sure this conditional is required
	    if (sourceLen > 0) {
	    	int tStart = p & rbRingBuffer.byteMask;
	        copyASCIIToBytes2(source, sourceIdx, sourceLen, rbRingBuffer, p, rbRingBuffer.unstructuredLayoutRingBuffer, tStart, 1+rbRingBuffer.byteMask - tStart);
	    }
		return p;
	}

	private static void copyASCIIToBytes2(CharSequence source, int sourceIdx,
			final int sourceLen, RingBuffer rbRingBuffer, final int p,
			byte[] target, int tStart, int len1) {
		if (len1>=sourceLen) {
			RingBuffer.copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
		} else {
		    // done as two copies
		    RingBuffer.copyASCIIToByte(source, sourceIdx, target, tStart, len1);
		    RingBuffer.copyASCIIToByte(source, sourceIdx + len1, target, 0, sourceLen - len1);
		}
		rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(p + sourceLen);
	}

    public static int copyASCIIToBytes(char[] source, int sourceIdx, final int sourceLen, RingBuffer rbRingBuffer) {
		final int p = rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	byte[] target = rbRingBuffer.unstructuredLayoutRingBuffer;

	        int tStart = p & targetMask;
	        int len1 = 1+targetMask - tStart;

			if (len1>=sourceLen) {
				copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    copyASCIIToByte(source, sourceIdx, target, tStart, 1+ targetMask - tStart);
			    copyASCIIToByte(source, sourceIdx + len1, target, 0, sourceLen - len1);
			}
	        rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(p + sourceLen);
	    }
		return p;
	}

	private static void copyASCIIToByte(char[] source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source[sourceIdx+i]);
		}
	}

	private static void copyASCIIToByte(CharSequence source, int sourceIdx, byte[] target, int targetIdx, int len) {
		int i = len;
		while (--i>=0) {
			target[targetIdx+i] = (byte)(0xFF&source.charAt(sourceIdx+i));
		}
	}

	public static void addUTF8(CharSequence source, RingBuffer rb) {
	    addUTF8(source, null==source? -1 : source.length(), rb);
	}

	public static void addUTF8(CharSequence source, int sourceCharCount, RingBuffer rb) {
		addBytePosAndLen(rb, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,sourceCharCount,rb));
	}

	public static void addUTF8(char[] source, int sourceCharCount, RingBuffer rb) {
		addBytePosAndLen(rb, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,sourceCharCount,rb));
	}

	/**
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
	public static int copyUTF8ToByte(CharSequence source, int sourceCharCount, RingBuffer rb) {
	    if (sourceCharCount>0) {
    		int byteLength = RingBuffer.copyUTF8ToByte(source, 0, rb.unstructuredLayoutRingBuffer, rb.byteMask, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, sourceCharCount);
    		rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value+byteLength);
    		return byteLength;
	    } else {
	        return 0;
	    }
	}

   public static int copyUTF8ToByte(CharSequence source, int sourceOffset, int sourceCharCount, RingBuffer rb) {
        if (sourceCharCount>0) {
            int byteLength = RingBuffer.copyUTF8ToByte(source, sourceOffset, rb.unstructuredLayoutRingBuffer, rb.byteMask, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, sourceCharCount);
            rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value+byteLength);
            return byteLength;
        } else {
            return 0;
        }
    }

	private static int copyUTF8ToByte(CharSequence source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {
	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source.charAt(sourceIdx+c++), target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}

	/**
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
	public static int copyUTF8ToByte(char[] source, int sourceCharCount, RingBuffer rb) {
		int byteLength = RingBuffer.copyUTF8ToByte(source, 0, rb.unstructuredLayoutRingBuffer, rb.byteMask, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, sourceCharCount);
		rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value+byteLength);
		return byteLength;
	}

	public static int copyUTF8ToByte(char[] source, int sourceOffset, int sourceCharCount, RingBuffer rb) {
	    int byteLength = RingBuffer.copyUTF8ToByte(source, sourceOffset, rb.unstructuredLayoutRingBuffer, rb.byteMask, rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, sourceCharCount);
	    rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value+byteLength);
	    return byteLength;
	}

	private static int copyUTF8ToByte(char[] source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {

	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source[sourceIdx+c++], target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}




	public static int encodeSingleChar(int c, byte[] buffer,int mask, int pos) {

	    if (c <= 0x007F) { // less than or equal to 7 bits or 127
	        // code point 7
	        buffer[mask&pos++] = (byte) c;
	    } else {
	        if (c <= 0x07FF) { // less than or equal to 11 bits or 2047
	            // code point 11
	            buffer[mask&pos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
	        } else {
	            if (c <= 0xFFFF) { // less than or equal to  16 bits or 65535

	            	//special case logic here because we know that c > 7FF and c <= FFFF so it may hit these
	            	// D800 through DFFF are reserved for UTF-16 and must be encoded as an 63 (error)
	            	if (0xD800 == (0xF800&c)) {
	            		buffer[mask&pos++] = 63;
	            		return pos;
	            	}

	                // code point 16
	                buffer[mask&pos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
	            } else {
	                pos = rareEncodeCase(c, buffer, mask, pos);
	            }
	            buffer[mask&pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
	        }
	        buffer[mask&pos++] = (byte) (0x80 | (c & 0x3F));
	    }
	    return pos;
	}

	private static int rareEncodeCase(int c, byte[] buffer, int mask, int pos) {
		if (c < 0x1FFFFF) {
		    // code point 21
		    buffer[mask&pos++] = (byte) (0xF0 | ((c >> 18) & 0x07));
		} else {
		    if (c < 0x3FFFFFF) {
		        // code point 26
		        buffer[mask&pos++] = (byte) (0xF8 | ((c >> 24) & 0x03));
		    } else {
		        if (c < 0x7FFFFFFF) {
		            // code point 31
		            buffer[mask&pos++] = (byte) (0xFC | ((c >> 30) & 0x01));
		        } else {
		            throw new UnsupportedOperationException("can not encode char with value: " + c);
		        }
		        buffer[mask&pos++] = (byte) (0x80 | ((c >> 24) & 0x3F));
		    }
		    buffer[mask&pos++] = (byte) (0x80 | ((c >> 18) & 0x3F));
		}
		buffer[mask&pos++] = (byte) (0x80 | ((c >> 12) & 0x3F));
		return pos;
	}

	public static void addByteBuffer(ByteBuffer source, RingBuffer rb) {
	    int bytePos = rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
	    int len = -1;
	    if (null!=source && source.hasRemaining()) {
	        len = source.remaining();
	        copyByteBuffer(source,source.remaining(),rb);
	    }
	    RingBuffer.addBytePosAndLen(rb, bytePos, len);
	}

	public static void copyByteBuffer(ByteBuffer source, int length, RingBuffer rb) {
		validateVarLength(rb, length);
		int idx = rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value & rb.byteMask;
		int partialLength = 1 + rb.byteMask - idx;
		//may need to wrap around ringBuffer so this may need to be two copies
		if (partialLength>=length) {
		    source.get(rb.unstructuredLayoutRingBuffer, idx, length);
		} else {
		    //read from source and write into byteBuffer
		    source.get(rb.unstructuredLayoutRingBuffer, idx, partialLength);
		    source.get(rb.unstructuredLayoutRingBuffer, 0, length - partialLength);
		}
		rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value + length);
	}

	public static void addByteArrayWithMask(final RingBuffer outputRing, int mask, int len, byte[] data, int offset) {
		validateVarLength(outputRing, len);
		copyBytesFromToRing(data,offset,mask,outputRing.unstructuredLayoutRingBuffer,outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value,outputRing.byteMask, len);
		addBytePosAndLen(outputRing, outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, len);
		outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(outputRing.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value + len);
	}

	public static int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static long peekLong(int[] buf, long pos, int mask) {

        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    public static boolean isShutdown(RingBuffer ring) {
    	return ring.imperativeShutDown.get();
    }

    public static void shutdown(RingBuffer ring) {
    	if (!ring.imperativeShutDown.getAndSet(true)) {
    		ring.firstShutdownCaller = new RingBufferException("Shutdown called");
    	}

    }

    public static void addByteArray(byte[] source, int sourceIdx, int sourceLen, RingBuffer rbRingBuffer) {

    	assert(sourceLen>=0);
    	validateVarLength(rbRingBuffer, sourceLen);

    	copyBytesFromToRing(source, sourceIdx, Integer.MAX_VALUE, rbRingBuffer.unstructuredLayoutRingBuffer, rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, rbRingBuffer.byteMask, sourceLen);

    	addBytePosAndLen(rbRingBuffer, rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, sourceLen);
        rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value + sourceLen);

    }

    public static void addNullByteArray(RingBuffer rbRingBuffer) {
        addBytePosAndLen(rbRingBuffer, rbRingBuffer.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value, -1);
    }


    public static void addIntValue(int value, RingBuffer rb) {
         assert(rb.structuredLayoutRingBufferHead.workingHeadPos.value <= rb.mask+RingBuffer.tailPosition(rb));
		 setValue(rb.structuredLayoutRingBuffer,rb.mask,rb.structuredLayoutRingBufferHead.workingHeadPos.value++,value);
	}

	//TODO: B, need to update build server to ensure this runs on both Java6 and Java ME 8

    //must be called by low-level API when starting a new message
    public static void addMsgIdx(RingBuffer rb, int msgIdx) {
        assert(rb.structuredLayoutRingBufferHead.workingHeadPos.value <= rb.mask+RingBuffer.tailPosition(rb));
    	assert(msgIdx>=0) : "Call publishEOF() instead of this method";

     	//this MUST be done here at the START of a message so all its internal fragments work with the same base position
     	 markBytesWriteBase(rb);

   // 	 assert(rb.llwNextHeadTarget<=rb.headPos.get() || rb.workingHeadPos.value<=rb.llwNextHeadTarget) : "Unsupported mix of high and low level API.";

		 rb.structuredLayoutRingBuffer[rb.mask & (int)rb.structuredLayoutRingBufferHead.workingHeadPos.value++] = msgIdx;
	}

	public static void setValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] = value;
    }


    public static void addBytePosAndLen(RingBuffer ring, int position, int length) {
        assert(ring.structuredLayoutRingBufferHead.workingHeadPos.value <= ring.mask+RingBuffer.tailPosition(ring));
		setBytePosAndLen(ring.structuredLayoutRingBuffer, ring.mask, ring.structuredLayoutRingBufferHead.workingHeadPos.value, position, length, RingBuffer.bytesWriteBase(ring));
		ring.structuredLayoutRingBufferHead.workingHeadPos.value+=2;
    }

	public static void addBytePosAndLenSpecial(int[] buffer, int mask, PaddedLong workingHeadPos, int bytesBasePos, int position, int length) {
		setBytePosAndLen(buffer, mask, workingHeadPos.value, position, length, bytesBasePos);
		workingHeadPos.value+=2;
	}

	public static void setBytePosAndLen(int[] buffer, int rbMask, long ringPos,	int positionDat, int lengthDat, int baseBytePos) {
	   	//negative position is written as is because the internal array does not have any offset (but it could some day)
    	//positive position is written after subtracting the rbRingBuffer.bytesHeadPos.longValue()
    	if (positionDat>=0) {
    		buffer[rbMask & (int)ringPos] = (int)(positionDat-baseBytePos) & RingBuffer.BYTES_WRAP_MASK; //mask is needed for the negative case, does no harm in positive case
    	} else {
    		buffer[rbMask & (int)ringPos] = positionDat;
    	}
        buffer[rbMask & (int)(ringPos+1)] = lengthDat;
	}

	public static int restorePosition(RingBuffer ring, int pos) {
		assert(pos>=0);
		return pos+ RingBuffer.bytesReadBase(ring);

	}

    public static int bytePosition(int meta, RingBuffer ring, int len) {
    	int pos = restorePosition(ring, meta & RELATIVE_POS_MASK);

        if (len>=0) {
        	ring.byteBufferTail.byteWorkingTailPos.value =  BYTES_WRAP_MASK&(len+ring.byteBufferTail.byteWorkingTailPos.value);
        }

        return pos;
    }

    public static int bytePositionGen(int meta, RingBuffer ring) {
    	return restorePosition(ring, meta & RELATIVE_POS_MASK);
    }


    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2, int value3) {

        long p = headCache.value;
        buffer[rbMask & (int)p++] = value1;
        buffer[rbMask & (int)p++] = value2;
        buffer[rbMask & (int)p++] = value3;
        headCache.value = p;

    }

    @Deprecated
    public static void addValues(int[] buffer, int rbMask, PaddedLong headCache, int value1, long value2) {

        headCache.value = setValues(buffer, rbMask, headCache.value, value1, value2);

    }

    public static void addDecimal(int exponent, long mantissa, RingBuffer ring) {
        ring.structuredLayoutRingBufferHead.workingHeadPos.value = setValues(ring.structuredLayoutRingBuffer, ring.mask, ring.structuredLayoutRingBufferHead.workingHeadPos.value, exponent, mantissa);
    }


	public static long setValues(int[] buffer, int rbMask, long pos, int value1, long value2) {
		buffer[rbMask & (int)pos++] = value1;
        buffer[rbMask & (int)pos++] = (int)(value2 >>> 32);
        buffer[rbMask & (int)pos++] = (int)(value2 & 0xFFFFFFFF);
		return pos;
	}

	@Deprecated //use addLongVlue(value, rb)
    public static void addLongValue(RingBuffer rb, long value) {
		 addLongValue(value, rb);
	}

	public static void addLongValue(long value, RingBuffer rb) {
		 addLongValue(rb.structuredLayoutRingBuffer, rb.mask, rb.structuredLayoutRingBufferHead.workingHeadPos, value);
	}

    public static void addLongValue(int[] buffer, int rbMask, PaddedLong headCache, long value) {

        long p = headCache.value;
        buffer[rbMask & (int)p] = (int)(value >>> 32);
        buffer[rbMask & (int)(p+1)] = ((int)value);
        headCache.value = p+2;

    }



    // WARNING: consumer of these may need to loop around end of buffer !!
    // these are needed for fast direct READ FROM here

    @Deprecated
    public static int readRingByteLen(int fieldPos, int[] rbB, int rbMask, PaddedLong rbPos) {
        return readRingByteLen(fieldPos,rbB, rbMask, rbPos.value);
    }

    public static int readRingByteLen(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int) (rbMask & (rbPos + fieldPos + 1))];// second int is always the length
    }

	public static int readRingByteLen(int idx, RingBuffer ring) {
		return readRingByteLen(idx,ring.structuredLayoutRingBuffer, ring.mask, ring.primaryBufferTail.workingTailPos.value);
	}

	public static int takeRingByteLen(RingBuffer ring) {
	    assert(ring.primaryBufferTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
		return ring.structuredLayoutRingBuffer[(int)(ring.mask & (ring.primaryBufferTail.workingTailPos.value++))];// second int is always the length
	}



    public static byte[] byteBackingArray(int meta, RingBuffer rbRingBuffer) {
        return rbRingBuffer.bufferLookup[meta>>>31];
    }

	public static int readRingByteMetaData(int pos, RingBuffer rb) {
		return readValue(pos,rb.structuredLayoutRingBuffer,rb.mask,rb.primaryBufferTail.workingTailPos.value);
	}

	public static int takeRingByteMetaData(RingBuffer ring) {
	    assert(ring.primaryBufferTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
		return readValue(0,ring.structuredLayoutRingBuffer,ring.mask,ring.primaryBufferTail.workingTailPos.value++);
	}

    public static int readValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }

    public static int readValue(int idx, RingBuffer ring) {
    	return readValue(idx, ring.structuredLayoutRingBuffer,ring.mask,ring.primaryBufferTail.workingTailPos.value);
    }

    public static int takeValue(RingBuffer ring) {
        //TODO: breaks code generator, should fix assert(ring.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
    	return readValue(0, ring.structuredLayoutRingBuffer, ring.mask, ring.primaryBufferTail.workingTailPos.value++);
    }

    public static long takeLong(RingBuffer ring) {
        assert(ring.primaryBufferTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
    	long result = readLong(ring.structuredLayoutRingBuffer,ring.mask,ring.primaryBufferTail.workingTailPos.value);
    	ring.primaryBufferTail.workingTailPos.value+=2;
    	return result;
    }

    public static long readLong(int idx, RingBuffer ring) {
    	return readLong(ring.structuredLayoutRingBuffer,ring.mask,idx+ring.primaryBufferTail.workingTailPos.value);

    }

    public static int takeMsgIdx(RingBuffer ring) {
        assert(ring.primaryBufferTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring)) : " tail is "+ring.primaryBufferTail.workingTailPos.value+" but head is "+RingBuffer.workingHeadPosition(ring);

    	//TODO: AAA, need to add assert to detect if this release was forgotten.
    	RingBuffer.markBytesReadBase(ring);

    	int msgIdx = readValue(0, ring.structuredLayoutRingBuffer,ring.mask,ring.primaryBufferTail.workingTailPos.value++);
    	return msgIdx;
    }

    public static int contentRemaining(RingBuffer rb) {
        return (int)(rb.structuredLayoutRingBufferHead.headPos.get() - rb.primaryBufferTail.tailPos.get()); //must not go past add count because it is not release yet.
    }


    public static void releaseReads(RingBuffer ring) {
    	takeValue(ring);
    	releaseReadLock(ring);
    }

    /**
     * Low level API release
     * @param ring
     */
    @Deprecated
    public static void readBytesAndreleaseReadLock(RingBuffer ring) {
    	releaseReads(ring);
    }

    //TODO: AAA, need to simplify API and keep this one internal
    public static void releaseReadLock(RingBuffer ring) {
        assert(RingBuffer.contentRemaining(ring)>=0);

        if ((--ring.batchReleaseCountDown<=0) ) {

    	    assert(ring.ringWalker.cursor<=0 && !RingReader.isNewMessage(ring.ringWalker)) : "Unsupported mix of high and low level API.  ";

    	    RingBuffer.setBytesTail(ring,RingBuffer.bytesWorkingTailPosition(ring));
    	    ring.primaryBufferTail.tailPos.lazySet(ring.primaryBufferTail.workingTailPos.value);
    	    ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;


    	} else {
    	    storeUnpublishedTail(ring);
    	}
    }

    public static void releaseReadLock2(RingBuffer ring) {

        if ((--ring.batchReleaseCountDown<=0)) {

                 RingBuffer.setBytesTail(ring,RingBuffer.bytesWorkingTailPosition(ring));
                 RingBuffer.publishWorkingTailPosition(ring, ring.ringWalker.nextWorkingTail);

                 ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;
        } else {
                 ring.lastReleasedBytesTail = ring.byteBufferTail.byteWorkingTailPos.value;
                 ring.lastReleasedTail = ring.ringWalker.nextWorkingTail;// ring.primaryBufferTail.workingTailPos.value;
        }


    }



    // value in lastReleased moves forward to the point for release up to
    // tail position is where we start releasing from.
    //
    //
    //  head
    //
    //
    //  working tail - read from here
    //
    //  new last release
    //
    //         //scan this block for data to remove?
    //
    //  tail
    //
    //
    //



    static void storeUnpublishedTail(RingBuffer ring) {
        ring.lastReleasedBytesTail = ring.byteBufferTail.byteWorkingTailPos.value;
        ring.lastReleasedTail = ring.primaryBufferTail.workingTailPos.value;
    }

    /**
     * Release any reads that were held back due to batching.
     * @param ring
     */
    public static void releaseAllBatchedReads(RingBuffer ring) {

        if (ring.lastReleasedTail>ring.primaryBufferTail.tailPos.get()) {
            PaddedInt.set(ring.byteBufferTail.bytesTailPos,ring.lastReleasedBytesTail);
            ring.primaryBufferTail.tailPos.lazySet(ring.lastReleasedTail);
            ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;
        }

        assert(debugHeadAssignment(ring));
    }

    @Deprecated
	public static void releaseAll(RingBuffer ring) {

			int i = ring.byteBufferTail.byteWorkingTailPos.value= ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
            PaddedInt.set(ring.byteBufferTail.bytesTailPos,i);
			ring.primaryBufferTail.tailPos.lazySet(ring.primaryBufferTail.workingTailPos.value= ring.structuredLayoutRingBufferHead.workingHeadPos.value);

    }

    @Deprecated
    public static void dump(RingBuffer rb) {

        // move the removePosition up to the addPosition
        // new Exception("WARNING THIS IS NO LONGER COMPATIBLE WITH PUMP CALLS").printStackTrace();
        rb.primaryBufferTail.tailPos.lazySet(rb.primaryBufferTail.workingTailPos.value = rb.structuredLayoutRingBufferHead.workingHeadPos.value);
    }


    /**
     * Low level API for publish
     * @param ring
     */
    public static void publishWrites(RingBuffer ring) {
    	//new Exception("publish trialing byte").printStackTrace();
    	//happens at the end of every fragment
        writeTrailingCountOfBytesConsumed(ring, ring.structuredLayoutRingBufferHead.workingHeadPos.value++); //increment because this is the low-level API calling

		publishWritesBatched(ring);
    }

    public static void publishWritesBatched(RingBuffer ring) {
        //single length field still needs to move this value up, so this is always done
		ring.bytesWriteLastConsumedBytePos = ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;

    	assert(ring.structuredLayoutRingBufferHead.workingHeadPos.value >= RingBuffer.headPosition(ring));
    	assert(ring.llWrite.llwConfirmedWrittenPosition<=RingBuffer.headPosition(ring) || ring.structuredLayoutRingBufferHead.workingHeadPos.value<=ring.llWrite.llwConfirmedWrittenPosition) : "Unsupported mix of high and low level API. NextHead>head and workingHead>nextHead";

    	publishHeadPositions(ring);
    }

    /**
     * Publish any writes that were held back due to batching.
     * @param ring
     */
    public static void publishAllBatchedWrites(RingBuffer ring) {

    	if (ring.lastPublishedStructuredLayoutRingBufferHead>ring.structuredLayoutRingBufferHead.headPos.get()) {
    		PaddedInt.set(ring.unstructuredLayoutRingBufferHead.bytesHeadPos,ring.lastPublishedUnstructuredLayoutRingBufferHead);
    		ring.structuredLayoutRingBufferHead.headPos.lazySet(ring.lastPublishedStructuredLayoutRingBufferHead);
    	}

		assert(debugHeadAssignment(ring));
		ring.batchPublishCountDown = ring.batchPublishCountDownInit;
    }


	private static boolean debugHeadAssignment(RingBuffer ring) {

		if (0!=(RingBufferConfig.SHOW_HEAD_PUBLISH&ring.debugFlags) ) {
			new Exception("Debug stack for assignment of published head positition"+ring.structuredLayoutRingBufferHead.headPos.get()).printStackTrace();
		}
		return true;
	}


	public static void publishHeadPositions(RingBuffer ring) {

	    //TODO: need way to test if publish was called on an input ? may be much easer to detect missing publish. or extra release.
	    if ((--ring.batchPublishCountDown<=0)) {
	        PaddedInt.set(ring.unstructuredLayoutRingBufferHead.bytesHeadPos,ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value);
	        ring.structuredLayoutRingBufferHead.headPos.lazySet(ring.structuredLayoutRingBufferHead.workingHeadPos.value);
	        assert(debugHeadAssignment(ring));
	        ring.batchPublishCountDown = ring.batchPublishCountDownInit;
	    } else {
	        storeUnpublishedHead(ring);
	    }
	}

	static void storeUnpublishedHead(RingBuffer ring) {
		ring.lastPublishedUnstructuredLayoutRingBufferHead = ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;
		ring.lastPublishedStructuredLayoutRingBufferHead = ring.structuredLayoutRingBufferHead.workingHeadPos.value;
	}

    public static void abandonWrites(RingBuffer ring) {
        //ignore the fact that any of this was written to the ring buffer
    	ring.structuredLayoutRingBufferHead.workingHeadPos.value = ring.structuredLayoutRingBufferHead.headPos.longValue();
    	ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value = PaddedInt.get(ring.unstructuredLayoutRingBufferHead.bytesHeadPos);
    	storeUnpublishedHead(ring);
    }

    /**
     * Blocks until there is enough room for this first fragment of the message and records the messageId.
     * @param ring
     * @param msgIdx
     */
	public static void blockWriteMessage(RingBuffer ring, int msgIdx) {
		//before write make sure the tail is moved ahead so we have room to write
	    spinBlockForRoom(ring, RingBuffer.from(ring).fragDataSize[msgIdx]);
		RingBuffer.addMsgIdx(ring, msgIdx);
	}


    //All the spin lock methods share the same implementation. Unfortunately these can not call
    //a common implementation because the extra method jump degrades the performance in tight loops
    //where these spin locks are commonly used.

    public static void spinBlockForRoom(RingBuffer ringBuffer, int size) {
        while (!roomToLowLevelWrite(ringBuffer, size)) {
            spinWork(ringBuffer);
        }
    }

    @Deprecated //use spinBlockForRoom then confirm the write afterwords
    public static long spinBlockOnTail(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while (null==ringBuffer.structuredLayoutRingBuffer || lastCheckedValue < targetValue) {
    		spinWork(ringBuffer);
		    lastCheckedValue = ringBuffer.primaryBufferTail.tailPos.longValue();
		}
		return lastCheckedValue;
    }

    public static void spinBlockForContent(RingBuffer ringBuffer) {
        while (!contentToLowLevelRead(ringBuffer, 1)) {
            spinWork(ringBuffer);
        }
    }

    //Used by RingInputStream to duplicate contract behavior,  TODO: AA rename to waitForAvailableContent or blockUntilContentReady?
    public static long spinBlockOnHead(long lastCheckedValue, long targetValue, RingBuffer ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		spinWork(ringBuffer);
		    lastCheckedValue = ringBuffer.structuredLayoutRingBufferHead.headPos.get();
		}
		return lastCheckedValue;
    }

	private static void spinWork(RingBuffer ringBuffer) {
		Thread.yield();//needed for now but re-evaluate performance impact
		if (isShutdown(ringBuffer) || Thread.currentThread().isInterrupted()) {
			throw null!=ringBuffer.firstShutdownCaller ? ringBuffer.firstShutdownCaller : new RingBufferException("Unexpected shutdown");
		}
	}

	public static int byteMask(RingBuffer ring) {
		return ring.byteMask;
	}

	public static long headPosition(RingBuffer ring) {
		 return ring.structuredLayoutRingBufferHead.headPos.get();
	}

    public static long workingHeadPosition(RingBuffer ring) {
        return PaddedLong.get(ring.structuredLayoutRingBufferHead.workingHeadPos);
    }

    public static void setWorkingHead(RingBuffer ring, long value) {
        PaddedLong.set(ring.structuredLayoutRingBufferHead.workingHeadPos, value);
    }

    public static long addAndGetWorkingHead(RingBuffer ring, int inc) {
        return PaddedLong.add(ring.structuredLayoutRingBufferHead.workingHeadPos, inc);
    }

    public static long getWorkingTailPosition(RingBuffer ring) {
        return PaddedLong.get(ring.primaryBufferTail.workingTailPos);
    }

    public static void setWorkingTailPosition(RingBuffer ring, long value) {
        PaddedLong.set(ring.primaryBufferTail.workingTailPos, value);
    }

    public static long addAndGetWorkingTail(RingBuffer ring, int inc) {
        return PaddedLong.add(ring.primaryBufferTail.workingTailPos, inc);
    }


	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param ring
	 * @param workingHeadPos
	 */
	public static void publishWorkingHeadPosition(RingBuffer ring, long workingHeadPos) {
		ring.structuredLayoutRingBufferHead.headPos.lazySet(ring.structuredLayoutRingBufferHead.workingHeadPos.value = workingHeadPos);
	}

	public static long tailPosition(RingBuffer ring) {
		return ring.primaryBufferTail.tailPos.get();
	}



	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param ring
	 * @param workingTailPos
	 */
	public static void publishWorkingTailPosition(RingBuffer ring, long workingTailPos) {
		ring.primaryBufferTail.tailPos.lazySet(ring.primaryBufferTail.workingTailPos.value = workingTailPos);
	}

	@Deprecated
	public static int primarySize(RingBuffer ring) {
		return ring.sizeOfStructuredLayoutRingBuffer;
	}

	public static FieldReferenceOffsetManager from(RingBuffer ring) {
		return ring.ringWalker.from;
	}

	public static int cursor(RingBuffer ring) {
        return ring.ringWalker.cursor;
    }

	public static void writeTrailingCountOfBytesConsumed(RingBuffer ring, long pos) {

		int consumed = ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value - ring.bytesWriteLastConsumedBytePos;
		//log.trace("wrote {} bytes consumed to position {}",consumed,pos);
		ring.structuredLayoutRingBuffer[ring.mask & (int)pos] = consumed>=0 ? consumed : consumed&BYTES_WRAP_MASK ;
		ring.bytesWriteLastConsumedBytePos = ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos.value;

	}

	public static IntBuffer wrappedStructuredLayoutRingBuffer(RingBuffer ring) {
		return ring.wrappedStructuredLayoutRingBuffer;
	}

	public static ByteBuffer wrappedUnstructuredLayoutRingBufferA(RingBuffer ring) {
		return ring.wrappedUnstructuredLayoutRingBufferA;
	}

    public static ByteBuffer wrappedUnstructuredLayoutRingBufferB(RingBuffer ring) {
        return ring.wrappedUnstructuredLayoutRingBufferB;
    }
    
	public static ByteBuffer wrappedUnstructuredLayoutConstBuffer(RingBuffer ring) {
		return ring.wrappedUnstructuredLayoutConstBuffer;
	}

	/////////////
	//low level API
	////////////


	//This holds the last known state of the tail position, if its sufficiently far ahead it indicates that
	//we do not need to fetch it again and this reduces contention on the CAS with the reader.
	//This is an important performance feature of the low level API and should not be modified.


	//TODO: AA, adjust unit tests to use this.
	public static boolean roomToLowLevelWrite(RingBuffer output, int size) {
		return roomToLowLevelWrite(output, output.llRead.llwConfirmedReadPosition+size);
	}

	private static boolean roomToLowLevelWrite(RingBuffer output, long target) {
		//only does second part if the first does not pass
		return (output.llRead.llrTailPosCache >= target) || roomToLowLevelWriteSlow(output, target);
	}

	private static boolean roomToLowLevelWriteSlow(RingBuffer output, long target) {
		return (output.llRead.llrTailPosCache = output.primaryBufferTail.tailPos.get()) >= target;
	}

	public static void confirmLowLevelWrite(RingBuffer output, int size) {
		output.llRead.llwConfirmedReadPosition += size;
	}


	public static boolean contentToLowLevelRead(RingBuffer input, int size) {
		return contentToLowLevelRead2(input, input.llWrite.llwConfirmedWrittenPosition+size);
	}

	private static boolean contentToLowLevelRead2(RingBuffer input, long target) {
		//only does second part if the first does not pass
		return (input.llWrite.llwHeadPosCache >= target) || contentToLowLevelReadSlow(input, target);
	}

	private static boolean contentToLowLevelReadSlow(RingBuffer input, long target) {
		return (input.llWrite.llwHeadPosCache = input.structuredLayoutRingBufferHead.headPos.get()) >= target;
	}

	public static long confirmLowLevelRead(RingBuffer input, long size) {
		return (input.llWrite.llwConfirmedWrittenPosition += size);
	}

    public static void setWorkingHeadTarget(RingBuffer input) {
        input.llWrite.llwConfirmedWrittenPosition =  RingBuffer.getWorkingTailPosition(input);
    }

	public static boolean hasReleasePending(RingBuffer ringBuffer) {
		return ringBuffer.batchReleaseCountDown!=ringBuffer.batchReleaseCountDownInit;
	}

    public static int bytesTailPosition(RingBuffer ring) {
        return PaddedInt.get(ring.byteBufferTail.bytesTailPos);
    }

    public static void setBytesTail(RingBuffer ring, int value) {
        PaddedInt.set(ring.byteBufferTail.bytesTailPos, value);
    }

    public static int bytesHeadPosition(RingBuffer ring) {
        return PaddedInt.get(ring.unstructuredLayoutRingBufferHead.bytesHeadPos);
    }

    public static void setBytesHead(RingBuffer ring, int value) {
        PaddedInt.set(ring.unstructuredLayoutRingBufferHead.bytesHeadPos, value);
    }

    public static int addAndGetBytesHead(RingBuffer ring, int inc) {
        return PaddedInt.add(ring.unstructuredLayoutRingBufferHead.bytesHeadPos, inc);
    }

    public static int bytesWorkingTailPosition(RingBuffer ring) {
        return PaddedInt.get(ring.byteBufferTail.byteWorkingTailPos);
    }

    public static int addAndGetBytesWorkingTailPosition(RingBuffer ring, int inc) {
        return PaddedInt.maskedAdd(ring.byteBufferTail.byteWorkingTailPos, inc, RingBuffer.BYTES_WRAP_MASK);
    }

    public static void setBytesWorkingTail(RingBuffer ring, int value) {
        PaddedInt.set(ring.byteBufferTail.byteWorkingTailPos, value);
    }

    public static int bytesWorkingHeadPosition(RingBuffer ring) {
        return PaddedInt.get(ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos);
    }

    public static int addAndGetBytesWorkingHeadPosition(RingBuffer ring, int inc) {
        return PaddedInt.maskedAdd(ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos, inc, RingBuffer.BYTES_WRAP_MASK);
    }

    public static void setBytesWorkingHead(RingBuffer ring, int value) {
        PaddedInt.set(ring.unstructuredLayoutRingBufferHead.byteWorkingHeadPos, value);
    }

    public static int decBatchRelease(RingBuffer rb) {
        return --rb.batchReleaseCountDown;
    }

    public static int decBatchPublish(RingBuffer rb) {
        return --rb.batchPublishCountDown;
    }

    public static void beginNewReleaseBatch(RingBuffer rb) {
        rb.batchReleaseCountDown = rb.batchReleaseCountDownInit;
    }

    public static void beginNewPublishBatch(RingBuffer rb) {
        rb.batchPublishCountDown = rb.batchPublishCountDownInit;
    }

    public static byte[] byteBuffer(RingBuffer rb) {
        return rb.unstructuredLayoutRingBuffer;
    }

    public static int[] primaryBuffer(RingBuffer rb) {
        return rb.structuredLayoutRingBuffer;
    }

    public static void updateBytesWriteLastConsumedPos(RingBuffer rb) {
        rb.bytesWriteLastConsumedBytePos = RingBuffer.bytesWorkingHeadPosition(rb);
    }

    public static PaddedLong getWorkingTailPositionObject(RingBuffer rb) {
        return rb.primaryBufferTail.workingTailPos;
    }

    public static PaddedLong getWorkingHeadPositionObject(RingBuffer rb) {
        return rb.structuredLayoutRingBufferHead.workingHeadPos;
    }




}
