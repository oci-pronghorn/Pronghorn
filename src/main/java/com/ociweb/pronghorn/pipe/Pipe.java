package com.ociweb.pronghorn.pipe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.PaddedAtomicLong;


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
 *
 * Schema aware data pipe implemented as an internal pair of ring buffers.
 *
 * One ring holds all the fixed-length fields and the fixed-length meta data relating to the variable-length
 * (unstructured fields).  The other ring holds only bytes which back the variable-length fields like Strings or Images.
 *
 * The supported Schema is defined in the FieldReferenceOffsetManager passed in upon construction.  The Schema is
 * made up of Messages. Messages are made up of one or more fixed-length fragments.
 *
 * The Message fragments enable direct lookup of fields within sequences and enable the consumption of larger messages
 * than would fit within the defined limits of the buffers.
 *
 * @author Nathan Tippy
 *
 * //cas: this needs expanded explanation of what a slot is. (best if done above.)
 * Storage:
 *  int     - 1 slot
 *  long    - 2 slots, high then low
 *  text    - 2 slots, index then length  (if index is negative use constant array)
 *  decimal - 3 slots, exponent then long mantissa
 *
 *  SlabRing - structured fixed size records
 *  BlobRing - unstructured variable length field data 
 *
 * StructuredLayoutRing   - These have strong type definition per field in addition to being fixed-length.
 * UnstructuredLayoutRing - These are just bytes that are commonly UTF-8 encoded strings but may be image data or
 * even video/audio streams.
 *
 * @since 0.1
 *
 */
public final class Pipe<T extends MessageSchema> {

    private static final AtomicInteger ringCounter = new AtomicInteger();
    
    /**
     * Holds the active head position information.
     */
    static class SlabRingHead {
        // Position used during creation of a message in the ring.
        final PaddedLong workingHeadPos;
        // Actual position of the next Write location.
        final AtomicLong headPos;

        SlabRingHead() {
            this.workingHeadPos = new PaddedLong();
            this.headPos = new PaddedAtomicLong();
        }
    }

    /**
     * Holds the active tail position information.
     */
    static class SlabRingTail {
        /**
         * The workingTailPosition is only to be used by the consuming thread. As values are read the tail is moved
         * forward.  Eventually the consumer finishes the read of the fragment and will use this working position as
         * the value to be published in order to inform the writer of this new free space.
         */
        final PaddedLong workingTailPos; // No need for an atomic operation since only one thread will ever use this.

        /**
         * This is the official published tail position. It is written to by the consuming thread and frequently
         * polled by the producing thread.  Making use of the built in CAS features of AtomicLong forms a memory
         * gate that enables this lock free implementation to function.
         */
        final AtomicLong tailPos;

        SlabRingTail() {
            this.workingTailPos = new PaddedLong();
            this.tailPos = new PaddedAtomicLong();
        }

        /**
         * Switch the working tail back to the published tail position.
         * Only used by the replay feature, not for general use.
         */
        // TODO: ?
        // Enforce the contract of replay-only.
		long rollBackWorking() {
			return workingTailPos.value = tailPos.get();
		}
    }


    /**
     * Spinning on a CAS AtomicLong leads to a lot of contention which will decrease performance.
     * Once we know that the producer can write up to a given position there is no need to keep polling until data
     * is written up to that point.  This class holds the head value until that position is reached.
     */
    static class LowLevelAPIWritePositionCache {
        /**
         * This is the position the producer is allowed to write up to before having to ask the CAS AtomicLong
         * for a new value.
         */
        long llwHeadPosCache;

        /**
         * Holds the last position that has been officially written.  The Low Level API uses the size of the next
         * fragment added to this value to determine if the next write will need to go past the cached head position
         * above.
         *
         * // TODO: reword "by the size of the fragment"?
         * Once it is known that the write will fit, this value is incremented by the size to confirm the write.
         * This is independent of the workingHeadPosition by design so we have two accounting mechanisms to help
         * detect errors.
         *
         * TODO:M add asserts that implement the claim found above in the comments.
         */
        long llwConfirmedWrittenPosition;

        // TODO: Determine is this is going to be used -- if not, delete it.
        LowLevelAPIWritePositionCache() {
        }
    }

    /**
     * Holds the tail value for the consumer.
     */
    static class LowLevelAPIReadPositionCache {
        long llrTailPosCache;
        /**
         * Holds the last position that has been officially read.
         */
        long llwConfirmedReadPosition;

        // TODO: Determine is this is going to be used -- if not, delete it.
        LowLevelAPIReadPositionCache() {
        }
    }

    /**
     * Serves the same function as the StructuredLayoutRingHead, but holds information for the UnstructuredLayoutRing.
     */
    static class BlobRingHead {
        final PaddedInt byteWorkingHeadPos;
        final PaddedInt bytesHeadPos;

        BlobRingHead() {
            this.byteWorkingHeadPos = new PaddedInt();
            this.bytesHeadPos = new PaddedInt();
        }
    }

    /**
     * Serves the same function as the StructuredLayoutRingTail, but holds information for the UnstructuredLayoutRing.
     */
    static class BlobRingTail {
        final PaddedInt byteWorkingTailPos;
        final PaddedInt bytesTailPos;

        BlobRingTail() {
            this.byteWorkingTailPos = new PaddedInt();
            this.bytesTailPos = new PaddedInt();
        }

        // TODO: The "only used by" needs to be enforced.
        /**
         * Switch the working tail back to the published tail position.
         * Only used by the replay feature, not for general use.
         */
		int rollBackWorking() {
			return byteWorkingTailPos.value = bytesTailPos.value;
		}
    }

    /**
     * Provides a container holding a long value that fills a 64-byte cache line.
     */
    public static class PaddedLong {
        // These primitives will be next to one another in memory if there are no other members of this object.
        // TODO: Is this public?
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;

        // The following accessor methods are static instead of instance methods because small static methods will
        // frequently be in-lined which allows direct access to the value member without method overhead.
        /**
         * Provides access to the value of this PaddedLong.
         * @param pl  is the PaddedLong containing the desired value.
         * @return    the value contained by the provided long.
         */
        public static long get(PaddedLong pl) {
            return pl.value;
        }

        /**
         * Sets the value of the provided PaddedLong.
         * @param pl     is the padded long to contain the value.
         * @param value  is the value to be put into the padded long.
         */
        public static void set(PaddedLong pl, long value) {
            pl.value = value;
        }

        /**
         * Adds the provided increment to the existing value of the long.
         * <b>N.B.</b> A PaddedLong is initialized to zero.  There is no problem invoking this method on a PaddedLong
         * that has never had the set method called.  It may not achieve the desired effect, but it will not cause a
         * runtime error.
         * @param pl   is the padded long containing the value to increment.
         * @param inc  is the amount to add.
         * @return     the incremented value of the provided padded long instance.
         */
        public static long add(PaddedLong pl, long inc) {
                return pl.value += inc;
        }

        /**
         * Provides a readable representation of the value of this padded long instance.
         * @return  a String of the Long value of this padded long instance.
         */
        public String toString() {
            return Long.toString(value);
        }

    }

    /**
     * Provides a container holding an int value that fills a 64-byte cache line.
     */
    public static class PaddedInt {
        // Most platforms have 64 byte cache lines so the value variable is padded so 16 four byte ints are consumed.
        // If a platform has smaller cache lines, this approach will use a little more memory than required but the
        // performance gains will still be preserved.
        // Modern Intel and AMD chips commonly have 64 byte cache lines.
        // TODO: code This should just be 15, shouldn't it?
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7, padding8,
            padding9, padding10, padding11, padding13, padding14, padding15, padding16;

        /**
         * Provides access to the value of this PaddedInt.
         * @param pi  is the PaddedInt containing the desired value.
         * @return    the value contained by the provided int.
         */
		public static int get(PaddedInt pi) {
	            return pi.value;
	    }

        /**
         * Sets the value of the provided PaddedInt.
         * @param pi     is the padded int to contain the value.
         * @param value  is the value to be put into the padded int.
         */
		public static void set(PaddedInt pi, int value) {
		    pi.value = value;
		}

        /**
         * Adds the provided increment to the existing value of the int.
         * <b>N.B.</b> A PaddedInt is initialized to zero.  There is not problem invoking this method on a PaddedInt
         * that has never had the set method called.  It may not achieve the desired effect, but it will not cause a
         * runtime error.
         * @param pi   is the padded int containing the value to increment.
         * @param inc  is the amount to add.
         * @return     the incremented value of the provided padded int instance.
         */
	    public static int add(PaddedInt pi, int inc) {
	            return pi.value += inc;
	    }

        /**
         * Provides an increment routine to support the need to wrap the head and tail markers of a buffer from the
         * maximum int value to 0 without going negative. The method adds the provided increment to the existing value
         * of the provided PaddedInt. The resultant sum is <code>and</code>ed to the provided mask to remove any
         * sign bit that may have been set in the case of an overflow of the maximum-sized integer.
         * <b>N.B.</b> A PaddedInt is initialized to zero.  There is no problem invoking this method on a PaddedInt
         * that has never had the set method called.  It may not achieve the desired effect, but it will not cause a
         * runtime error.
         * @param pi   is the padded int containing the value to increment.
         * @param inc  is the amount to add.
         * @return     the incremented value of the provided padded int instance.
         */
	    public static int maskedAdd(PaddedInt pi, int inc, int wrapMask) {
               return pi.value = wrapMask & (inc + pi.value);
        }

        /**
         * Provides a readable representation of the value of this padded long instance.
         * @return  a String of the Long value of this padded long instance.
         */
		public String toString() {
		    return Integer.toString(value);
		}
    }

    private static final Logger log = LoggerFactory.getLogger(Pipe.class);

    //I would like to follow the convention where all caps constants are used to indicate static final values which are resolved at compile time.
    //This is distinct from other static finals which hold run time instances and values computed from runtime input.
    //The reason for this distinction is that these members have special properties.
    //    A) the literal value replaces the variable by the compiler so..   a change of value requires a recompile of all dependent jars.
    //    B) these are the only variables which are allowed as case values in switch statements.

    //This mask is used to filter the meta value used for variable-length fields.
    //after applying this mask to meta the result is always the relative offset within the byte buffer of where the variable-length data starts.
    //NOTE: when the high bit is set we will not pull the value from the ring buffer but instead use the constants array (these are pronouns)
    public static final int RELATIVE_POS_MASK = 0x7FFFFFFF; //removes high bit which indicates this is a constant

    //This mask is here to support the fact that variable-length fields will run out of space because the head/tail are 32 bit ints instead of
    //longs that are used for the structured layout data.  This mask enables the int to wrap back down to zero instead of going negative.
    //this will only happen once for every 2GB written.
    public static final int BYTES_WRAP_MASK = 0x7FFFFFFF;//NOTE: this trick only works because its 1 bit less than the roll-over sign bit

    //A few corner use cases require a poison pill EOF message to be sent down the pipes to ensure each consumer knows when to shut down.
    //This is here for compatibility with legacy APIs,  This constant is the size of the EOF message.
    public static final int EOF_SIZE = 2;

    //these public fields are fine because they are all final
    public final int ringId;
    public final int sizeOfSlabRing;
    public final int sizeOfBlobRing;
    public final int mask;
    public final int byteMask;
    public final byte bitsOfSlabRing;
    public final byte bitsOfBlogRing;
    public final int maxAvgVarLen;


    //TODO: B, need to add constant for gap always kept after head and before tail, this is for debug mode to store old state upon error. NEW FEATURE.
    //            the time slices of the graph will need to be kept for all rings to reconstruct history later.


    private final SlabRingHead slabRingHead = new SlabRingHead();
    private final BlobRingHead blobRingHead = new BlobRingHead();

    LowLevelAPIWritePositionCache llWrite; //low level write head pos cache and target
    LowLevelAPIReadPositionCache llRead; //low level read tail pos cache and target

    final StackStateWalker ringWalker;

    private final SlabRingTail slabRingTail = new SlabRingTail(); //primary working and public
    private final BlobRingTail blobRingTail = new BlobRingTail(); //primary working and public

    //these values are only modified and used when replay is NOT in use
    //hold the publish position when batching so the batch can be flushed upon shutdown and thread context switches
    private int lastReleasedBlobTail;
    long lastReleasedSlabTail;

    private int blobWriteLastConsumedPos = 0;

    //All references found in the messages/fragments to variable-length content are relative.  These members hold the current
    //base offset to which the relative value is added to find the absolute position in the ring.
    //These values are only updated as each fragment is consumed or produced.
    private int blobWriteBase = 0;
    private int blobReadBase = 0;

    //Non Uniform Memory Architectures (NUMA) are supported by the Java Virtual Machine(JVM)
    //However there are some limitations.
    //   A) NUMA support must be enabled with the command line argument
    //   B) The heap space must be allocated by the same thread which expects to use it long term.
    //
    // As a result of the above the construction of the buffers is postponed and done with an initBuffers() method.
    // The initBuffers() method will be called by the consuming thread before the pipe is used. (see Pronghorn)
    public byte[] blobRing; //TODO: B, these two must remain public until the meta/sql modules are fully integrated.
    private int[] slabRing;
    //defined externally and never changes
    protected final byte[] blobConstBuffer;
    private byte[][] blobRingLookup;
    //NOTE:
    //     This is the future direction of the ring buffer which is not yet complete
    //     By migrating all array index usages to these the backing ring can be moved outside the Java heap
    //     By moving the ring outside the Java heap other applications have have direct access
    //     The Overhead of the poly method call is what has prevented this change

    private IntBuffer wrappedSlabRing;
    private ByteBuffer wrappedBlobRingA;
    private ByteBuffer wrappedBlobRingB;
    private ByteBuffer wrappedBlobConstBuffer;

    //for writes validates that bytes of var length field is within the expected bounds.
    private int varLenMovingAverage = 0;//this is an exponential moving average

    static final int JUMP_MASK = 0xFFFFF;

    //Exceptions must not occur within consumers/producers of rings however when they do we no longer have
    //a clean understanding of state. To resolve the problem all producers and consumers must also shutdown.
    //This flag passes the signal so any producer/consumer that sees it on knows to shut down and pass on the flag.
    private final AtomicBoolean imperativeShutDown = new AtomicBoolean(false);
    private PipeException firstShutdownCaller = null;


	//hold the batch positions, when the number reaches zero the records are send or released
	private int batchReleaseCountDown = 0;
	private int batchReleaseCountDownInit = 0;
	private int batchPublishCountDown = 0;
	private int batchPublishCountDownInit = 0;
	//cas: jdoc -- This is the first mention of batch(ing).  It would really help the maintainer's comprehension of what
	// you mean if you would explain this hugely overloaded word somewhere prior to use -- probably in the class's javadoc.
	    //hold the publish position when batching so the batch can be flushed upon shutdown and thread context switches
    private int lastPublishedBlobRingHead;
    private long lastPublishedSlabRingHead;

	private final int debugFlags;

	private long holdingSlabWorkingTail;
	private int  holdingBlobWorkingTail;
	private int holdingBlobReadBase;

    public Pipe(PipeConfig<T> config) {

        byte primaryBits = config.primaryBits;
        byte byteBits = config.byteBits;
        byte[] byteConstants = config.byteConst;


        debugFlags = config.debugFlags;

        //Assign the immutable universal id value for this specific instance
        //these values are required to keep track of all ring buffers when graphs are built
        this.ringId = ringCounter.getAndIncrement();

        this.bitsOfSlabRing = primaryBits;
        this.bitsOfBlogRing = byteBits;

        assert (primaryBits >= 0); //zero is a special case for a mock ring

//cas: naming.  This should be consistent with the maxByteSize, i.e., maxFixedSize or whatever.
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.sizeOfSlabRing = 1 << primaryBits;
        this.mask = sizeOfSlabRing - 1;

        //single text and byte buffers because this is where the variable-length data will go.

        this.sizeOfBlobRing =  1 << byteBits;
        this.byteMask = sizeOfBlobRing - 1;

        FieldReferenceOffsetManager from = config.from;
        this.ringWalker = new StackStateWalker(from);
        this.blobConstBuffer = byteConstants;


        if (0 == from.maxVarFieldPerUnit || 0==primaryBits) { //zero bits is for the dummy mock case
            maxAvgVarLen = 0; //no fragments had any variable-length fields so we never allow any
        } else {
            //given outer ring buffer this is the maximum number of var fields that can exist at the same time.
            int mx = sizeOfSlabRing;
            int maxVarCount = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, mx);
            //to allow more almost 2x more flexibility in variable-length bytes we track pairs of writes and ensure the
            //two together are below the threshold rather than each alone
            maxAvgVarLen = sizeOfBlobRing/maxVarCount;
        }
    }
    
    
	public static <S extends MessageSchema> void replayUnReleased(Pipe<S> ringBuffer) {

//We must enforce this but we have a few unit tests that are in violation which need to be fixed first
//	    if (!RingBuffer.from(ringBuffer).hasSimpleMessagesOnly) {
//	        throw new UnsupportedOperationException("replay of unreleased messages is not supported unless every message is also a single fragment.");
//	    }

		if (!isReplaying(ringBuffer)) {
			//save all working values only once if we re-enter replaying multiple times.

		    ringBuffer.holdingSlabWorkingTail = Pipe.getWorkingTailPosition(ringBuffer);
			ringBuffer.holdingBlobWorkingTail = Pipe.getWorkingBlobRingTailPosition(ringBuffer);

			//NOTE: we must never adjust the ringWalker.nextWorkingHead because this is replay and must not modify write position!
			ringBuffer.ringWalker.holdingNextWorkingTail = ringBuffer.ringWalker.nextWorkingTail;

			ringBuffer.holdingBlobReadBase = ringBuffer.blobReadBase;

		}

		//clears the stack and cursor position back to -1 so we assume that the next read will begin a new message
		StackStateWalker.resetCursorState(ringBuffer.ringWalker);

		//set new position values for high and low api
		ringBuffer.ringWalker.nextWorkingTail = ringBuffer.slabRingTail.rollBackWorking();
		ringBuffer.blobReadBase = ringBuffer.blobRingTail.rollBackWorking(); //this byte position is used by both high and low api
	}

/**
 * Returns <code>true</code> if the provided pipe is replaying.
 *
 * @param ringBuffer  the ringBuffer to check.
 * @return            <code>true</code> if the ringBuffer is replaying, <code>false</code> if it is not.
 */
	public static <S extends MessageSchema> boolean isReplaying(Pipe<S> ringBuffer) {
		return Pipe.getWorkingTailPosition(ringBuffer)<ringBuffer.holdingSlabWorkingTail;
	}

	public static <S extends MessageSchema> void cancelReplay(Pipe<S> ringBuffer) {
		ringBuffer.slabRingTail.workingTailPos.value = ringBuffer.holdingSlabWorkingTail;
		ringBuffer.blobRingTail.byteWorkingTailPos.value = ringBuffer.holdingBlobWorkingTail;

		ringBuffer.blobReadBase = ringBuffer.holdingBlobReadBase;

		ringBuffer.ringWalker.nextWorkingTail = ringBuffer.ringWalker.holdingNextWorkingTail;
		//NOTE while replay is in effect the head can be moved by the other (writing) thread.
	}

	////
	////
	public static <S extends MessageSchema> void batchAllReleases(Pipe<S> rb) {
	       rb.batchReleaseCountDownInit = Integer.MAX_VALUE;
	       rb.batchReleaseCountDown = Integer.MAX_VALUE;
	}


    public static <S extends MessageSchema> void setReleaseBatchSize(Pipe<S> rb, int size) {

    	validateBatchSize(rb, size);

    	rb.batchReleaseCountDownInit = size;
    	rb.batchReleaseCountDown = size;
    }

    public static <S extends MessageSchema> void setPublishBatchSize(Pipe<S> rb, int size) {

    	validateBatchSize(rb, size);

    	rb.batchPublishCountDownInit = size;
    	rb.batchPublishCountDown = size;
    }
    
    public static <S extends MessageSchema> int getPublishBatchSize(Pipe<S> pipe) {
        return pipe.batchPublishCountDownInit;
    }
    
    public static <S extends MessageSchema> int getReleaseBatchSize(Pipe<S> pipe) {
        return pipe.batchReleaseCountDownInit;
    }

    public static <S extends MessageSchema> void setMaxPublishBatchSize(Pipe<S> rb) {

    	int size = computeMaxBatchSize(rb, 3);

    	rb.batchPublishCountDownInit = size;
    	rb.batchPublishCountDown = size;

    }

    public static <S extends MessageSchema> void setMaxReleaseBatchSize(Pipe<S> rb) {

    	int size = computeMaxBatchSize(rb, 3);
    	rb.batchReleaseCountDownInit = size;
    	rb.batchReleaseCountDown = size;

    }


//cas: naming -- a couple of things, neither new.  Obviously the name of the buffer, bytes.  Also the use of base in
// the variable buffer, but not in the fixed.  Otoh, by now, maybe the interested reader would already understand.
    public static <S extends MessageSchema> int bytesWriteBase(Pipe<S> rb) {
    	return rb.blobWriteBase;
    }

    public static <S extends MessageSchema> void markBytesWriteBase(Pipe<S> rb) {
    	rb.blobWriteBase = rb.blobRingHead.byteWorkingHeadPos.value;
    }

    public static <S extends MessageSchema> int bytesReadBase(Pipe<S> rb) {
    	return rb.blobReadBase;
    }
        
    
    public static <S extends MessageSchema> void markBytesReadBase(Pipe<S> pipe, int bytesConsumed) {
        pipe.blobReadBase = Pipe.BYTES_WRAP_MASK & (pipe.blobReadBase+bytesConsumed);
     //   assert(0==bytesConsumed || pipe.blobReadBase <= Pipe.bytesHeadPosition(pipe)); //not true for some tests, must investigate jFAST usage.
    }
    
    public static <S extends MessageSchema> void markBytesReadBase(Pipe<S> pipe) {
        int value = PaddedInt.get(pipe.blobRingTail.byteWorkingTailPos);        
        pipe.blobReadBase = Pipe.BYTES_WRAP_MASK & value;
    }
    
    //;

    /**
     * Helpful user readable summary of the ring buffer.
     * Shows where the head and tail positions are along with how full the ring is at the time of call.
     */
    public String toString() {

    	StringBuilder result = new StringBuilder();
    	result.append("RingId:").append(ringId);
    	result.append(" slabTailPos ").append(slabRingTail.tailPos.get());
    	result.append(" slabWrkTailPos ").append(slabRingTail.workingTailPos.value);
    	result.append(" slabHeadPos ").append(slabRingHead.headPos.get());
    	result.append(" slabWrkHeadPos ").append(slabRingHead.workingHeadPos.value);
    	result.append("  ").append(slabRingHead.headPos.get()-slabRingTail.tailPos.get()).append("/").append(sizeOfSlabRing);
    	result.append("  blobTailPos ").append(PaddedInt.get(blobRingTail.bytesTailPos));
    	result.append(" blobWrkTailPos ").append(blobRingTail.byteWorkingTailPos.value);
    	result.append(" blobHeadPos ").append(PaddedInt.get(blobRingHead.bytesHeadPos));
    	result.append(" blobWrkHeadPos ").append(blobRingHead.byteWorkingHeadPos.value);

    	return result.toString();
    }


    /**
     * Return the configuration used for this ring buffer, Helpful when we need to make clones of the ring which will hold same message types.
     * @return
     */
    public PipeConfig<T> config() {
        //TODO:M, this creates garbage and we should just hold the config object instead of copying the values out.  Then return the same instance here.
        return new PipeConfig<T>(bitsOfSlabRing,bitsOfBlogRing,blobConstBuffer,ringWalker.from);
    }




    public static <S extends MessageSchema> int totalRings() {
        return ringCounter.get();
    }

	public Pipe<T> initBuffers() {
		assert(!isInit(this)) : "RingBuffer was already initialized";
		if (!isInit(this)) {
			buildBuffers();
		} else {
			log.warn("Init was already called once already on this ring buffer");
		}
		return this;
    }

	private void buildBuffers() {

        assert(slabRingHead.workingHeadPos.value == slabRingHead.headPos.get());
        assert(slabRingTail.workingTailPos.value == slabRingTail.tailPos.get());
        assert(slabRingHead.workingHeadPos.value == slabRingTail.workingTailPos.value);
        assert(slabRingTail.tailPos.get()==slabRingHead.headPos.get());

        long toPos = slabRingHead.workingHeadPos.value;//can use this now that we have confirmed they all match.

        this.llRead = new LowLevelAPIReadPositionCache();
        this.llWrite = new LowLevelAPIWritePositionCache();

        //This init must be the same as what is done in reset()
        //This target is a counter that marks if there is room to write more data into the ring without overwriting other data.
        this.llRead.llwConfirmedReadPosition = 0-this.sizeOfSlabRing;
        llWrite.llwHeadPosCache = toPos;
        llRead.llrTailPosCache = toPos;
        llRead.llwConfirmedReadPosition = toPos - sizeOfSlabRing;
        llWrite.llwConfirmedWrittenPosition = toPos;

        this.blobRing = new byte[sizeOfBlobRing];
        this.slabRing = new int[sizeOfSlabRing];
        this.blobRingLookup = new byte[][] {blobRing,blobConstBuffer};

        this.wrappedSlabRing = IntBuffer.wrap(this.slabRing);        
        this.wrappedBlobRingA = ByteBuffer.wrap(this.blobRing);
        this.wrappedBlobRingB = ByteBuffer.wrap(this.blobRing);
        this.wrappedBlobConstBuffer = null==this.blobConstBuffer?null:ByteBuffer.wrap(this.blobConstBuffer);
        
        assert(0==wrappedBlobRingA.position() && wrappedBlobRingA.capacity()==wrappedBlobRingA.limit()) : "The ByteBuffer is not clear.";

	}

	public static <S extends MessageSchema> boolean isInit(Pipe<S> ring) {
	    //Due to the fact that no locks are used it becomes necessary to check
	    //every single field to ensure the full initialization of the object
	    //this is done as part of graph set up and as such is called rarely.
		return null!=ring.blobRing &&
			   null!=ring.slabRing &&
			   null!=ring.blobRingLookup &&
			   null!=ring.wrappedSlabRing &&
			   null!=ring.wrappedBlobRingA &&
			   null!=ring.llRead &&
			   null!=ring.llWrite;
	}

	public static <S extends MessageSchema> void validateVarLength(Pipe<S> rb, int length) {
		int newAvg = (length+rb.varLenMovingAverage)>>1;
        if (newAvg>rb.maxAvgVarLen)	{
            //compute some helpful information to add to the exception
        	int bytesPerInt = (int)Math.ceil(length*Pipe.from(rb).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);

        	throw new UnsupportedOperationException("Can not write byte array of length "+length+
        	                                        ". The dif between primary and byte bits should be at least "+bitsDif+
        	                                        ". "+rb.bitsOfSlabRing+","+rb.bitsOfBlogRing+
        	                                        ". The limit is "+rb.maxAvgVarLen+" for pipe "+rb);
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
     * @param structuredPos
     */
    public void reset(int structuredPos, int unstructuredPos) {

    	slabRingHead.workingHeadPos.value = structuredPos;
        slabRingTail.workingTailPos.value = structuredPos;
        slabRingTail.tailPos.set(structuredPos);
        slabRingHead.headPos.set(structuredPos);

        if (null!=llWrite) {
            llWrite.llwHeadPosCache = structuredPos;
            llRead.llrTailPosCache = structuredPos;
            llRead.llwConfirmedReadPosition = structuredPos - sizeOfSlabRing;
            llWrite.llwConfirmedWrittenPosition = structuredPos;
        }

        blobRingHead.byteWorkingHeadPos.value = unstructuredPos;
        PaddedInt.set(blobRingHead.bytesHeadPos,unstructuredPos);

        blobWriteBase = unstructuredPos;
        blobReadBase = unstructuredPos;
        blobWriteLastConsumedPos = unstructuredPos;
        

        blobRingTail.byteWorkingTailPos.value = unstructuredPos;
        PaddedInt.set(blobRingTail.bytesTailPos,unstructuredPos);
        StackStateWalker.reset(ringWalker, structuredPos);
    }

    public static <S extends MessageSchema> ByteBuffer wrappedBlobForWriting(int originalBlobPosition, Pipe<S> output) {
        ByteBuffer target = Pipe.wrappedBlobRingA(output);  //Get the blob array as a wrapped byte buffer     
        int writeToPos = originalBlobPosition & Pipe.blobMask(output); //Get the offset in the blob where we should write
        target.position(writeToPos);   
        target.limit(Math.min(target.capacity(), writeToPos+output.maxAvgVarLen )); //ensure we stop at end of wrap or max var length 
        return target;
    }


    public static <S extends MessageSchema> void moveBlobPointerAndRecordPosAndLength(int originalBlobPosition, int len, Pipe<S> output) {
        Pipe.addAndGetBytesWorkingHeadPosition(output, len);
        Pipe.addBytePosAndLen(output, originalBlobPosition, len);
    }


    public static <S extends MessageSchema> ByteBuffer wrappedBlobRingB(Pipe<S> ring, int meta, int len) {
        return wrappedBlobReadingRingB(ring,meta,len);
    }

    public static <S extends MessageSchema> ByteBuffer wrappedBlobReadingRingB(Pipe<S> ring, int meta, int len) {
        ByteBuffer buffer;
        if (meta < 0) {
        	//always zero because constant array never wraps
        	buffer = wrappedBlobConstBuffer(ring);
        	buffer.position(0);
        	buffer.limit(0);
        } else {
        	buffer = wrappedBlobRingB(ring);
        	int position = ring.byteMask & restorePosition(ring,meta);
        	buffer.clear();
            //position is zero
        	int endPos = position+len;
        	if (endPos>ring.sizeOfBlobRing) {
        		buffer.limit(ring.byteMask & endPos);
        	} else {
        		buffer.limit(0);
        	}
        }		
    	return buffer;
    }

    public static <S extends MessageSchema> ByteBuffer wrappedBlobRingA(Pipe<S> ring, int meta, int len) {
        return wrappedBlobReadingRingA(ring, meta, len);
    }
    
    public static <S extends MessageSchema> ByteBuffer wrappedBlobReadingRingA(Pipe<S> ring, int meta, int len) {
        ByteBuffer buffer;
        if (meta < 0) {
        	buffer = wrappedBlobConstBuffer(ring);
        	int position = PipeReader.POS_CONST_MASK & meta;    
        	buffer.position(position);
        	buffer.limit(position+len);        	
        } else {
        	buffer = wrappedBlobRingA(ring);
        	int position = ring.byteMask & restorePosition(ring,meta);
        	buffer.clear();
        	buffer.position(position);
        	//use the end of the buffer if the lengh runs past it.
        	buffer.limit(Math.min(ring.sizeOfBlobRing, position+len));
        }
        return buffer;
    }

    public static int convertToUTF8(final char[] charSeq, final int charSeqOff, final int charSeqLength, final byte[] targetBuf, final int targetIdx, final int targetMask) {
    	
    	int target = targetIdx;				
        int c = 0;
        while (c < charSeqLength) {
        	target = encodeSingleChar((int) charSeq[charSeqOff+c++], targetBuf, targetMask, target);
        }
        //NOTE: the above loop will keep looping around the target buffer until done and will never cause an array out of bounds.
        //      the length returned however will be larger than targetMask, this should be treated as an error.
        return target-targetIdx;//length;
    }

    public static int convertToUTF8(final CharSequence charSeq, final int charSeqOff, final int charSeqLength, final byte[] targetBuf, final int targetIdx, final int targetMask) {
        /**
         * 
         * Converts CharSequence (base class of String) into UTF-8 encoded bytes and writes those bytes to an array.
         * The write loops around the end using the targetMask so the returned length must be checked after the call
         * to determine if and overflow occurred. 
         * 
         * Due to the variable nature of converting chars into bytes there is not easy way to know before walking how
         * many bytes will be needed.  To prevent any overflow ensure that you have 6*lengthOfCharSequence bytes available.
         * 
         */
    	
    	int target = targetIdx;				
        int c = 0;
        while (c < charSeqLength) {
        	target = encodeSingleChar((int) charSeq.charAt(charSeqOff+c++), targetBuf, targetMask, target);
        }
        //NOTE: the above loop will keep looping around the target buffer until done and will never cause an array out of bounds.
        //      the length returned however will be larger than targetMask, this should be treated as an error.
        return target-targetIdx;//length;
    }

    public static <S extends MessageSchema> void appendFragment(Pipe<S> input, Appendable target, int cursor) {
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
            PipeReader.log.error("Unable to build text for fragment.",ioe);
            throw new RuntimeException(ioe);
        }
    }

    public static <S extends MessageSchema> ByteBuffer readBytes(Pipe<S> ring, ByteBuffer target, int meta, int len) {
		if (meta < 0) {
	        return readBytesConst(ring,len,target,PipeReader.POS_CONST_MASK & meta);
	    } else {
	        return readBytesRing(ring,len,target,restorePosition(ring,meta));
	    }
	}

    public static <S extends MessageSchema> void readBytes(Pipe<S> ring, byte[] target, int targetIdx, int targetMask, int meta, int len) {
		if (meta < 0) {
			//NOTE: constByteBuffer does not wrap so we do not need the mask
			copyBytesFromToRing(ring.blobConstBuffer, PipeReader.POS_CONST_MASK & meta, 0xFFFFFFFF, target, targetIdx, targetMask, len);
	    } else {
			copyBytesFromToRing(ring.blobRing,restorePosition(ring,meta),ring.byteMask,target,targetIdx,targetMask,len);
	    }
	}

	private static <S extends MessageSchema> ByteBuffer readBytesRing(Pipe<S> ring, int len, ByteBuffer target, int pos) {
		int mask = ring.byteMask;
		byte[] buffer = ring.blobRing;

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

	private static <S extends MessageSchema> ByteBuffer readBytesConst(Pipe<S> ring, int len, ByteBuffer target, int pos) {
	    	target.put(ring.blobConstBuffer, pos, len);
	        return target;
	    }

	public static <S extends MessageSchema> Appendable readASCII(Pipe<S> ring, Appendable target,	int meta, int len) {
		if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
	        return readASCIIConst(ring,len,target,PipeReader.POS_CONST_MASK & meta);
	    } else {
	        return readASCIIRing(ring,len,target,restorePosition(ring, meta));
	    }
	}

	public static <S extends MessageSchema> boolean isEqual(Pipe<S> ring, CharSequence charSeq, int meta, int len) {
		if (len!=charSeq.length()) {
			return false;
		}
		if (meta < 0) {

			int pos = PipeReader.POS_CONST_MASK & meta;

	    	byte[] buffer = ring.blobConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	    		if (charSeq.charAt(len)!=buffer[pos+len]) {
	    			return false;
	    		}
	        }

		} else {

			byte[] buffer = ring.blobRing;
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

	   public static <S extends MessageSchema> boolean isEqual(Pipe<S> ring, byte[] expected, int expectedPos, int meta, int len) {
	        if (len>(expected.length-expectedPos)) {
	            return false;
	        }
	        if (meta < 0) {

	            int pos = PipeReader.POS_CONST_MASK & meta;

	            byte[] buffer = ring.blobConstBuffer;
	            assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	            
	            while (--len >= 0) {
	                if (expected[expectedPos+len]!=buffer[pos+len]) {
	                    return false;
	                }
	            }

	        } else {

	            byte[] buffer = ring.blobRing;
	            int mask = ring.byteMask;
	            int pos = restorePosition(ring, meta);

	            while (--len >= 0) {
	                if (expected[expectedPos+len]!=buffer[mask&(pos+len)]) {
	                    return false;
	                }
	            }

	        }

	        return true;
	    }
	
	private static <S extends MessageSchema> Appendable readASCIIRing(Pipe<S> ring, int len, Appendable target, int pos) {
		byte[] buffer = ring.blobRing;
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

	private static <S extends MessageSchema> Appendable readASCIIConst(Pipe<S> ring, int len, Appendable target, int pos) {
	    try {
	    	byte[] buffer = ring.blobConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	            target.append((char)buffer[pos++]);
	        }
	    } catch (IOException e) {
	       throw new RuntimeException(e);
	    }
	    return target;
	}

	public static <S extends MessageSchema> Appendable readUTF8(Pipe<S> ring, Appendable target, int meta, int len) { //TODO: update to use generics
		if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
	        return readUTF8Const(ring,len,target,PipeReader.POS_CONST_MASK & meta);
	    } else {
	        return readUTF8Ring(ring,len,target,restorePosition(ring,meta));
	    }
	}

	private static <S extends MessageSchema> Appendable readUTF8Const(Pipe<S> ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(ring.blobConstBuffer, charAndPos, 0xFFFFFFFF); //constants do not wrap
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

	private static <S extends MessageSchema> Appendable readUTF8Ring(Pipe<S> ring, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(ring.blobRing, charAndPos, ring.byteMask);
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

	public static <S extends MessageSchema> void addDecimalAsASCII(int readDecimalExponent,	long readDecimalMantissa, Pipe<S> outputRing) {
		long ones = (long)(readDecimalMantissa*PipeReader.powdi[64 + readDecimalExponent]);
		validateVarLength(outputRing, 21);
		int max = 21 + outputRing.blobRingHead.byteWorkingHeadPos.value;
		int len = leftConvertLongToASCII(outputRing, ones, max);
		outputRing.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(len + outputRing.blobRingHead.byteWorkingHeadPos.value);

		copyASCIIToBytes(".", outputRing);

		long frac = Math.abs(readDecimalMantissa - (long)(ones/PipeReader.powdi[64 + readDecimalExponent]));

		validateVarLength(outputRing, 21);
		int max1 = 21 + outputRing.blobRingHead.byteWorkingHeadPos.value;
		int len1 = leftConvertLongWithLeadingZerosToASCII(outputRing, readDecimalExponent, frac, max1);
		outputRing.blobRingHead.byteWorkingHeadPos.value = Pipe.BYTES_WRAP_MASK&(len1 + outputRing.blobRingHead.byteWorkingHeadPos.value);

		//may require trailing zeros
		while (len1<readDecimalExponent) {
			copyASCIIToBytes("0",outputRing);
			len1++;
		}


	}

	public static <S extends MessageSchema> void addLongAsASCII(Pipe<S> outputRing, long value) {
		validateVarLength(outputRing, 21);
		int max = 21 + outputRing.blobRingHead.byteWorkingHeadPos.value;
		int len = leftConvertLongToASCII(outputRing, value, max);
		addBytePosAndLen(outputRing, outputRing.blobRingHead.byteWorkingHeadPos.value, len);
		outputRing.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(len + outputRing.blobRingHead.byteWorkingHeadPos.value);
	}

	public static <S extends MessageSchema> void addIntAsASCII(Pipe<S> outputRing, int value) {
		validateVarLength(outputRing, 12);
		int max = 12 + outputRing.blobRingHead.byteWorkingHeadPos.value;
		int len = leftConvertIntToASCII(outputRing, value, max);
		addBytePosAndLen(outputRing, outputRing.blobRingHead.byteWorkingHeadPos.value, len);
		outputRing.blobRingHead.byteWorkingHeadPos.value = Pipe.BYTES_WRAP_MASK&(len + outputRing.blobRingHead.byteWorkingHeadPos.value);
	}

	/**
     * All bytes even those not yet committed.
     *
     * @param ringBuffer
     * @return
     */
	public static <S extends MessageSchema> int bytesOfContent(Pipe<S> ringBuffer) {
		int dif = (ringBuffer.byteMask&ringBuffer.blobRingHead.byteWorkingHeadPos.value) - (ringBuffer.byteMask&PaddedInt.get(ringBuffer.blobRingTail.bytesTailPos));
		return ((dif>>31)<<ringBuffer.bitsOfBlogRing)+dif;
	}

	public static <S extends MessageSchema> void validateBatchSize(Pipe<S> rb, int size) {
		int maxBatch = computeMaxBatchSize(rb);
		if (size>maxBatch) {
			throw new UnsupportedOperationException("For the configured ring buffer the batch size can be no larger than "+maxBatch);
		}
	}

	public static <S extends MessageSchema> int computeMaxBatchSize(Pipe<S> rb) {
		return computeMaxBatchSize(rb,2);//default mustFit of 2
	}

	public static <S extends MessageSchema> int computeMaxBatchSize(Pipe<S> rb, int mustFit) {
		assert(mustFit>=1);
		int maxBatchFromBytes = rb.maxAvgVarLen==0?Integer.MAX_VALUE:(rb.sizeOfBlobRing/rb.maxAvgVarLen)/mustFit;
		int maxBatchFromPrimary = (rb.sizeOfSlabRing/FieldReferenceOffsetManager.maxFragmentSize(from(rb)))/mustFit;
		return Math.min(maxBatchFromBytes, maxBatchFromPrimary);
	}


	public static <S extends MessageSchema> void publishEOF(Pipe<S> pipe) {

		assert(pipe.slabRingTail.tailPos.get()+pipe.sizeOfSlabRing>=pipe.slabRingHead.headPos.get()+Pipe.EOF_SIZE) : "Must block first to ensure we have 2 spots for the EOF marker";

		PaddedInt.set(pipe.blobRingHead.bytesHeadPos,pipe.blobRingHead.byteWorkingHeadPos.value);
		pipe.slabRing[pipe.mask &((int)pipe.slabRingHead.workingHeadPos.value +  from(pipe).templateOffset)]    = -1;
		pipe.slabRing[pipe.mask &((int)pipe.slabRingHead.workingHeadPos.value +1 +  from(pipe).templateOffset)] = 0;

		pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value = pipe.slabRingHead.workingHeadPos.value + Pipe.EOF_SIZE);

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

	public static <S extends MessageSchema> int leftConvertIntToASCII(Pipe<S> rb, int value, int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.blobRing;
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
		if (idx!=rb.blobRingHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[rb.byteMask & (s+rb.blobRingHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

	public static <S extends MessageSchema> int leftConvertLongToASCII(Pipe<S> rb, long value,	int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = rb.blobRing;
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
		if (idx!=rb.blobRingHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[rb.byteMask & (s+rb.blobRingHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

   public static <S extends MessageSchema> int leftConvertLongWithLeadingZerosToASCII(Pipe<S> rb, int chars, long value, int idx) {
        //max places is value for -2B therefore its 11 places so we start out that far and work backwards.
        //this will leave a gap but that is not a problem.
        byte[] target = rb.blobRing;
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
        if (idx!=rb.blobRingHead.byteWorkingHeadPos.value) {
            int s = 0;
            while (s<length) {
                target[rb.byteMask & (s+rb.blobRingHead.byteWorkingHeadPos.value)] = target[rb.byteMask & (s+idx)];
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

	public static <S extends MessageSchema> int copyASCIIToBytes(CharSequence source, Pipe<S> rbRingBuffer) {
		return copyASCIIToBytes(source, 0, source.length(), rbRingBuffer);
	}

	public static <S extends MessageSchema> void addASCII(CharSequence source, Pipe<S> rb) {
	    addASCII(source, 0, null==source ? -1 : source.length(), rb);
	}

	public static <S extends MessageSchema> void addASCII(CharSequence source, int sourceIdx, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static <S extends MessageSchema> void addASCII(char[] source, int sourceIdx, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static <S extends MessageSchema> int copyASCIIToBytes(CharSequence source, int sourceIdx, final int sourceLen, Pipe<S> rbRingBuffer) {
		final int p = rbRingBuffer.blobRingHead.byteWorkingHeadPos.value;
		//TODO: revisit this not sure this conditional is required
	    if (sourceLen > 0) {
	    	int tStart = p & rbRingBuffer.byteMask;
	        copyASCIIToBytes2(source, sourceIdx, sourceLen, rbRingBuffer, p, rbRingBuffer.blobRing, tStart, 1+rbRingBuffer.byteMask - tStart);
	    }
		return p;
	}

	private static <S extends MessageSchema> void copyASCIIToBytes2(CharSequence source, int sourceIdx,
			final int sourceLen, Pipe<S> rbRingBuffer, final int p,
			byte[] target, int tStart, int len1) {
		if (len1>=sourceLen) {
			Pipe.copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
		} else {
		    // done as two copies
		    Pipe.copyASCIIToByte(source, sourceIdx, target, tStart, len1);
		    Pipe.copyASCIIToByte(source, sourceIdx + len1, target, 0, sourceLen - len1);
		}
		rbRingBuffer.blobRingHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(p + sourceLen);
	}

    public static <S extends MessageSchema> int copyASCIIToBytes(char[] source, int sourceIdx, final int sourceLen, Pipe<S> rbRingBuffer) {
		final int p = rbRingBuffer.blobRingHead.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.byteMask;
	    	byte[] target = rbRingBuffer.blobRing;

	        int tStart = p & targetMask;
	        int len1 = 1+targetMask - tStart;

			if (len1>=sourceLen) {
				copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    copyASCIIToByte(source, sourceIdx, target, tStart, 1+ targetMask - tStart);
			    copyASCIIToByte(source, sourceIdx + len1, target, 0, sourceLen - len1);
			}
	        rbRingBuffer.blobRingHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(p + sourceLen);
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

	public static <S extends MessageSchema> void addUTF8(CharSequence source, Pipe<S> rb) {
	    addUTF8(source, null==source? -1 : source.length(), rb);
	}

	public static <S extends MessageSchema> void addUTF8(CharSequence source, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, rb.blobRingHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,sourceCharCount,rb));
	}

	public static <S extends MessageSchema> void addUTF8(char[] source, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, rb.blobRingHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,sourceCharCount,rb));
	}

	/**
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
	public static <S extends MessageSchema> int copyUTF8ToByte(CharSequence source, int sourceCharCount, Pipe<S> rb) {
	    if (sourceCharCount>0) {
    		int byteLength = Pipe.copyUTF8ToByte(source, 0, rb.blobRing, rb.byteMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
    		rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
    		return byteLength;
	    } else {
	        return 0;
	    }
	}

   public static <S extends MessageSchema> int copyUTF8ToByte(CharSequence source, int sourceOffset, int sourceCharCount, Pipe<S> rb) {
        if (sourceCharCount>0) {
            int byteLength = Pipe.copyUTF8ToByte(source, sourceOffset, rb.blobRing, rb.byteMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
            rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
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
	public static <S extends MessageSchema> int copyUTF8ToByte(char[] source, int sourceCharCount, Pipe<S> rb) {
		int byteLength = Pipe.copyUTF8ToByte(source, 0, rb.blobRing, rb.byteMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
		rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
		return byteLength;
	}

	public static <S extends MessageSchema> int copyUTF8ToByte(char[] source, int sourceOffset, int sourceCharCount, Pipe<S> rb) {
	    int byteLength = Pipe.copyUTF8ToByte(source, sourceOffset, rb.blobRing, rb.byteMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
	    rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
	    return byteLength;
	}

	private static <S extends MessageSchema> int copyUTF8ToByte(char[] source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {

	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source[sourceIdx+c++], target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}




	public static <S extends MessageSchema> int encodeSingleChar(int c, byte[] buffer,int mask, int pos) {

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

	private static <S extends MessageSchema> int rareEncodeCase(int c, byte[] buffer, int mask, int pos) {
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

	public static <S extends MessageSchema> void addByteBuffer(ByteBuffer source, Pipe<S> pipe) {
	    int bytePos = pipe.blobRingHead.byteWorkingHeadPos.value;
	    int len = -1;
	    if (null!=source && source.hasRemaining()) {
	        len = source.remaining();
	        copyByteBuffer(source,source.remaining(),pipe);
	    }
	    //System.out.println("len to write "+len+" text:"+  readUTF8Ring(pipe, len, new StringBuilder(), bytePos));

	    Pipe.addBytePosAndLen(pipe, bytePos, len);
	}

   public static <S extends MessageSchema> void addByteBuffer(ByteBuffer source, int length, Pipe<S> rb) {
        int bytePos = rb.blobRingHead.byteWorkingHeadPos.value;
        int len = -1;
        if (null!=source && length>0) {
            len = length;
            copyByteBuffer(source,length,rb);
        }
        Pipe.addBytePosAndLen(rb, bytePos, len);
    }
	   
	public static <S extends MessageSchema> void copyByteBuffer(ByteBuffer source, int length, Pipe<S> rb) {
		validateVarLength(rb, length);
		int idx = rb.blobRingHead.byteWorkingHeadPos.value & rb.byteMask;
		int partialLength = 1 + rb.byteMask - idx;
		//may need to wrap around ringBuffer so this may need to be two copies
		if (partialLength>=length) {
		    source.get(rb.blobRing, idx, length);
		} else {
		    //read from source and write into byteBuffer
		    source.get(rb.blobRing, idx, partialLength);
		    source.get(rb.blobRing, 0, length - partialLength);
		}
		rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value + length);
	}

	public static <S extends MessageSchema> void addByteArrayWithMask(final Pipe<S> outputRing, int mask, int len, byte[] data, int offset) {
		validateVarLength(outputRing, len);
		copyBytesFromToRing(data,offset,mask,outputRing.blobRing,outputRing.blobRingHead.byteWorkingHeadPos.value,outputRing.byteMask, len);
		addBytePosAndLen(outputRing, outputRing.blobRingHead.byteWorkingHeadPos.value, len);
		outputRing.blobRingHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(outputRing.blobRingHead.byteWorkingHeadPos.value + len);
	}

	public static <S extends MessageSchema> int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static <S extends MessageSchema> long peekLong(int[] buf, long pos, int mask) {

        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    public static <S extends MessageSchema> boolean isShutdown(Pipe<S> ring) {
    	return ring.imperativeShutDown.get();
    }

    public static <S extends MessageSchema> void shutdown(Pipe<S> ring) {
    	if (!ring.imperativeShutDown.getAndSet(true)) {
    		ring.firstShutdownCaller = new PipeException("Shutdown called");
    	}

    }

    public static <S extends MessageSchema> void addByteArray(byte[] source, int sourceIdx, int sourceLen, Pipe<S> rbRingBuffer) {

    	assert(sourceLen>=0);
    	validateVarLength(rbRingBuffer, sourceLen);

    	copyBytesFromToRing(source, sourceIdx, Integer.MAX_VALUE, rbRingBuffer.blobRing, rbRingBuffer.blobRingHead.byteWorkingHeadPos.value, rbRingBuffer.byteMask, sourceLen);

    	addBytePosAndLen(rbRingBuffer, rbRingBuffer.blobRingHead.byteWorkingHeadPos.value, sourceLen);
        rbRingBuffer.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rbRingBuffer.blobRingHead.byteWorkingHeadPos.value + sourceLen);

    }

    public static <S extends MessageSchema> void addNullByteArray(Pipe<S> rbRingBuffer) {
        addBytePosAndLen(rbRingBuffer, rbRingBuffer.blobRingHead.byteWorkingHeadPos.value, -1);
    }


    public static <S extends MessageSchema> void addIntValue(int value, Pipe<S> rb) {
         assert(rb.slabRingHead.workingHeadPos.value <= rb.mask+Pipe.tailPosition(rb));
		 setValue(rb.slabRing,rb.mask,rb.slabRingHead.workingHeadPos.value++,value);
	}

	//TODO: B, need to update build server to ensure this runs on both Java6 and Java ME 8

    //must be called by low-level API when starting a new message
    public static <S extends MessageSchema> int addMsgIdx(Pipe<S> rb, int msgIdx) {
         assert(rb.slabRingHead.workingHeadPos.value <= rb.mask+Pipe.tailPosition(rb));
    	 assert(msgIdx>=0) : "Call publishEOF() instead of this method";

     	//this MUST be done here at the START of a message so all its internal fragments work with the same base position
     	 markBytesWriteBase(rb);

   // 	 assert(rb.llwNextHeadTarget<=rb.headPos.get() || rb.workingHeadPos.value<=rb.llwNextHeadTarget) : "Unsupported mix of high and low level API.";

		 rb.slabRing[rb.mask & (int)rb.slabRingHead.workingHeadPos.value++] = msgIdx;
		 return Pipe.from(rb).fragDataSize[msgIdx];
		 
	}

	public static <S extends MessageSchema> void setValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] = value;
    }


    public static <S extends MessageSchema> void addBytePosAndLen(Pipe<S> ring, int position, int length) {
        assert(ring.slabRingHead.workingHeadPos.value <= ring.mask+Pipe.tailPosition(ring));
		setBytePosAndLen(ring.slabRing, ring.mask, ring.slabRingHead.workingHeadPos.value, position, length, Pipe.bytesWriteBase(ring));
		ring.slabRingHead.workingHeadPos.value+=2;
    }

    public static <S extends MessageSchema> void addBytePosAndLenSpecial(Pipe<S> targetOutput, final int startBytePos, int bytesLength) {
        PaddedLong workingHeadPos = getWorkingHeadPositionObject(targetOutput);
        setBytePosAndLen(primaryBuffer(targetOutput), targetOutput.mask, workingHeadPos.value, startBytePos, bytesLength, bytesWriteBase(targetOutput));
        PaddedLong.add(workingHeadPos, 2);
    }

	public static <S extends MessageSchema> void setBytePosAndLen(int[] buffer, int rbMask, long ringPos,	int positionDat, int lengthDat, int baseBytePos) {
	   	//negative position is written as is because the internal array does not have any offset (but it could some day)
    	//positive position is written after subtracting the rbRingBuffer.bytesHeadPos.longValue()
    	if (positionDat>=0) {
    		buffer[rbMask & (int)ringPos] = (int)(positionDat-baseBytePos) & Pipe.BYTES_WRAP_MASK; //mask is needed for the negative case, does no harm in positive case
    	} else {
    		buffer[rbMask & (int)ringPos] = positionDat;
    	}
        buffer[rbMask & (int)(ringPos+1)] = lengthDat;
	}

	public static <S extends MessageSchema> int restorePosition(Pipe<S> ring, int pos) {
		assert(pos>=0);
		return pos + Pipe.bytesReadBase(ring);

	}

    public static <S extends MessageSchema> int bytePosition(int meta, Pipe<S> ring, int len) {
    	int pos =  restorePosition(ring, meta & RELATIVE_POS_MASK);    	
        if (len>=0) {
            ring.blobRingTail.byteWorkingTailPos.value =  BYTES_WRAP_MASK&(len+ring.blobRingTail.byteWorkingTailPos.value);
        }        
        return pos;
    }

    public static <S extends MessageSchema> int bytePositionGen(int meta, Pipe<S> ring) {
    	return restorePosition(ring, meta & RELATIVE_POS_MASK);
    }


    public static <S extends MessageSchema> void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2, int value3) {

        long p = headCache.value;
        buffer[rbMask & (int)p++] = value1;
        buffer[rbMask & (int)p++] = value2;
        buffer[rbMask & (int)p++] = value3;
        headCache.value = p;

    }

    @Deprecated
    public static <S extends MessageSchema> void addValues(int[] buffer, int rbMask, PaddedLong headCache, int value1, long value2) {

        headCache.value = setValues(buffer, rbMask, headCache.value, value1, value2);

    }

    public static <S extends MessageSchema> void addDecimal(int exponent, long mantissa, Pipe<S> ring) {
        ring.slabRingHead.workingHeadPos.value = setValues(ring.slabRing, ring.mask, ring.slabRingHead.workingHeadPos.value, exponent, mantissa);
    }


	static <S extends MessageSchema> long setValues(int[] buffer, int rbMask, long pos, int value1, long value2) {
		buffer[rbMask & (int)pos++] = value1;
        buffer[rbMask & (int)pos++] = (int)(value2 >>> 32);
        buffer[rbMask & (int)pos++] = (int)(value2 & 0xFFFFFFFF);
		return pos;
	}

	@Deprecated //use addLongVlue(value, rb)
    public static <S extends MessageSchema> void addLongValue(Pipe<S> rb, long value) {
		 addLongValue(value, rb);
	}

	public static <S extends MessageSchema> void addLongValue(long value, Pipe<S> rb) {
		 addLongValue(rb.slabRing, rb.mask, rb.slabRingHead.workingHeadPos, value);
	}

    public static <S extends MessageSchema> void addLongValue(int[] buffer, int rbMask, PaddedLong headCache, long value) {

        long p = headCache.value;
        buffer[rbMask & (int)p] = (int)(value >>> 32);
        buffer[rbMask & (int)(p+1)] = ((int)value);
        headCache.value = p+2;

    }

    static <S extends MessageSchema> int readRingByteLen(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int) (rbMask & (rbPos + fieldPos + 1))];// second int is always the length
    }

	public static <S extends MessageSchema> int readRingByteLen(int idx, Pipe<S> ring) {
		return readRingByteLen(idx,ring.slabRing, ring.mask, ring.slabRingTail.workingTailPos.value);
	}

	public static <S extends MessageSchema> int takeRingByteLen(Pipe<S> ring) {
	//    assert(ring.structuredLayoutRingTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
		return ring.slabRing[(int)(ring.mask & (ring.slabRingTail.workingTailPos.value++))];// second int is always the length
	}



    public static <S extends MessageSchema> byte[] byteBackingArray(int meta, Pipe<S> rbRingBuffer) {
        return rbRingBuffer.blobRingLookup[meta>>>31];
    }

	public static <S extends MessageSchema> int readRingByteMetaData(int pos, Pipe<S> rb) {
		return readValue(pos,rb.slabRing,rb.mask,rb.slabRingTail.workingTailPos.value);
	}

	//TODO: must always read metadata before length, easy mistake to make, need assert to ensure this is caught if happens.
	public static <S extends MessageSchema> int takeRingByteMetaData(Pipe<S> ring) {
	//    assert(ring.structuredLayoutRingTail.workingTailPos.value<RingBuffer.workingHeadPosition(ring));
		return readValue(0,ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value++);
	}

    static <S extends MessageSchema> int readValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }

    public static <S extends MessageSchema> int readValue(int idx, Pipe<S> ring) {
    	return readValue(idx, ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value);
    }

    public static <S extends MessageSchema> int takeValue(Pipe<S> ring) {
    	return readValue(0, ring.slabRing, ring.mask, ring.slabRingTail.workingTailPos.value++);
    }

    public static <S extends MessageSchema> long takeLong(Pipe<S> ring) {
        assert(ring.slabRingTail.workingTailPos.value<Pipe.workingHeadPosition(ring)) : "working tail "+ring.slabRingTail.workingTailPos.value+" but head is "+Pipe.workingHeadPosition(ring);
    	long result = readLong(ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value);
    	ring.slabRingTail.workingTailPos.value+=2;
    	return result;
    }

    public static <S extends MessageSchema> long readLong(int idx, Pipe<S> ring) {
    	return readLong(ring.slabRing,ring.mask,idx+ring.slabRingTail.workingTailPos.value);

    }

    public static <S extends MessageSchema> int takeMsgIdx(Pipe<S> ring) {
        
        /**
         * TODO: mask the result int to only the bits which contain the msgId.
         *       The other bits can bet retrieved by the getMessagePackedBits
         *       The limit for the byte length is also known so there is 
         *       another method getFragmentPackedBits which come from the tail.
         *   
         *       This bits must be defined in the template/FROM and bounds checked at compile time.
         * 
         */
        
        
        assert(ring.slabRingTail.workingTailPos.value<Pipe.workingHeadPosition(ring)) : " tail is "+ring.slabRingTail.workingTailPos.value+" but head is "+Pipe.workingHeadPosition(ring);
    	return readValue(0, ring.slabRing, ring.mask, ring.slabRingTail.workingTailPos.value++);
    }

    public static <S extends MessageSchema> int peekInt(Pipe<S> ring) {
        return readValue(0, ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value);
    }
    
    public static <S extends MessageSchema> int peekInt(Pipe<S> ring, int offset) {
        return readValue(0, ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value+offset);
    }
   
    public static <S extends MessageSchema> long peekLong(Pipe<S> ring, int offset) {
        return readLong(ring.slabRing,ring.mask,ring.slabRingTail.workingTailPos.value+offset);
    }
    
    
    public static <S extends MessageSchema> int contentRemaining(Pipe<S> rb) {
        return (int)(rb.slabRingHead.headPos.get() - rb.slabRingTail.tailPos.get()); //must not go past add count because it is not release yet.
    }


    //TODO: AAA rename as releaseReadLock
    public static <S extends MessageSchema> int releaseReads(Pipe<S> ring) {
        int len = takeValue(ring);
        Pipe.markBytesReadBase(ring, len);
    	batchedReleasePublish(ring);
    	return len;
    }


    public static <S extends MessageSchema> void batchedReleasePublish(Pipe<S> ring) {
        assert(Pipe.contentRemaining(ring)>=0);
        batchedReleasePublish(ring, ring.blobRingTail.byteWorkingTailPos.value, ring.slabRingTail.workingTailPos.value);
    }

    public static <S extends MessageSchema> void batchedReleasePublish(Pipe<S> ring, int byteWorkingTailPos, long workingTailPos) {
        if ((--ring.batchReleaseCountDown<=0) ) {

    	    assert(ring.ringWalker.cursor<=0 && !PipeReader.isNewMessage(ring.ringWalker)) : "Unsupported mix of high and low level API.  ";

    	    Pipe.setBytesTail(ring,byteWorkingTailPos);
    	    ring.slabRingTail.tailPos.lazySet(workingTailPos);
    	    
    	    ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;

    	} else {
    	    storeUnpublishedTail(ring, workingTailPos, byteWorkingTailPos);
    	}
    }


    static <S extends MessageSchema> void storeUnpublishedTail(Pipe<S> ring, long workingTailPos, int byteWorkingTailPos) {
        ring.lastReleasedBlobTail = byteWorkingTailPos;
        ring.lastReleasedSlabTail = workingTailPos;
    }
    
    static <S extends MessageSchema> void releaseReadLockForHighLevelAPI(Pipe<S> ring) {

        
        assert(Pipe.isReplaying(ring) || ring.ringWalker.nextWorkingTail!=Pipe.getWorkingTailPosition(ring)) : "Only call release once per message";
        //not sure if this is always true must check.
 //       assert(Pipe.isReplaying(ring) || ring.lastReleasedSlabTail != ring.ringWalker.nextWorkingTail) : "Only call release once per message";

        //take new tail position and make it the base because we are about to start a new message.        
        Pipe.markBytesReadBase(ring);
        
        if (decBatchRelease(ring)<=0) {

                 Pipe.setBytesTail(ring,Pipe.getWorkingBlobRingTailPosition(ring));
                 Pipe.publishWorkingTailPosition(ring, ring.ringWalker.nextWorkingTail);

                 ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;
        } else {
                 ring.lastReleasedBlobTail = ring.blobRingTail.byteWorkingTailPos.value;
                 ring.lastReleasedSlabTail = ring.ringWalker.nextWorkingTail;// ring.primaryBufferTail.workingTailPos.value;
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



    /**
     * Release any reads that were held back due to batching.
     * @param ring
     */
    public static <S extends MessageSchema> void releaseAllBatchedReads(Pipe<S> ring) {

        if (ring.lastReleasedSlabTail > ring.slabRingTail.tailPos.get()) {
            PaddedInt.set(ring.blobRingTail.bytesTailPos,ring.lastReleasedBlobTail);
            ring.slabRingTail.tailPos.lazySet(ring.lastReleasedSlabTail);
            ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;
        }

        assert(debugHeadAssignment(ring));
    }
    
    public static <S extends MessageSchema> void releaseBatchedReadReleasesUpToThisPosition(Pipe<S> ring) {
        
        long newTailToPublish = Pipe.getWorkingTailPosition(ring);
        int newTailBytesToPublish = Pipe.getWorkingBlobRingTailPosition(ring);
        
        //int newTailBytesToPublish = RingBuffer.bytesReadBase(ring);
        
        releaseBatchedReadReleasesUpToPosition(ring, newTailToPublish, newTailBytesToPublish);
                
    }

    public static <S extends MessageSchema> void releaseBatchedReadReleasesUpToPosition(Pipe<S> ring, long newTailToPublish,  int newTailBytesToPublish) {
        assert(newTailToPublish<=ring.lastReleasedSlabTail) : "This new value is forward of the next Release call, eg its too large";
        assert(newTailToPublish>=ring.slabRingTail.tailPos.get()) : "This new value is behind the existing published Tail, eg its too small ";
        
//        //TODO: These two asserts would be nice to have but the int of bytePos wraps every 2 gig causing false positives, these need more mask logic to be right
//        assert(newTailBytesToPublish<=ring.lastReleasedBytesTail) : "This new value is forward of the next Release call, eg its too large";
//        assert(newTailBytesToPublish>=ring.unstructuredLayoutRingTail.bytesTailPos.value) : "This new value is behind the existing published Tail, eg its too small ";
//        assert(newTailBytesToPublish<=ring.bytesWorkingTailPosition(ring)) : "Out of bounds should never be above working tail";
//        assert(newTailBytesToPublish<=ring.bytesHeadPosition(ring)) : "Out of bounds should never be above head";
        
        
        PaddedInt.set(ring.blobRingTail.bytesTailPos, newTailBytesToPublish);
        ring.slabRingTail.tailPos.lazySet(newTailToPublish);
        ring.batchReleaseCountDown = ring.batchReleaseCountDownInit;
    }

    @Deprecated
	public static <S extends MessageSchema> void releaseAll(Pipe<S> ring) {

			int i = ring.blobRingTail.byteWorkingTailPos.value= ring.blobRingHead.byteWorkingHeadPos.value;
            PaddedInt.set(ring.blobRingTail.bytesTailPos,i);
			ring.slabRingTail.tailPos.lazySet(ring.slabRingTail.workingTailPos.value= ring.slabRingHead.workingHeadPos.value);

    }

    @Deprecated
    public static <S extends MessageSchema> void dump(Pipe<S> rb) {

        // move the removePosition up to the addPosition
        // new Exception("WARNING THIS IS NO LONGER COMPATIBLE WITH PUMP CALLS").printStackTrace();
        rb.slabRingTail.tailPos.lazySet(rb.slabRingTail.workingTailPos.value = rb.slabRingHead.workingHeadPos.value);
    }


    /**
     * Low level API for publish
     * @param ring
     */
    public static <S extends MessageSchema> void publishWrites(Pipe<S> ring) {
    	//new Exception("publish trialing byte").printStackTrace();
    	//happens at the end of every fragment
        writeTrailingCountOfBytesConsumed(ring, ring.slabRingHead.workingHeadPos.value++); //increment because this is the low-level API calling

		publishWritesBatched(ring);
    }

    public static <S extends MessageSchema> void publishWritesBatched(Pipe<S> ring) {
        //single length field still needs to move this value up, so this is always done
		ring.blobWriteLastConsumedPos = ring.blobRingHead.byteWorkingHeadPos.value;

    	assert(ring.slabRingHead.workingHeadPos.value >= Pipe.headPosition(ring));
    	assert(ring.llWrite.llwConfirmedWrittenPosition<=Pipe.headPosition(ring) || ring.slabRingHead.workingHeadPos.value<=ring.llWrite.llwConfirmedWrittenPosition) : "Unsupported mix of high and low level API. NextHead>head and workingHead>nextHead";

    	//TODO: uncomment and add this new check!!
    	//assert(ring.llRead.llwConfirmedReadPosition == Pipe.workingHeadPosition(ring));
    	
    	
    	publishHeadPositions(ring);
    }

    /**
     * Publish any writes that were held back due to batching.
     * @param ring
     */
    public static <S extends MessageSchema> void publishAllBatchedWrites(Pipe<S> ring) {

    	if (ring.lastPublishedSlabRingHead>ring.slabRingHead.headPos.get()) {
    		PaddedInt.set(ring.blobRingHead.bytesHeadPos,ring.lastPublishedBlobRingHead);
    		ring.slabRingHead.headPos.lazySet(ring.lastPublishedSlabRingHead);
    	}

		assert(debugHeadAssignment(ring));
		ring.batchPublishCountDown = ring.batchPublishCountDownInit;
    }


	private static <S extends MessageSchema> boolean debugHeadAssignment(Pipe<S> ring) {

		if (0!=(PipeConfig.SHOW_HEAD_PUBLISH&ring.debugFlags) ) {
			new Exception("Debug stack for assignment of published head positition"+ring.slabRingHead.headPos.get()).printStackTrace();
		}
		return true;
	}


	public static <S extends MessageSchema> void publishHeadPositions(Pipe<S> ring) {

	    //TODO: need way to test if publish was called on an input ? may be much easer to detect missing publish. or extra release.
	    if ((--ring.batchPublishCountDown<=0)) {
	        PaddedInt.set(ring.blobRingHead.bytesHeadPos,ring.blobRingHead.byteWorkingHeadPos.value);
	        ring.slabRingHead.headPos.lazySet(ring.slabRingHead.workingHeadPos.value);
	        assert(debugHeadAssignment(ring));
	        ring.batchPublishCountDown = ring.batchPublishCountDownInit;
	    } else {
	        storeUnpublishedHead(ring);
	    }
	}

	static <S extends MessageSchema> void storeUnpublishedHead(Pipe<S> ring) {
		ring.lastPublishedBlobRingHead = ring.blobRingHead.byteWorkingHeadPos.value;
		ring.lastPublishedSlabRingHead = ring.slabRingHead.workingHeadPos.value;
	}

    public static <S extends MessageSchema> void abandonWrites(Pipe<S> ring) {
        //ignore the fact that any of this was written to the ring buffer
    	ring.slabRingHead.workingHeadPos.value = ring.slabRingHead.headPos.longValue();
    	ring.blobRingHead.byteWorkingHeadPos.value = PaddedInt.get(ring.blobRingHead.bytesHeadPos);
    	storeUnpublishedHead(ring);
    }

    /**
     * Blocks until there is enough room for this first fragment of the message and records the messageId.
     * @param ring
     * @param msgIdx
     */
	public static <S extends MessageSchema> void blockWriteMessage(Pipe<S> ring, int msgIdx) {
		//before write make sure the tail is moved ahead so we have room to write
	    spinBlockForRoom(ring, Pipe.from(ring).fragDataSize[msgIdx]);
		Pipe.addMsgIdx(ring, msgIdx);
	}


    //All the spin lock methods share the same implementation. Unfortunately these can not call
    //a common implementation because the extra method jump degrades the performance in tight loops
    //where these spin locks are commonly used.

    public static <S extends MessageSchema> void spinBlockForRoom(Pipe<S> ringBuffer, int size) {
        while (!hasRoomForWrite(ringBuffer, size)) {
            spinWork(ringBuffer);
        }
    }

    @Deprecated //use spinBlockForRoom then confirm the write afterwords
    public static <S extends MessageSchema> long spinBlockOnTail(long lastCheckedValue, long targetValue, Pipe<S> ringBuffer) {
    	while (null==ringBuffer.slabRing || lastCheckedValue < targetValue) {
    		spinWork(ringBuffer);
		    lastCheckedValue = ringBuffer.slabRingTail.tailPos.longValue();
		}
		return lastCheckedValue;
    }

    public static <S extends MessageSchema> void spinBlockForContent(Pipe<S> ringBuffer) {
        while (!hasContentToRead(ringBuffer)) {
            spinWork(ringBuffer);
        }
    }

    //Used by RingInputStream to duplicate contract behavior,  TODO: AA rename to waitForAvailableContent or blockUntilContentReady?
    public static <S extends MessageSchema> long spinBlockOnHead(long lastCheckedValue, long targetValue, Pipe<S> ringBuffer) {
    	while ( lastCheckedValue < targetValue) {
    		spinWork(ringBuffer);
		    lastCheckedValue = ringBuffer.slabRingHead.headPos.get();
		}
		return lastCheckedValue;
    }

	private static <S extends MessageSchema> void spinWork(Pipe<S> ringBuffer) {
		Thread.yield();//needed for now but re-evaluate performance impact
		if (isShutdown(ringBuffer) || Thread.currentThread().isInterrupted()) {
			throw null!=ringBuffer.firstShutdownCaller ? ringBuffer.firstShutdownCaller : new PipeException("Unexpected shutdown");
		}
	}

	public static <S extends MessageSchema> int blobMask(Pipe<S> pipe) {
		return pipe.byteMask;
	}
	
	public static <S extends MessageSchema> int slabMask(Pipe<S> pipe) {
	    return pipe.mask;
	}

	public static <S extends MessageSchema> long headPosition(Pipe<S> ring) {
		 return ring.slabRingHead.headPos.get();
	}

    public static <S extends MessageSchema> long workingHeadPosition(Pipe<S> ring) {
        return PaddedLong.get(ring.slabRingHead.workingHeadPos);
    }

    public static <S extends MessageSchema> void setWorkingHead(Pipe<S> ring, long value) {
        PaddedLong.set(ring.slabRingHead.workingHeadPos, value);
    }

    public static <S extends MessageSchema> long addAndGetWorkingHead(Pipe<S> ring, int inc) {
        return PaddedLong.add(ring.slabRingHead.workingHeadPos, inc);
    }

    public static <S extends MessageSchema> long getWorkingTailPosition(Pipe<S> ring) {
        return PaddedLong.get(ring.slabRingTail.workingTailPos);
    }

    public static <S extends MessageSchema> void setWorkingTailPosition(Pipe<S> ring, long value) {
        PaddedLong.set(ring.slabRingTail.workingTailPos, value);
    }

    public static <S extends MessageSchema> long addAndGetWorkingTail(Pipe<S> ring, int inc) {
        return PaddedLong.add(ring.slabRingTail.workingTailPos, inc);
    }


	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param ring
	 * @param workingHeadPos
	 */
	public static <S extends MessageSchema> void publishWorkingHeadPosition(Pipe<S> ring, long workingHeadPos) {
		ring.slabRingHead.headPos.lazySet(ring.slabRingHead.workingHeadPos.value = workingHeadPos);
	}

	public static <S extends MessageSchema> long tailPosition(Pipe<S> ring) {
		return ring.slabRingTail.tailPos.get();
	}



	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param ring
	 * @param workingTailPos
	 */
	public static <S extends MessageSchema> void publishWorkingTailPosition(Pipe<S> ring, long workingTailPos) {
		ring.slabRingTail.tailPos.lazySet(ring.slabRingTail.workingTailPos.value = workingTailPos);
	}

	@Deprecated
	public static <S extends MessageSchema> int primarySize(Pipe<S> ring) {
		return ring.sizeOfSlabRing;
	}

	public static <S extends MessageSchema> FieldReferenceOffsetManager from(Pipe<S> ring) {
		return ring.ringWalker.from;
	}

	public static <S extends MessageSchema> int cursor(Pipe<S> ring) {
        return ring.ringWalker.cursor;
    }

	public static <S extends MessageSchema> void writeTrailingCountOfBytesConsumed(Pipe<S> ring, long pos) {

		int consumed = ring.blobRingHead.byteWorkingHeadPos.value - ring.blobWriteLastConsumedPos;
		//log.trace("wrote {} bytes consumed to position {}",consumed,pos);
		ring.slabRing[ring.mask & (int)pos] = consumed>=0 ? consumed : consumed&BYTES_WRAP_MASK;
		ring.blobWriteLastConsumedPos = ring.blobRingHead.byteWorkingHeadPos.value;

	}

	public static <S extends MessageSchema> IntBuffer wrappedSlabRing(Pipe<S> ring) {
		return ring.wrappedSlabRing;
	}

	public static <S extends MessageSchema> ByteBuffer wrappedBlobRingA(Pipe<S> ring) {
		return ring.wrappedBlobRingA;
	}

    public static <S extends MessageSchema> ByteBuffer wrappedBlobRingB(Pipe<S> ring) {
        return ring.wrappedBlobRingB;
    }

	public static <S extends MessageSchema> ByteBuffer wrappedBlobConstBuffer(Pipe<S> ring) {
		return ring.wrappedBlobConstBuffer;
	}

	/////////////
	//low level API
	////////////



	@Deprecated
	public static <S extends MessageSchema> boolean roomToLowLevelWrite(Pipe<S> output, int size) {
		return hasRoomForWrite(output, size);
	}

	//This holds the last known state of the tail position, if its sufficiently far ahead it indicates that
	//we do not need to fetch it again and this reduces contention on the CAS with the reader.
	//This is an important performance feature of the low level API and should not be modified.
    public static <S extends MessageSchema> boolean hasRoomForWrite(Pipe<S> output, int size) {
        return roomToLowLevelWrite(output, output.llRead.llwConfirmedReadPosition+size);
    }
    
    public static <S extends MessageSchema> boolean hasRoomForWrite(Pipe<S> output) {
        return roomToLowLevelWrite(output, output.llRead.llwConfirmedReadPosition+FieldReferenceOffsetManager.maxFragmentSize(Pipe.from(output)));
    }

    
    
	private static <S extends MessageSchema> boolean roomToLowLevelWrite(Pipe<S> output, long target) {
		//only does second part if the first does not pass
		return (output.llRead.llrTailPosCache >= target) || roomToLowLevelWriteSlow(output, target);
	}

	private static <S extends MessageSchema> boolean roomToLowLevelWriteSlow(Pipe<S> output, long target) {
		return (output.llRead.llrTailPosCache = output.slabRingTail.tailPos.get()) >= target;
	}

	public static <S extends MessageSchema> long confirmLowLevelWrite(Pipe<S> output, int size) { //TOOD: rename
		return output.llRead.llwConfirmedReadPosition += size; //TODO: add check if this size does not match how many written we have a problem.
	}

	//delete nov 2015
	@Deprecated
	public static <S extends MessageSchema> boolean contentToLowLevelRead(Pipe<S> input, int size) {
		return hasContentToRead(input, size);
	}

    public static <S extends MessageSchema> boolean hasContentToRead(Pipe<S> input, int size) {
        return contentToLowLevelRead2(input, input.llWrite.llwConfirmedWrittenPosition+size);
    }
    
    public static <S extends MessageSchema> boolean hasContentToRead(Pipe<S> input) {
        return contentToLowLevelRead2(input, input.llWrite.llwConfirmedWrittenPosition+1);
    }

	private static <S extends MessageSchema> boolean contentToLowLevelRead2(Pipe<S> input, long target) {
		//only does second part if the first does not pass
		return (input.llWrite.llwHeadPosCache >= target) || contentToLowLevelReadSlow(input, target);
	}

	private static <S extends MessageSchema> boolean contentToLowLevelReadSlow(Pipe<S> input, long target) {
		return (input.llWrite.llwHeadPosCache = input.slabRingHead.headPos.get()) >= target;
	}

	public static <S extends MessageSchema> long confirmLowLevelRead(Pipe<S> input, long size) {
	    assert(size>0) : "Must have read something.";
	     //not sure if this assert is true in all cases
	    assert(input.llWrite.llwConfirmedWrittenPosition + size <= input.slabRingHead.workingHeadPos.value+Pipe.EOF_SIZE) : "size was far too large, past known data";
	    assert(input.llWrite.llwConfirmedWrittenPosition + size >= input.slabRingTail.tailPos.get()) : "size was too small, under known data";        
		return (input.llWrite.llwConfirmedWrittenPosition += size);
	}

    public static <S extends MessageSchema> void setWorkingHeadTarget(Pipe<S> input) {
        input.llWrite.llwConfirmedWrittenPosition =  Pipe.getWorkingTailPosition(input);
    }

	public static <S extends MessageSchema> boolean hasReleasePending(Pipe<S> ringBuffer) {
		return ringBuffer.batchReleaseCountDown!=ringBuffer.batchReleaseCountDownInit;
	}

	public static <S extends MessageSchema> int getBlobRingTailPosition(Pipe<S> ring) {
	    return PaddedInt.get(ring.blobRingTail.bytesTailPos);
	}

	//Delete Nov 2015
	@Deprecated
    public static <S extends MessageSchema> int bytesTailPosition(Pipe<S> ring) {
        return getBlobRingTailPosition(ring);
    }

    public static <S extends MessageSchema> void setBytesTail(Pipe<S> ring, int value) {
        PaddedInt.set(ring.blobRingTail.bytesTailPos, value);
    }

    public static <S extends MessageSchema> int bytesHeadPosition(Pipe<S> ring) {
        return PaddedInt.get(ring.blobRingHead.bytesHeadPos);
    }

    public static <S extends MessageSchema> void setBytesHead(Pipe<S> ring, int value) {
        PaddedInt.set(ring.blobRingHead.bytesHeadPos, value);
    }

    public static <S extends MessageSchema> int addAndGetBytesHead(Pipe<S> ring, int inc) {
        return PaddedInt.add(ring.blobRingHead.bytesHeadPos, inc);
    }

    public static <S extends MessageSchema> int getWorkingBlobRingTailPosition(Pipe<S> ring) {
        return PaddedInt.get(ring.blobRingTail.byteWorkingTailPos);
    }

   //Delete Nov 2015
    @Deprecated
    public static <S extends MessageSchema> int bytesWorkingTailPosition(Pipe<S> ring) {
        return getWorkingBlobRingTailPosition(ring);
    }

    public static <S extends MessageSchema> int addAndGetBytesWorkingTailPosition(Pipe<S> ring, int inc) {
        return PaddedInt.maskedAdd(ring.blobRingTail.byteWorkingTailPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    public static <S extends MessageSchema> void setBytesWorkingTail(Pipe<S> ring, int value) {
        PaddedInt.set(ring.blobRingTail.byteWorkingTailPos, value);
    }

    public static <S extends MessageSchema> int bytesWorkingHeadPosition(Pipe<S> ring) {
        return PaddedInt.get(ring.blobRingHead.byteWorkingHeadPos);
    }

    public static <S extends MessageSchema> int addAndGetBytesWorkingHeadPosition(Pipe<S> ring, int inc) {
        return PaddedInt.maskedAdd(ring.blobRingHead.byteWorkingHeadPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    public static <S extends MessageSchema> void setBytesWorkingHead(Pipe<S> ring, int value) {
        PaddedInt.set(ring.blobRingHead.byteWorkingHeadPos, value);
    }

    public static <S extends MessageSchema> int decBatchRelease(Pipe<S> rb) {
        return --rb.batchReleaseCountDown;
    }

    public static <S extends MessageSchema> int decBatchPublish(Pipe<S> rb) {
        return --rb.batchPublishCountDown;
    }

    public static <S extends MessageSchema> void beginNewReleaseBatch(Pipe<S> rb) {
        rb.batchReleaseCountDown = rb.batchReleaseCountDownInit;
    }

    public static <S extends MessageSchema> void beginNewPublishBatch(Pipe<S> rb) {
        rb.batchPublishCountDown = rb.batchPublishCountDownInit;
    }

    public static <S extends MessageSchema> byte[] byteBuffer(Pipe<S> rb) {
        return rb.blobRing;
    }

    public static <S extends MessageSchema> int[] primaryBuffer(Pipe<S> rb) {
        return rb.slabRing;
    }

    public static <S extends MessageSchema> void updateBytesWriteLastConsumedPos(Pipe<S> rb) {
        rb.blobWriteLastConsumedPos = Pipe.bytesWorkingHeadPosition(rb);
    }

    public static <S extends MessageSchema> PaddedLong getWorkingTailPositionObject(Pipe<S> rb) {
        return rb.slabRingTail.workingTailPos;
    }

    public static <S extends MessageSchema> PaddedLong getWorkingHeadPositionObject(Pipe<S> rb) {
        return rb.slabRingHead.workingHeadPos;
    }
}
