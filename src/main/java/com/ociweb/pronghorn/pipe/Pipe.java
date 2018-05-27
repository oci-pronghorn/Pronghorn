package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.PaddedAtomicLong;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


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

public class Pipe<T extends MessageSchema<T>> {

    private static final AtomicInteger pipeCounter = new AtomicInteger(0);
    
    //package protected to be called by low and high level API
    PipePublishListener[] pubListeners = new PipePublishListener[0];
    
    private StructRegistry typeData;

    /**
     * Adds a pub listener to the specified pipe
     * @param p pipe to be used
     * @param listener pub listener to add
     */
    public static void addPubListener(Pipe p, PipePublishListener listener) {
    	p.pubListeners = grow(p.pubListeners, listener);
    }

    /**
     * Removes a pub listener from the specified pipe
     * @param p pipe to be used
     * @param listener pub listener to remove
     */
	public static void removePubListener(Pipe p, PipePublishListener listener) {
    	p.pubListeners = shrink(p.pubListeners, listener);
    }
 	   
	
	private static PipePublishListener[] grow(PipePublishListener[] source, PipePublishListener listener) {
		PipePublishListener[] result = new PipePublishListener[source.length+1];
		System.arraycopy(source, 0, result, 0, source.length);
		result[source.length] = listener;
		return result;
	}
    
    private static PipePublishListener[] shrink(PipePublishListener[] source, PipePublishListener listener) {
    	int matches = 0;
    	int i = source.length;
    	while (--i>=0) {
    		if (source[i]==listener) {
    			matches++;
    		}
    	}
    	PipePublishListener[] result = new PipePublishListener[source.length-matches];
    	i=0;
    	for(int j = 0; j<source.length; j++) {
    		if (source[j]!=listener) {
    			result[i++] = listener;
    		}
    	}
    	return result;
    }


	public static void structRegistry(Pipe p, StructRegistry recordTypeData) {
    	assert(null!=recordTypeData) : "must not be null";
    	assert(null==p.typeData || recordTypeData==p.typeData) :
    		"can not modify type data after setting";
    	p.typeData = recordTypeData;
    }
    
    public static StructRegistry structRegistry(Pipe p) {
    	return p.typeData;
    }
    
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
        long llrConfirmedPosition;

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
        long llwConfirmedPosition;

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
    //NOTE: when the 32nd bit is set we will not pull the value from the ring buffer but instead use the constants array (these are pronouns)
    //NOTE: when the 31nd bit is set this blob is structured.
    public static final int RELATIVE_POS_MASK = 0x3FFFFFFF; //removes flag bits which indicate this is a constant and/or structure
    public static final int STRUCTURED_POS_MASK = 0x40000000; 
    
    static {
    	assert(STRUCTURED_POS_MASK == StructRegistry.IS_STRUCT_BIT);    	
    }
    
    //This mask is here to support the fact that variable-length fields will run out of space because the head/tail are 32 bit ints instead of
    //longs that are used for the structured layout data.  This mask enables the int to wrap back down to zero instead of going negative.
    //this will only happen once for every 2GB written.
    public static final int BYTES_WRAP_MASK   = 0x7FFFFFFF;//NOTE: this trick only works because its 1 bit less than the roll-over sign bit

    //A few corner use cases require a poison pill EOF message to be sent down the pipes to ensure each consumer knows when to shut down.
    //This is here for compatibility with legacy APIs,  This constant is the size of the EOF message.
    public static final int EOF_SIZE = 2;

    //these public fields are fine because they are all final
    public final int id;
    public final int sizeOfSlabRing;
    public final int sizeOfBlobRing;

    public final int slabMask;
    public final int blobMask;
    
    public final byte bitsOfSlabRing;
    public final byte bitsOfBlogRing;
    
    public final int maxVarLen;//to be used when copying data in dense chunks.
    private final T schema;
    
    final boolean usingHighLevelAPI;
    private final PipeConfig<T> config;

    //TODO: B, need to add constant for gap always kept after head and before tail, this is for debug mode to store old state upon error. NEW FEATURE.
    //            the time slices of the graph will need to be kept for all rings to reconstruct history later.


    private final SlabRingHead slabRingHead = new SlabRingHead();
    private final BlobRingHead blobRingHead = new BlobRingHead();

    LowLevelAPIWritePositionCache llWrite; //low level write head pos cache and target
    LowLevelAPIReadPositionCache llRead; //low level read tail pos cache and target

    //TODO: C, add a configuration to disable this construction when we know it s not used.
    StackStateWalker ringWalker;//only needed for high level API
    
    //TODO: C, add a configuration to disable this construction when we know it s not used.
    PendingReleaseData pendingReleases;//only used when we want to release blob data async from our walking of each fragment
    

    final SlabRingTail slabRingTail = new SlabRingTail(); //primary working and public
    private final BlobRingTail blobRingTail = new BlobRingTail(); //primary working and public

    //these values are only modified and used when replay is NOT in use
    //hold the publish position when batching so the batch can be flushed upon shutdown and thread context switches
    private int lastReleasedBlobTail;
    long lastReleasedSlabTail;

    int blobWriteLastConsumedPos = 0;
    private long totalWrittenFragments = 0;

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
    //     These are provided for seamless integration with the APIs that use ByteBuffers
    //     This include the SSLEngine and SocketChanels and many other NIO APIs
    //     For performance reasons ByteBuffers should not be used unless the API has no other integration points.
    //     Using the Pipes directly is more perfomrmant and the use of DataOutput and DataInput should also be considered.

    private IntBuffer wrappedSlabRing;
    private ByteBuffer wrappedBlobReadingRingA;
    private ByteBuffer wrappedBlobReadingRingB;
    
    private ByteBuffer wrappedBlobWritingRingA;
    private ByteBuffer wrappedBlobWritingRingB;
    
    private ByteBuffer[] wrappedWritingBuffers;
    private ByteBuffer[] wrappedReadingBuffers;
    
    
    private ByteBuffer wrappedBlobConstBuffer;
    
    //      These are the recommended objects to be used for reading and writing streams into the blob
    private DataOutputBlobWriter<T> blobWriter;
    private DataInputBlobReader<T> blobReader;
    
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
	int batchReleaseCountDownInit = 0;
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

    private PipeRegulator regulatorConsumer; //disabled by default
    private PipeRegulator regulatorProducer; //disabled by default

    //for monitoring only
    public int lastMsgIdx; //last msgId read

    //helper method for those stages that are not watching for the poison pill.
	private long knownPositionOfEOF = Long.MAX_VALUE;
	
    
    private long markedHeadSlab;
    private int markedHeadBlob;
    
    private long markedTailSlab;
    private int markedTailBlob;

    private int activeBlobHead = -1;
	
	//////////////////
    /////////////////
	private static ThreadBasedCallerLookup callerLookup;
	
	//for debugging
	public static void setThreadCallerLookup(ThreadBasedCallerLookup callerLookup) {
		Pipe.callerLookup = callerLookup;
	}
	
	static boolean singleThreadPerPipeWrite(int pipeId) {
		if (null != callerLookup) {			
			int callerId = callerLookup.getCallerId();
			if (callerId>=0) {
				int expected = callerLookup.getProducerId(pipeId);
				if (expected>=0) {
					assert(callerId == expected) : "Check your graph construction and stage constructors.\n Pipe "+pipeId+" must only have 1 stage therefore 1 thread writing to it.";
				}
			}
		}
		return true;
	}

	static boolean singleThreadPerPipeRead(int pipeId) {
		if (null != callerLookup) {			
			int callerId = callerLookup.getCallerId();
			if (callerId>=0) {
				int expected = callerLookup.getConsumerId(pipeId);
				if (expected>=0) {
					assert(callerId == expected) : "Check your graph construction and stage constructors.\n Pipe "+pipeId+" must only have 1 stage therefore 1 thread reading from it.";
				}
			}
		}
		return true;
	}
	
	/////////////////
	/////////////////
	
	public static long totalWrittenFragments(Pipe<?> p) {
		return p.totalWrittenFragments;
	}
    
	public static void sumWrittenFragments(Pipe<?> p, long sum) {
		p.totalWrittenFragments+=sum;
	}
	
	////////////////////
	////////////////////
	public Pipe(PipeConfig<T> config) {
		this(config,true);
	}
    	
    public Pipe(PipeConfig<T> config, boolean usingHighLevelAPI) {
    	assert(holdConstructionLocation());
    	this.config = config;
    	this.usingHighLevelAPI = usingHighLevelAPI;
        byte primaryBits = config.slabBits;
        byte byteBits = config.blobBits;
        byte[] byteConstants = config.byteConst;
        this.schema = config.schema;

        debugFlags = config.debugFlags;
                

        //Assign the immutable universal id value for this specific instance
        //these values are required to keep track of all ring buffers when graphs are built
        this.id = pipeCounter.getAndIncrement();

        this.bitsOfSlabRing = primaryBits;
        this.bitsOfBlogRing = byteBits;
        assert(primaryBits<=30) : "Must be 1G or smaller, requested "+byteBits+" bits";
        assert(byteBits<=30) : "Must be 1G or smaller, requested "+byteBits+" bits";
        

        assert (primaryBits >= 0); //zero is a special case for a mock ring

//cas: naming.  This should be consistent with the maxByteSize, i.e., maxFixedSize or whatever.
        //single buffer size for every nested set of groups, must be set to support the largest need.
        this.sizeOfSlabRing = 1 << primaryBits;
        this.slabMask = Math.max(1, sizeOfSlabRing - 1);  //mask can no be any smaller than 1
        //single text and byte buffers because this is where the variable-length data will go.

        this.sizeOfBlobRing =  1 << byteBits;
        this.blobMask = Math.max(1, sizeOfBlobRing - 1); //mask can no be any smaller than 1

        FieldReferenceOffsetManager from = MessageSchema.from(config.schema); 

        this.blobConstBuffer = byteConstants;


        if (null==from || 0 == from.maxVarFieldPerUnit || 0==primaryBits) { //zero bits is for the dummy mock case
        	maxVarLen = 0; //no fragments had any variable-length fields so we never allow any
        } else {
            //given outer ring buffer this is the maximum number of var fields that can exist at the same time.
            int maxVarCount = FieldReferenceOffsetManager.maxVarLenFieldsPerPrimaryRingSize(from, sizeOfSlabRing);
            //to allow more almost 2x more flexibility in variable-length bytes we track pairs of writes and ensure the
            //two together are below the threshold rather than each alone
            maxVarLen = blobMask/maxVarCount;
        }
    }
 
    
    private Exception createdStack;
    
    private boolean holdConstructionLocation() {
		createdStack = new Exception("new Pipe created");
    	return true;
	}
    
    public void creationStack() {
    	if (null!=createdStack) {
    		createdStack.printStackTrace();
    	}
    }
    

	private AtomicBoolean isInBlobFieldWrite = new AtomicBoolean(false);

    //this is used along with tail position to capture the byte count
	private long totalBlobBytesRead=0;
    
    public static <S extends MessageSchema<S>> boolean isInBlobFieldWrite(Pipe<S> pipe) {
        return pipe.isInBlobFieldWrite.get();
    }
    
    public long totalBlobBytesRead() {
    	return totalBlobBytesRead;
    }
    
    public void openBlobFieldWrite() {  
    	//System.out.println("open stream on "+id);
        if (!isInBlobFieldWrite.compareAndSet(false, true)) {
        	if (null!=blobOpenStack) {
        		blobOpenStack.printStackTrace();
        	}        	
            throw new UnsupportedOperationException("only one open write against the blob at a time.");
        } 
        assert(recordOpenStack());
    }

    private Exception blobOpenStack;
    private boolean recordOpenStack() {
    	blobOpenStack = new Exception("Blob first opened here but it is attempted to be opened again later.");
		return true;
	}

	public void closeBlobFieldWrite() {
		blobOpenStack = null;
    	//System.out.println("close stream on "+id);
        if (!isInBlobFieldWrite.compareAndSet(true, false)) {
            throw new UnsupportedOperationException("can not close blob if not open.");
        }
    }
    
    //NOTE: can we compute the speed limit based on destination CPU Usage?
    //TODO: add checking mode where it can communicate back that regulation is too big or too small?
    
    public static <S extends MessageSchema<S>> boolean isRateLimitedConsumer(Pipe<S> pipe) {
        return null!=pipe.regulatorConsumer;
    }
    
    public static <S extends MessageSchema<S>> boolean isRateLimitedProducer(Pipe<S> pipe) {
        return null!=pipe.regulatorProducer;
    }
    
    /**
     * Returns nano-second count of how much time should pass before consuming more data 
     * from this pipe.  This is based on the rate configuration.  
     */
    public static <S extends MessageSchema<S>> long computeRateLimitConsumerDelay(Pipe<S> pipe) {        
        return PipeRegulator.computeRateLimitDelay(pipe, Pipe.getWorkingTailPosition(pipe), pipe.regulatorConsumer);
    }

    /**
     * Returns nano-second count of how much time should pass before producing more data 
     * into this pipe.  This is based on the rate configuration.  
     */
    public static <S extends MessageSchema<S>> long computeRateLimitProducerDelay(Pipe<S> pipe) {        
        return PipeRegulator.computeRateLimitDelay(pipe, Pipe.workingHeadPosition(pipe), pipe.regulatorProducer);
    }
    
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSchema(Pipe<S> pipe, T schema) {
        return pipe.schema == schema;
    }
    
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSchema(Pipe<S> pipe, Class<T> schema) {
        return schema.isInstance(pipe.schema);
    }
    
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSameSchema(Pipe<S> pipeA, Pipe<T> pipeB) {
        return pipeA.schema == pipeB.schema;
    }
    
    public static <S extends MessageSchema<S>> boolean isForDynamicSchema(Pipe<S> pipe) {
        return pipe.schema instanceof MessageSchemaDynamic;
    }
    
    public static <S extends MessageSchema<S>> long estBytesAllocated(Pipe<S> pipe) {
    	return ((long)pipe.blobRing.length) + (pipe.slabRing.length*4L) + 1024L;//1K for overhead
    }
    
    public static <S extends MessageSchema<S>> String schemaName(Pipe<S> pipe) {
        return null==pipe.schema? "NoSchemaFor "+Pipe.from(pipe).name  :pipe.schema.getClass().getSimpleName();
    }
    
	public static <S extends MessageSchema<S>> void replayUnReleased(Pipe<S> ringBuffer) {

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
 * Checks to see if the provided pipe is replaying.
 * @param ringBuffer the ringBuffer to check.
 * @return <code>true</code> if the ringBuffer is replaying, <code>false</code> if it is not.
 */
	public static <S extends MessageSchema<S>> boolean isReplaying(Pipe<S> ringBuffer) {
		return Pipe.getWorkingTailPosition(ringBuffer)<ringBuffer.holdingSlabWorkingTail;
	}

    /**
     * Cancels replay of specified ringBuffer
     * @param ringBuffer ringBuffer to cancel replay
     */
	public static <S extends MessageSchema<S>> void cancelReplay(Pipe<S> ringBuffer) {
		ringBuffer.slabRingTail.workingTailPos.value = ringBuffer.holdingSlabWorkingTail;
		ringBuffer.blobRingTail.byteWorkingTailPos.value = ringBuffer.holdingBlobWorkingTail;

		ringBuffer.blobReadBase = ringBuffer.holdingBlobReadBase;

		ringBuffer.ringWalker.nextWorkingTail = ringBuffer.ringWalker.holdingNextWorkingTail;
		//NOTE while replay is in effect the head can be moved by the other (writing) thread.
	}

	private static final int INDEX_BASE_OFFSET = 4; //Room for 1 int (struct info)
	
	///////////
	//support for adding indexes onto the end of the var len blob field
	///////////
	public static <S extends MessageSchema<S>> int blobIndexBasePosition(Pipe<S> rb) {
		if (rb.maxVarLen<INDEX_BASE_OFFSET) {
			throw new UnsupportedOperationException("no var length for index");
		}		
		return rb.maxVarLen-INDEX_BASE_OFFSET;
	}
	
	
	////
	////
	public static <S extends MessageSchema<S>> void batchAllReleases(Pipe<S> rb) {
	   rb.batchReleaseCountDownInit = Integer.MAX_VALUE;
	   rb.batchReleaseCountDown = Integer.MAX_VALUE;
	}


    public static <S extends MessageSchema<S>> void setReleaseBatchSize(Pipe<S> pipe, int size) {

    	validateBatchSize(pipe, size);

    	pipe.batchReleaseCountDownInit = size;
    	pipe.batchReleaseCountDown = size;
    }

    public static <S extends MessageSchema<S>> void setPublishBatchSize(Pipe<S> pipe, int size) {

    	validateBatchSize(pipe, size);

    	pipe.batchPublishCountDownInit = size;
    	pipe.batchPublishCountDown = size;
    }
    
    public static <S extends MessageSchema<S>> int getPublishBatchSize(Pipe<S> pipe) {
        return pipe.batchPublishCountDownInit;
    }
    
    public static <S extends MessageSchema<S>> int getReleaseBatchSize(Pipe<S> pipe) {
        return pipe.batchReleaseCountDownInit;
    }

    public static <S extends MessageSchema<S>> void setMaxPublishBatchSize(Pipe<S> rb) {

    	int size = computeMaxBatchSize(rb, 3);

    	rb.batchPublishCountDownInit = size;
    	rb.batchPublishCountDown = size;

    }

    public static <S extends MessageSchema<S>> void setMaxReleaseBatchSize(Pipe<S> rb) {

    	int size = computeMaxBatchSize(rb, 3);
    	rb.batchReleaseCountDownInit = size;
    	rb.batchReleaseCountDown = size;

    }


//cas: naming -- a couple of things, neither new.  Obviously the name of the buffer, bytes.  Also the use of base in
// the variable buffer, but not in the fixed.  Otoh, by now, maybe the interested reader would already understand.
    public static <S extends MessageSchema<S>> int bytesWriteBase(Pipe<S> rb) {
    	return rb.blobWriteBase;
    }

    public static <S extends MessageSchema<S>> void markBytesWriteBase(Pipe<S> rb) {
    	rb.blobWriteBase = rb.blobRingHead.byteWorkingHeadPos.value;
    }

    public static <S extends MessageSchema<S>> int bytesReadBase(Pipe<S> pipe) {
          
        assert(validateInsideData(pipe, pipe.blobReadBase));
        
    	return pipe.blobReadBase;
    }        
    
    private static <S extends MessageSchema<S>> boolean validateInsideData(Pipe<S> pipe, int value) {
		

	    int mHead = Pipe.blobMask(pipe) & Pipe.getBlobHeadPosition(pipe);
	    int mTail = Pipe.blobMask(pipe) & Pipe.getBlobTailPosition(pipe);
	    int mValue = Pipe.blobMask(pipe) & value;
	    if (mTail<=mHead) {
	    	assert(mTail<=mValue && mValue<=mHead) : "tail "+mTail+" readBase "+mValue+" head "+mHead;
	        return mTail<=mValue && mValue<=mHead;
	    } else {
	    	assert(mValue<=mHead || mValue>=mTail) : "tail "+mTail+" readBase "+mValue+" head "+mHead;
	    	return mValue<=mHead || mValue>=mTail;
	    }
	}

	public static <S extends MessageSchema<S>> void markBytesReadBase(Pipe<S> pipe, int bytesConsumed) {
        assert(bytesConsumed>=0) : "Bytes consumed must be positive";
        //base has future pos added to it so this value must be masked and kept as small as possible

    //TODO: bytes consumed is wrong when we are using an index...    
        
        pipe.totalBlobBytesRead = pipe.totalBlobBytesRead+bytesConsumed;        
        pipe.blobReadBase = pipe.blobMask /*Pipe.BYTES_WRAP_MASK*/ & (pipe.blobReadBase+bytesConsumed);
        assert(validateInsideData(pipe, pipe.blobReadBase)) : "consumed "+bytesConsumed+" bytes using mask "+pipe.blobMask+" new base is "+pipe.blobReadBase;
    }
    
    public static <S extends MessageSchema<S>> void markBytesReadBase(Pipe<S> pipe) {
    	final int newBasePosition = pipe.blobMask & PaddedInt.get(pipe.blobRingTail.byteWorkingTailPos);
    
    	//do comparison to find the new consumed value?
    	int bytesConsumed = newBasePosition - pipe.blobReadBase;
    	if (bytesConsumed<0) {
    		bytesConsumed += pipe.sizeOfBlobRing;
    	}
    	pipe.totalBlobBytesRead = pipe.totalBlobBytesRead+bytesConsumed;
    	    	
    	//base has future pos added to it so this value must be masked and kept as small as possible
		pipe.blobReadBase = newBasePosition;
    }
    
    //;

    /**
     * Helpful user readable summary of the ring buffer.
     * Shows where the head and tail positions are along with how full the ring is at the time of call.
     */
    public String toString() {

        int contentRem = Pipe.contentRemaining(this);
        assert(contentRem <= sizeOfSlabRing) : "ERROR: can not have more content than the size of the pipe. content "+contentRem+" vs "+sizeOfSlabRing;
        
    	StringBuilder result = new StringBuilder();
    	result.append("RingId<").append(schemaName(this));
    	Appendables.appendValue(result.append(">:"), id);
    	
    	Appendables.appendValue(result.append(" slabTailPos "),slabRingTail.tailPos.get());
    	Appendables.appendValue(result.append(" slabWrkTailPos "),slabRingTail.workingTailPos.value);
    	Appendables.appendValue(result.append(" slabHeadPos "),slabRingHead.headPos.get());
    	Appendables.appendValue(result.append(" slabWrkHeadPos "),slabRingHead.workingHeadPos.value);
    	Appendables.appendValue(result.append("  ").append(contentRem).append("/"),sizeOfSlabRing);
    	Appendables.appendValue(result.append("  blobTailPos "),PaddedInt.get(blobRingTail.bytesTailPos));
    	Appendables.appendValue(result.append(" blobWrkTailPos "),blobRingTail.byteWorkingTailPos.value);
    	Appendables.appendValue(result.append(" blobHeadPos "),PaddedInt.get(blobRingHead.bytesHeadPos));
    	Appendables.appendValue(result.append(" blobWrkHeadPos "),blobRingHead.byteWorkingHeadPos.value);
    	
    	if (isEndOfPipe(this, slabRingTail.tailPos.get())) {
    		Appendables.appendValue(result.append(" Ended at "),this.knownPositionOfEOF);
    	}

    	return result.toString();
    }


    /**
     * Return the configuration used for this ring buffer, Helpful when we need to make clones of the ring which will hold same message types.
     */
    public PipeConfig<T> config() {
    	return config;
    }

    public static <S extends MessageSchema> int totalPipes() {
        return pipeCounter.get();
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
    
    public static <S extends MessageSchema<S>> void setConsumerRegulation(Pipe<S> pipe, int msgPerMs, int msgSize) {
        assert(null==pipe.regulatorConsumer) : "regulator must only be set once";
        assert(!isInit(pipe)) : "regular may only be set before scheduler has initialized the pipe";
        pipe.regulatorConsumer = new PipeRegulator(msgPerMs, msgSize);
    }
  
    public static <S extends MessageSchema<S>> void setProducerRegulation(Pipe<S> pipe, int msgPerMs, int msgSize) {
        assert(null==pipe.regulatorProducer) : "regulator must only be set once";
        assert(!isInit(pipe)) : "regular may only be set before scheduler has initialized the pipe";
        pipe.regulatorProducer = new PipeRegulator(msgPerMs, msgSize);
    } 
    
	private void buildBuffers() {

	    this.pendingReleases = 
	    		((null == schema.from) ?	null : //pending releases only supported with real FROM schemas
	    		new PendingReleaseData(
	    				sizeOfSlabRing / FieldReferenceOffsetManager.minFragmentSize(MessageSchema.from(schema))	
	    		));
	    
	    
	    //NOTE: this is only needed for high level API, if only low level is in use it would be nice to not create this 
	    if (usingHighLevelAPI && null!=schema.from) {
	    	this.ringWalker = new StackStateWalker(MessageSchema.from(schema), sizeOfSlabRing);
	    }
	    
        assert(slabRingHead.workingHeadPos.value == slabRingHead.headPos.get());
        assert(slabRingTail.workingTailPos.value == slabRingTail.tailPos.get());
        assert(slabRingHead.workingHeadPos.value == slabRingTail.workingTailPos.value);
        assert(slabRingTail.tailPos.get()==slabRingHead.headPos.get());

        long toPos = slabRingHead.workingHeadPos.value;//can use this now that we have confirmed they all match.

        this.llRead = new LowLevelAPIReadPositionCache();
        this.llWrite = new LowLevelAPIWritePositionCache();

        //This init must be the same as what is done in reset()
        //This target is a counter that marks if there is room to write more data into the ring without overwriting other data.
        llWrite.llwHeadPosCache = toPos;
        llRead.llrTailPosCache = toPos;
        llRead.llwConfirmedPosition = toPos - sizeOfSlabRing;// TODO: hack test,  mask;//must be mask to ensure zero case works.
        llWrite.llrConfirmedPosition = toPos;

        try {
	        this.blobRing = new byte[sizeOfBlobRing];
	        this.slabRing = new int[sizeOfSlabRing];
	        this.blobRingLookup = new byte[][] {blobRing,blobConstBuffer};
        } catch (OutOfMemoryError oome) {
        	
        	log.warn("attempted to allocate Slab:{} Blob:{} in {}", sizeOfSlabRing, sizeOfBlobRing, this, oome);
        	shutdown(this);
        	System.exit(-1);
        }
        //This assignment is critical to knowing that init was called
        this.wrappedSlabRing = IntBuffer.wrap(this.slabRing);        

        //only create if there is a possibility that they may be used.
        if (sizeOfBlobRing>0) {
	        this.wrappedBlobReadingRingA = ByteBuffer.wrap(this.blobRing);
	        this.wrappedBlobReadingRingB = ByteBuffer.wrap(this.blobRing);
	        this.wrappedBlobWritingRingA = ByteBuffer.wrap(this.blobRing);
	        this.wrappedBlobWritingRingB = ByteBuffer.wrap(this.blobRing);	        
	        this.wrappedBlobConstBuffer = null==this.blobConstBuffer?null:ByteBuffer.wrap(this.blobConstBuffer);
	        
	        this.wrappedReadingBuffers = new ByteBuffer[]{wrappedBlobReadingRingA,wrappedBlobReadingRingB}; 
	        this.wrappedWritingBuffers = new ByteBuffer[]{wrappedBlobWritingRingA,wrappedBlobWritingRingB};
	        
	        
	        assert(0==wrappedBlobReadingRingA.position() && wrappedBlobReadingRingA.capacity()==wrappedBlobReadingRingA.limit()) : "The ByteBuffer is not clear.";
	
	        //blobReader and writer must be last since they will be checking isInit in construction.
	        this.blobReader = createNewBlobReader();
	        this.blobWriter = createNewBlobWriter();
        }
	}
	
	//Can be overridden to support specific classes which extend DataInputBlobReader
	protected DataInputBlobReader<T> createNewBlobReader() {
		 return new DataInputBlobReader<T>(this);
	}
	
	//Can be overridden to support specific classes which extend DataOutputBlobWriter
	protected DataOutputBlobWriter<T> createNewBlobWriter() {
		return new DataOutputBlobWriter<T>(this);
	}
	

	public static <S extends MessageSchema<S>> boolean isInit(Pipe<S> ring) {
	    //Due to the fact that no locks are used it becomes necessary to check
	    //every single field to ensure the full initialization of the object
	    //this is done as part of graph set up and as such is called rarely.
		return null!=ring.blobRing &&
			   null!=ring.slabRing &&
			   null!=ring.blobRingLookup &&
			   null!=ring.wrappedSlabRing &&
			   null!=ring.llRead &&
			   null!=ring.llWrite &&
			   (
			    ring.sizeOfBlobRing == 0 || //no init of these if the blob is not used
			    (null!=ring.wrappedBlobReadingRingA &&
		         null!=ring.wrappedBlobReadingRingB &&
			     null!=ring.wrappedBlobWritingRingA &&
			     null!=ring.wrappedBlobWritingRingB
			     )
			   );
		      //blobReader and blobWriter and not checked since they call isInit on construction
	}

	public static <S extends MessageSchema<S>> boolean validateVarLength(Pipe<S> pipe, int length) {
		assert(length>=-1) : "invalid length value "+length;
		int newAvg = (length+pipe.varLenMovingAverage)>>1;
        if (newAvg>pipe.maxVarLen)	{
            //compute some helpful information to add to the exception
        	int bytesPerInt = (int)Math.ceil(length*Pipe.from(pipe).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);
        	Pipe.shutdown(pipe);
        	throw new UnsupportedOperationException("Can not write byte array of length "+length+
        	                                        ". The dif between slab and byte blob should be at least "+bitsDif+
        	                                        ". "+pipe.bitsOfSlabRing+","+pipe.bitsOfBlogRing+
        	                                        ". The limit is "+pipe.maxVarLen+" for pipe "+pipe);
        }
        pipe.varLenMovingAverage = newAvg;
        return true;
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
            llRead.llwConfirmedPosition = structuredPos -  sizeOfSlabRing;//mask;sss  TODO: hack test.
            llWrite.llrConfirmedPosition = structuredPos;
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

    public static void releaseReadsBatched(Pipe<MessageSchemaDynamic> p) {
		Pipe.batchedReleasePublish(p, Pipe.getWorkingBlobRingTailPosition(p),
	    		                      Pipe.getWorkingTailPosition(p));
	}

    /**
     * Returns the slab size of this copied fragment.
     * This method assumes there is both a fragment to be copied and
     * that there will be room at the destination.
     */
	public static <S extends MessageSchema<S>> int copyFragment(
    		Pipe<S> source, 
    		Pipe<S> target) {
    	
    	return copyFragment(source, 
    			Pipe.tailPosition(source),
    			Pipe.getBlobTailPosition(source),
    			target);

    }
    		
    
	static <S extends MessageSchema<S>> int copyFragment(
			Pipe<S> sourcePipe, long sourceSlabPos, int sourceBlobPos,
			Pipe<S> localTarget) {
		
		int mask = Pipe.slabMask(sourcePipe);
		int[] slab = Pipe.slab(sourcePipe);
		
		int msgIdx = slab[mask&(int)sourceSlabPos];
		
		//look up the data size to copy...
		int slabMsgSize = Pipe.from(sourcePipe).fragDataSize[msgIdx];
		int blobMsgSize = slab[mask&((int)(sourceSlabPos+slabMsgSize-1))]; //min one for byte count
			
		Pipe.copyFragment(localTarget,
				slabMsgSize, blobMsgSize, 
				Pipe.blob(sourcePipe), Pipe.slab(sourcePipe), 
				Pipe.blobMask(sourcePipe), Pipe.slabMask(sourcePipe), 
				sourceBlobPos, (int)sourceSlabPos);
		
		//move source pointers forward
		Pipe.addAndGetWorkingTail(sourcePipe, slabMsgSize-1);
		Pipe.addAndGetBytesWorkingTailPosition(sourcePipe, blobMsgSize);
		Pipe.confirmLowLevelRead(sourcePipe, slabMsgSize);
		Pipe.releaseReadLock(sourcePipe);		
		
		
		return slabMsgSize;
		
	}

	public static Pipe[] buildPipes(PipeConfig[] configs) {
		int i = configs.length;
		Pipe[] result = new Pipe[i];
		while (--i>=0) {
			result[i] = new Pipe(configs[i]);
		}		
		return result;
	}

	public static <S extends MessageSchema<S>> Pipe<S>[] buildPipes(int count, PipeConfig<S> comonConfig) {		
		Pipe[] result = new Pipe[count];
		int i = count;
		while (--i>=0) {
			result[i] = new Pipe<S>(comonConfig);
		}
		return (Pipe<S>[])result;
	}

    /**
     * Checks blob to see if there is data to read
     * @param pipe pipe to check
     * @param blobPos position of blob
     * @param length length of blob
     */
	public static <S extends MessageSchema<S>> boolean validatePipeBlobHasDataToRead(Pipe<S> pipe, int blobPos, int length) {

		assert(length>=0) : "bad length:"+length;
		if (length==0) {			
			return true;//nothing to check in this case.
		}
		
		assert(length<=pipe.sizeOfBlobRing) : "length is larger than backing array "+length+" vs "+pipe.sizeOfBlobRing;
	
		//we know that we are looking for a non zero length
		assert(Pipe.getBlobHeadPosition(pipe)!=Pipe.getBlobTailPosition(pipe)) : "Needs "+length+" but pipe is empty and can not have any data: "+pipe;
		
		
	    int mHead = Pipe.blobMask(pipe) & Pipe.getBlobHeadPosition(pipe);
	    int mTail = Pipe.blobMask(pipe) & Pipe.getBlobTailPosition(pipe);
	    //ensure that starting at testPos up to testLen is all contained between tail and head
	        
	    int mStart = Pipe.blobMask(pipe) & blobPos;
	    int stop = mStart+length;
	    int mStop = Pipe.blobMask(pipe) & stop;
	         
	    //we have 4 cases where position and length can be inside (inbetween tail and head)
	    //these cases are all drawn below  pllll is the content starting at position p and
	    //running with p for the full length
	    
	    //Head - where new data can be written (can only write up to tail)
	    //Tail - where data is consumed (can only consume up to head)
	    
	    
	    if ((mStop<mStart) && (stop >= pipe.sizeOfBlobRing)) {
					////////////////////////////////////////////////////////////
					//pppppppppppp      H                      T     ppppppppp//
					////////////////////////////////////////////////////////////
	        assert(mStop<=mHead)  : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	        assert(mTail<=mStart) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	        assert(mHead<=mStart) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	        assert(mHead<mTail)   : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	        assert(mStop<=mHead)  : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	       
	        return (mStop<=mHead)
	        		&&(mTail<=mStart)
	        		&&(mHead<=mStart)
	        		&&(mHead<mTail)
	        		&&(mStop<=mHead);
	        
	    } else {
	      if (mHead>mTail) {
					////////////////////////////////////////////////////////////
					//        T     pppppppppppppppppppppp            H       //
					////////////////////////////////////////////////////////////
	    	 assert(mTail<mHead)  : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	    	 assert(mTail<=mStart): "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	    	 assert(mTail<=mStop) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	    	 assert(mStop<=mHead) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	    	 assert(stop<=(pipe.sizeOfBlobRing*2L)) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	    	  
	    	 return (mTail<mHead)
	    			 &&(mTail<=mStart)
	    			 &&(mTail<=mStop)
	    			 &&(mStop<=mHead)
	    			 &&(stop<=(pipe.sizeOfBlobRing*2L));
	    	 
	      } else {
				if (mStart>=mTail) {
		    	  
					////////////////////////////////////////////////////////////
					//                  H                      T     pppppppp //
					////////////////////////////////////////////////////////////
				    assert(mHead<mTail) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
				    assert(mTail<=mStart): "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;			
					assert(stop<=(pipe.sizeOfBlobRing*2L)) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
					
					return (mHead<mTail)
							&&(mTail<=mStart)
							&&(stop<=(pipe.sizeOfBlobRing*2L));
					
				} else {
				    	  
					////////////////////////////////////////////////////////////
					//pppppppppppp      H                      T              //
					////////////////////////////////////////////////////////////
				    assert(mStart<mHead) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
				    assert(mStop<=mHead) : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
				    assert(mHead<mTail)  : "tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
		   			assert(stop<=(pipe.sizeOfBlobRing*2L)) : "absStop "+stop+" tail "+mTail+" start "+mStart+" stop "+mStop+" head "+mHead+" mask "+pipe.blobMask+" pipe "+pipe;
	
					return (mStart<mHead)
							&&(mStop<=mHead)
							&&(mHead<mTail)
							&&(stop<=(pipe.sizeOfBlobRing*2L));
					
				}
	      }
	    }
	}

    /**
     * Writes in an int as an ASCII character to specified pipe
     * @param output pipe to write to
     * @param value int to write as ASCII
     * @param <S> MessageSchema to extend
     * @return addLongAsUTF8(output, value)
     */
	public static <S extends MessageSchema<S>> int addIntAsASCII(Pipe<S> output, int value) {
		validateVarLength(output, 12);
		return addLongAsUTF8(output, value);
	}

	
    public static <S extends MessageSchema<S>> int addRationalAsASCII(Pipe<S> digitBuffer, long numerator, long denominator) {
  	      
    	  validateVarLength(digitBuffer, 21);
	      DataOutputBlobWriter<S> outputStream = Pipe.outputStream(digitBuffer);
	      outputStream.openField();
	      
	      Appendables.appendValue(outputStream, numerator);
	      outputStream.writeChar('/');
	      Appendables.appendValue(outputStream, denominator);
	      	      
	      return outputStream.closeLowLevelField();	
	
    }

    /**
     * Writes in a long as an ASCII character to specified pipe
     * @param output pipe to write to
     * @param value long to write as ASCII
     * @param <S> MessageSchema to extend
     * @return addLongAsUTF8(output, value)
     */
	public static <S extends MessageSchema<S>> int addLongAsASCII(Pipe<S> output, long value) {
		return addLongAsUTF8(output, value);
	}

    /**
     * Writes long as UTF8 with specified length to named Pipe
     * @param digitBuffer Pipe reference
     * @param length length of long to add
     * @param <S> MessageSchema to extend
     * @return outputStream of Pipe
     */
    public static <S extends MessageSchema<S>> int addLongAsUTF8(Pipe<S> digitBuffer, long length) {
    	  validateVarLength(digitBuffer, 21);
	      DataOutputBlobWriter<S> outputStream = Pipe.outputStream(digitBuffer);
	      outputStream.openField();
	      Appendables.appendValue(outputStream, length);
	      return outputStream.closeLowLevelField();	
	}

    /**
     * Writes long as UTF8 with specified length to Pipe
     * @param digitBuffer Pipe reference
     * @param length length of int to add
     * @return outputStream of Pipe
     */
    public static <S extends MessageSchema<S>> int addLongAsUTF8(Pipe<S> digitBuffer, int length) {
  	      validateVarLength(digitBuffer, 21);
	      DataOutputBlobWriter<S> outputStream = Pipe.outputStream(digitBuffer);
	      outputStream.openField();
	      Appendables.appendValue(outputStream, length);
	      return outputStream.closeLowLevelField();	
	}
    
	public static <S extends MessageSchema<S>> Pipe<S>[][] splitPipes(int pipeCount, Pipe<S>[] socketResponse) {
		
		Pipe<S>[][] result = new Pipe[pipeCount][];
			
		int fullLen = socketResponse.length;
		int last = 0;
		for(int p = 1;p<pipeCount;p++) {			
			int nextLimit = (p*fullLen)/pipeCount;			
			int plen = nextLimit-last;			
		    Pipe<S>[] newPipe = new Pipe[plen];
		    System.arraycopy(socketResponse, last, newPipe, 0, plen);
		    result[p-1]=newPipe;
			last = nextLimit;
		}
		int plen = fullLen-last;
	    Pipe<S>[] newPipe = new Pipe[plen];
	    System.arraycopy(socketResponse, last, newPipe, 0, plen);
	    result[pipeCount-1]=newPipe;
				
		return result;
				
	}
    
    //TODO: URGENT we need a unit test to ensure split pipes and split groups give the same splitting results.
    
    /**
     * matching the above splitPipes logic this method produces an inverse lookup array to determine group given a single index.
     * 
     * @param groups
     * @param fullLen
     * @return array to look up which group a value is in
     */
    public static int[] splitGroups(final int groups, final int fullLen) {
		
    	int c = 0;
		int[] result = new int[fullLen];
			
		int last = 0;
		
		for(int p = 1;p<groups;p++) {			
			int nextLimit = (p*fullLen)/groups;			
			int plen = nextLimit-last;	
			
			while (--plen>=0) {
				result[c++] = p-1;
			}
		    
			last = nextLimit;
		}
		int plen = fullLen-last;
	    
		while (--plen>=0) {
			result[c++] = groups-1;
		}
		
		return result;
				
	}
    

	public static <S extends MessageSchema<S>> void writeFieldToOutputStream(Pipe<S> pipe, OutputStream out) throws IOException {
        int meta = Pipe.takeRingByteMetaData(pipe);
        int length    = Pipe.takeRingByteLen(pipe);    
        if (length>0) {                
            int off = bytePosition(meta,pipe,length) & Pipe.blobMask(pipe);
            copyFieldToOutputStream(out, length, Pipe.byteBackingArray(meta, pipe), off, pipe.sizeOfBlobRing-off);
        }
    }

    private static void copyFieldToOutputStream(OutputStream out, int length, byte[] backing, int off, int lenFromOffsetToEnd)
            throws IOException {
        if (lenFromOffsetToEnd>=length) {
            //simple add bytes
            out.write(backing, off, length); 
        } else {                        
            //rolled over the end of the buffer
            out.write(backing, off, lenFromOffsetToEnd);
            out.write(backing, 0, length-lenFromOffsetToEnd);
        }
    }
    
    public static boolean readFieldFromInputStream(Pipe pipe, InputStream inputStream, final int byteCount) throws IOException {
        return buildFieldFromInputStream(pipe, inputStream, byteCount, Pipe.getWorkingBlobHeadPosition(pipe), Pipe.blobMask(pipe), Pipe.blob(pipe), pipe.sizeOfBlobRing);
    }

    private static boolean buildFieldFromInputStream(Pipe pipe, InputStream inputStream, final int byteCount, int startPosition, int byteMask, byte[] buffer, int sizeOfBlobRing) throws IOException {
        boolean result = copyFromInputStreamLoop(inputStream, byteCount, startPosition, byteMask, buffer, sizeOfBlobRing, 0);        
        Pipe.addBytePosAndLen(pipe, startPosition, byteCount);
        Pipe.addAndGetBytesWorkingHeadPosition(pipe, byteCount);
        assert(Pipe.validateVarLength(pipe, byteCount));
        return result;
    }

    private static boolean copyFromInputStreamLoop(InputStream inputStream, int remaining, int position, int byteMask, byte[] buffer, int sizeOfBlobRing, int size) throws IOException {
        while ( (remaining>0) && (size=safeRead(inputStream, position&byteMask, buffer, sizeOfBlobRing, remaining))>=0 ) { 
            if (size>0) {
                remaining -= size;                    
                position += size;
            } else {
                if (size<0) {
                	return false;
                }
            	Thread.yield();
                
            }
        }
        return true;
    }
    
    static int safeRead(InputStream inputStream, int position, byte[] buffer, int sizeOfBlobRing, int remaining) throws IOException {
        return inputStream.read(buffer, position, safeLength(sizeOfBlobRing, position, remaining)  );
    }
    
    static int safeRead(DataInput dataInput, int position, byte[] buffer, int sizeOfBlobRing, int remaining) throws IOException {
        int safeLength = safeLength(sizeOfBlobRing, position, remaining);
		dataInput.readFully(buffer, position, safeLength);
		return safeLength;
    }
    
    static int safeLength(int sizeOfBlobRing, int position, int remaining) {
        return ((position+remaining)<=sizeOfBlobRing) ? remaining : sizeOfBlobRing-position;
    }
    
    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobForWritingA(int originalBlobPosition, Pipe<S> output) {
        ByteBuffer target = output.wrappedBlobWritingRingA; //Get the blob array as a wrapped byte buffer     
        int writeToPos = originalBlobPosition & Pipe.blobMask(output); //Get the offset in the blob where we should write
        target.limit(target.capacity());
        target.position(writeToPos);   
        target.limit(Math.min(target.capacity(), writeToPos+output.maxVarLen)); //ensure we stop at end of wrap or max var length 
        return target;
    }

    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobForWritingB(int originalBlobPosition, Pipe<S> output) {
        ByteBuffer target = output.wrappedBlobWritingRingB; //Get the blob array as a wrapped byte buffer     
        int writeToPos = originalBlobPosition & Pipe.blobMask(output); //Get the offset in the blob where we should write
        target.position(0);   
        int endPos = writeToPos+output.maxVarLen;
    	if (endPos>output.sizeOfBlobRing) {
    		target.limit(output.blobMask & endPos);
    	} else {
    		target.limit(0);
    	}
        return target;
    }
      
    public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(Pipe<S> output) {
    	return wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(output),output);
    }
    
    public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(int originalBlobPosition, Pipe<S> output) {
    	return wrappedWritingBuffers(originalBlobPosition, output, output.maxVarLen);
    }

	public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(int originalBlobPosition,	Pipe<S> output,
			int maxLen) {
		
		assert(maxLen>=0);
		
		int writeToPos = originalBlobPosition & Pipe.blobMask(output); //Get the offset in the blob where we should write
		int endPos = writeToPos+maxLen;

    	ByteBuffer aBuf = output.wrappedBlobWritingRingA; //Get the blob array as a wrapped byte buffer     
		((Buffer)aBuf).limit(aBuf.capacity());
		((Buffer)aBuf).position(writeToPos);   
		((Buffer)aBuf).limit(Math.min(aBuf.capacity(), endPos ));
		
		ByteBuffer bBuf = output.wrappedBlobWritingRingB; //Get the blob array as a wrapped byte buffer     
		((Buffer)bBuf).position(0);   
		((Buffer)bBuf).limit(endPos>output.sizeOfBlobRing ? output.blobMask & endPos: 0);
		
		return output.wrappedWritingBuffers;
	}
 
    public static <S extends MessageSchema<S>> void moveBlobPointerAndRecordPosAndLength(int len, Pipe<S> output) {
    	moveBlobPointerAndRecordPosAndLength(Pipe.unstoreBlobWorkingHeadPosition(output), len, output);
    }
    
    public static <S extends MessageSchema<S>> void moveBlobPointerAndRecordPosAndLength(int originalBlobPosition, int len, Pipe<S> output) {
    	
    	assert(verifyHasRoomForWrite(len, output));    	
    	
    	//blob head position is moved forward
    	if (len>0) { //len can be 0 so do nothing, len can be -1 for eof also nothing to move forward
    		Pipe.addAndGetBytesWorkingHeadPosition(output, len);
    	}
        //record the new start and length to the slab for this blob
        Pipe.addBytePosAndLen(output, originalBlobPosition, len);
    }

	private static <S extends MessageSchema<S>> boolean verifyHasRoomForWrite(int len, Pipe<S> output) {
				
		int h = getWorkingBlobHeadPosition(output)&output.blobMask;
    	int t = getBlobTailPosition(output)&output.blobMask;
    	int consumed;
    	if (h>=t) {
    		consumed = len+(h-t);
			assert(consumed<=output.blobMask) : "length too large for existing data, proposed addition "+len+" head "+h+" tail "+t+" "+output+" "+Pipe.contentRemaining(output);
    	} else {
    		consumed = len+h+(output.sizeOfBlobRing-t);
			assert(consumed<=output.blobMask) : "length is too large for existing data  "+len+" + t:"+t+" h:"+h+" max "+output.blobMask;
    	}
    	return (consumed<=output.blobMask);
	}


    @Deprecated
    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingA(Pipe<S> pipe, int meta, int len) {
        return wrappedBlobReadingRingA(pipe, meta, len);
    }
    
    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobReadingRingA(Pipe<S> pipe, int meta, int len) {
        ByteBuffer buffer;
        if (meta < 0) {
        	buffer = wrappedBlobConstBuffer(pipe);
        	int position = PipeReader.POS_CONST_MASK & meta;    
        	((Buffer)buffer).position(position);
        	((Buffer)buffer).limit(position+len);        	
        } else {
        	buffer = wrappedBlobRingA(pipe);
        	int position = pipe.blobMask & restorePosition(pipe,meta);
        	((Buffer)buffer).clear();
        	((Buffer)buffer).position(position);
        	//use the end of the buffer if the length runs past it.
        	((Buffer)buffer).limit(Math.min(pipe.sizeOfBlobRing, position+len));
        }
        return buffer;
    }
    
    @Deprecated
    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingB(Pipe<S> pipe, int meta, int len) {
        return wrappedBlobReadingRingB(pipe,meta,len);
    }

    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobReadingRingB(Pipe<S> pipe, int meta, int len) {
        ByteBuffer buffer;
        if (meta < 0) {
        	//always zero because constant array never wraps
        	buffer = wrappedBlobConstBuffer(pipe);
        	((Buffer)buffer).position(0);
        	((Buffer)buffer).limit(0);
        } else {
        	buffer = wrappedBlobRingB(pipe);
        	int position = pipe.blobMask & restorePosition(pipe,meta);
        	((Buffer)buffer).clear();
            //position is zero
        	int endPos = position+len;
        	if (endPos>pipe.sizeOfBlobRing) {
        		((Buffer)buffer).limit(pipe.blobMask & endPos);
        	} else {
        		((Buffer)buffer).limit(0);
        	}
        }		
    	return buffer;
    }

    public static <S extends MessageSchema<S>> ByteBuffer[] wrappedReadingBuffers(Pipe<S> pipe, int meta, int len) {
    	if (meta >= 0) {
    		//MUST call this one which creates side effect of assuming this data is consumed
			wrappedReadingBuffersRing(pipe, len, pipe.blobMask & bytePosition(meta,pipe,len));
		} else {
			wrappedReadingBuffersConst(pipe, meta, len);
		}
		return pipe.wrappedReadingBuffers;
    }

	static <S extends MessageSchema<S>> ByteBuffer[] wrappedReadingBuffersRing(Pipe<S> pipe, int len, final int position) {
		final int endPos = position+len;
		
		ByteBuffer aBuf = wrappedBlobRingA(pipe);
		((Buffer)aBuf).clear();
		((Buffer)aBuf).position(position);
		//use the end of the buffer if the length runs past it.
		((Buffer)aBuf).limit(Math.min(pipe.sizeOfBlobRing, endPos));
		
		ByteBuffer bBuf = wrappedBlobRingB(pipe);
		((Buffer)bBuf).clear();
		((Buffer)bBuf).limit(endPos > pipe.sizeOfBlobRing ? pipe.blobMask & endPos : 0 ); 
				
		return pipe.wrappedReadingBuffers;
	}

	static <S extends MessageSchema<S>> ByteBuffer[] wrappedReadingBuffersConst(Pipe<S> pipe, int meta, int len) {
		
		ByteBuffer aBuf = wrappedBlobConstBuffer(pipe);
		int position = PipeReader.POS_CONST_MASK & meta;    
		aBuf.position(position);
		aBuf.limit(position+len);        	

		//always zero because constant array never wraps
		ByteBuffer bBuf = wrappedBlobConstBuffer(pipe);
		bBuf.position(0);
		bBuf.limit(0);
		
		return pipe.wrappedReadingBuffers;
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

    public static <S extends MessageSchema<S>> void appendFragment(Pipe<S> input, Appendable target, int cursor) {
        try {

            FieldReferenceOffsetManager from = from(input);
            int fields = from.fragScriptSize[cursor];
            assert (cursor<from.tokensLen-1);//there are no single token messages so there is no room at the last position.


            int dataSize = from.fragDataSize[cursor];
            String msgName = from.fieldNameScript[cursor];
            long msgId = from.fieldIdScript[cursor];

            target.append(" cursor:");
            Appendables.appendValue(target, cursor);
            target.append(" fields: ");
            Appendables.appendValue(target, fields);
            target.append(" ");
            target.append(msgName);
            target.append(" id: ");
            Appendables.appendValue(target, msgId).append("\n");

            if (0==fields && cursor==from.tokensLen-1) { //this is an odd case and should not happen
                //TODO: AA length is too long and we need to detect cursor out of bounds!
                System.err.println("total tokens:"+from.tokens.length);//Arrays.toString(from.fieldNameScript));
                throw new RuntimeException("unable to convert fragment to text");
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
                            int len = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                            value = Integer.toHexString(len)+"("+len+")";
                            break;
                        case TypeMask.IntegerSigned:
                        case TypeMask.IntegerUnsigned:
                        case TypeMask.IntegerSignedOptional:
                        case TypeMask.IntegerUnsignedOptional:
                            int readInt = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                            value = Integer.toHexString(readInt)+"("+readInt+")";
                            break;
                        case TypeMask.LongSigned:
                        case TypeMask.LongUnsigned:
                        case TypeMask.LongSignedOptional:
                        case TypeMask.LongUnsignedOptional:
                            long readLong = readLong(slab(input), input.slabMask, pos+tailPosition(input));
                            value = Long.toHexString(readLong)+"("+readLong+")";
                            break;
                        case TypeMask.Decimal:
                        case TypeMask.DecimalOptional:

                            int exp = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                            long mantissa = readLong(slab(input), input.slabMask, pos+tailPosition(input)+1);
                            value = exp+" "+mantissa;

                            break;
                        case TypeMask.TextASCII:
                        case TypeMask.TextASCIIOptional:
                            {
                                int meta = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                                int length = readInt(slab(input), input.slabMask, pos+tailPosition(input)+1);
                                readASCII(input, target, meta, length);
                                value = meta+" len:"+length;
                                // value = target.toString();
                            }
                            break;
                        case TypeMask.TextUTF8:
                        case TypeMask.TextUTF8Optional:

                            {
                                int meta = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                                int length = readInt(slab(input), input.slabMask, pos+tailPosition(input)+1);
                                readUTF8(input, target, meta, length);
                                value = meta+" len:"+length;
                               // value = target.toString();
                            }
                            break;
                        case TypeMask.ByteVector:
                        case TypeMask.ByteVectorOptional:
                            {
                                int meta = readInt(slab(input), input.slabMask, pos+tailPosition(input));
                                int length = readInt(slab(input), input.slabMask, pos+tailPosition(input)+1);
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

    public static <S extends MessageSchema<S>> ByteBuffer readBytes(Pipe<S> pipe, ByteBuffer target, int meta, int len) {
		if (meta >= 0) {
			return readBytesRing(pipe,len,target,restorePosition(pipe,meta));
	    } else {
	    	return readBytesConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	    }
	}
    
    public static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytes(Pipe<S> pipe, DataOutputBlobWriter<?> target, int meta, int len) {
		if (meta >= 0) {
			return readBytesRing(pipe,len,target,restorePosition(pipe,meta));
	    } else {
	    	return readBytesConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	    }
	}

    public static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytes(Pipe<S> pipe, DataOutputBlobWriter<?> target) {
    	return Pipe.readBytes(pipe, target, Pipe.takeRingByteMetaData(pipe), Pipe.takeRingByteLen(pipe));
 	}

    /**
     * Reads bytes from specified pipe at given index
     * @param pipe to read from
     * @param target array to check
     * @param targetIdx index to check
     */
    public static <S extends MessageSchema<S>> void readBytes(Pipe<S> pipe, byte[] target, int targetIdx, int targetMask, int meta, int len) {
		if (meta >= 0) {
			copyBytesFromToRing(pipe.blobRing,restorePosition(pipe,meta),pipe.blobMask,target,targetIdx,targetMask,len);
	    } else {
	    	//NOTE: constByteBuffer does not wrap so we do not need the mask
	    	copyBytesFromToRing(pipe.blobConstBuffer, PipeReader.POS_CONST_MASK & meta, 0xFFFFFFFF, target, targetIdx, targetMask, len);
	    }
	}

	private static <S extends MessageSchema<S>> ByteBuffer readBytesRing(Pipe<S> pipe, int len, ByteBuffer target, int pos) {
		int mask = pipe.blobMask;
		byte[] buffer = pipe.blobRing;

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
	
	private static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytesRing(Pipe<S> pipe, int len, DataOutputBlobWriter<?> target, int pos) {
				
		DataOutputBlobWriter.write(target, pipe.blobRing, pos, len, pipe.blobMask);
		
		//could use this code to make something similar for writing to  DataOutput or output stream?
//		int mask = pipe.blobMask;
//		byte[] buffer = pipe.blobRing;
//        int len1 = 1+mask - (pos & mask);
//
//		if (len1>=len) {
//			target.write(buffer, mask&pos, len);
//		} else {
//			target.write(buffer, mask&pos, len1);
//			target.write(buffer, 0, len-len1);
//		}

	    return target;
	}

	private static <S extends MessageSchema<S>> ByteBuffer readBytesConst(Pipe<S> pipe, int len, ByteBuffer target, int pos) {
	    	target.put(pipe.blobConstBuffer, pos, len);
	        return target;
	    }

	private static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytesConst(Pipe<S> pipe, int len, DataOutputBlobWriter<?> target, int pos) {
    	target.write(pipe.blobConstBuffer, pos, len);
        return target;
    }

    /**
     * Reads ASCII characters at given section of a pipe
     * @param pipe to read from
     * @param target area to read from
     * @param len length of area to read from
     * @return ASCII characters read
     */
	public static <S extends MessageSchema<S>, A extends Appendable> A readASCII(Pipe<S> pipe, A target, int meta, int len) {
		if (meta < 0) {//NOTE: only uses const for const or default, may be able to optimize away this conditional.
	        return readASCIIConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	    } else {
	        return readASCIIRing(pipe,len,target,restorePosition(pipe, meta));
	    }
	}
	
   public static <S extends MessageSchema<S>, A extends Appendable> A readOptionalASCII(Pipe<S> pipe, A target, int meta, int len) {
        if (len<0) {
            return null;
        }
        if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
            return readASCIIConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
        } else {
            return readASCIIRing(pipe,len,target,restorePosition(pipe, meta));
        }
    }

   
   public static <S extends MessageSchema<S>> void skipNextFragment(Pipe<S> pipe) {
		   
	   skipNextFragment(pipe, Pipe.takeMsgIdx(pipe));
	   
   }

    /**
     * Skips over specified section of the pipe
     * @param pipe that you're reading from
     * @param msgIdx TODO: index to skip over or to skip too??
     */
	public static <S extends MessageSchema<S>> void skipNextFragment(Pipe<S> pipe, int msgIdx) {
		   long pos = Pipe.getWorkingTailPosition(pipe);
		   int msgSize = Pipe.sizeOf(pipe, msgIdx);
		   int idx = (int)(pos+msgSize-2);
		   int msgBytesConsumed = Pipe.slab(pipe)[ Pipe.slabMask(pipe) & idx ]; 

		   //position for the bytes consumed is stepped over and we have already moved forward by size of messageIdx header so substract 2.	   
		   pipe.slabRingTail.workingTailPos.value += (msgSize-2); 
		   pipe.blobRingTail.byteWorkingTailPos.value =  pipe.blobMask & (msgBytesConsumed + pipe.blobRingTail.byteWorkingTailPos.value);
		   
		   Pipe.confirmLowLevelRead(pipe, msgSize);
		   Pipe.releaseReadLock(pipe);
	}

    /**
     * Checks if given CharSequence is equal to data in a given area
     * @param pipe used in comparison
     * @param charSeq CharSequence to compare
     * @param meta TODO: ??
     * @param len TODO: ??
     * @return true if they are equal
     */
	public static <S extends MessageSchema<S>> boolean isEqual(Pipe<S> pipe, CharSequence charSeq, int meta, int len) {
		if (len!=charSeq.length()) {
			return false;
		}
		if (meta < 0) {

			int pos = PipeReader.POS_CONST_MASK & meta;

	    	byte[] buffer = pipe.blobConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	    		if (charSeq.charAt(len)!=buffer[pos+len]) {
	    			return false;
	    		}
	        }

		} else {

			byte[] buffer = pipe.blobRing;
			int mask = pipe.blobMask;
			int pos = restorePosition(pipe, meta);

	        while (--len >= 0) {
	    		if (charSeq.charAt(len)!=buffer[mask&(pos+len)]) {
	    			return false;
	    		}
	        }

		}

		return true;
	}

	   public static <S extends MessageSchema<S>> boolean isEqual(Pipe<S> pipe, byte[] expected, int expectedPos, int meta, int len) {
	        if (len>(expected.length-expectedPos)) {
	            return false;
	        }
	        if (meta < 0) {

	            int pos = PipeReader.POS_CONST_MASK & meta;

	            byte[] buffer = pipe.blobConstBuffer;
	            assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	            
	            while (--len >= 0) {
	                if (expected[expectedPos+len]!=buffer[pos+len]) {
	                    return false;
	                }
	            }

	        } else {

	            byte[] buffer = pipe.blobRing;
	            int mask = pipe.blobMask;
	            int pos = restorePosition(pipe, meta);

	            while (--len >= 0) {
	                if (expected[expectedPos+len]!=buffer[mask&(pos+len)]) {
	                    return false;
	                }
	            }

	        }

	        return true;
	    }
	
	   public static boolean isEqual(byte[] aBack, int aPos, int aMask, 
			                         byte[] bBack, int bPos, int bMask, int len) {

		   while (--len>=0) {
			   byte a = aBack[(aPos+len)&aMask];
			   byte b = bBack[(bPos+len)&bMask];
			   if (a!=b) {
				   return false;
			   }
		   }
		   return true;
		   
	   }

	   
	private static <S extends MessageSchema<S>,  A extends Appendable> A readASCIIRing(Pipe<S> pipe, int len, A target, int pos) {
		byte[] buffer = pipe.blobRing;
		int mask = pipe.blobMask;

	    try {
	        while (--len >= 0) {
	            target.append((char)buffer[mask & pos++]);
	        }
	    } catch (IOException e) {
	       throw new RuntimeException(e);
	    }
	    return target;
	}

	private static <S extends MessageSchema<S>, A extends Appendable> A readASCIIConst(Pipe<S> pipe, int len, A target, int pos) {
	    try {
	    	byte[] buffer = pipe.blobConstBuffer;
	    	assert(null!=buffer) : "If constants are used the constByteBuffer was not initialized. Otherwise corruption in the stream has been discovered";
	    	while (--len >= 0) {
	            target.append((char)buffer[pos++]);
	        }
	    } catch (IOException e) {
	       throw new RuntimeException(e);
	    }
	    return target;
	}


    /**
     * Reads UTF8 characters from specified Pipe
     * @param pipe pipe to read from
     * @param target section of pipe to read from
     * @param len number of characters to read
     * @param <S> MessageSchema to extend
     * @param <A> Appendable to extend
     * @return TODO: unsure of return
     */
	public static <S extends MessageSchema<S>, A extends Appendable> A readUTF8(Pipe<S> pipe, A target, int meta, int len) { 
    		if (meta < 0) {//NOTE: only uses const for const or default, may be able to optimize away this conditional.
    	        return (A) readUTF8Const(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
    	    } else {
    	        return (A) readUTF8Ring(pipe,len,target,restorePosition(pipe,meta));
    	    }
	}
	
	   public static <S extends MessageSchema<S>> Appendable readOptionalUTF8(Pipe<S> pipe, Appendable target, int meta, int len) {
	       
    	     if (len<0) {
    	         return null;
    	     }
	        if (meta < 0) {//NOTE: only uses const for const or default, may be able to optimize away this conditional.
	            return readUTF8Const(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	        } else {
	            return readUTF8Ring(pipe,len,target,restorePosition(pipe,meta));
	        }
	        
	    }

	private static <S extends MessageSchema<S>> Appendable readUTF8Const(Pipe<S> pipe, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(pipe.blobConstBuffer, charAndPos, 0xFFFFFFFF); //constants do not wrap
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

	private static <S extends MessageSchema<S>> Appendable readUTF8Ring(Pipe<S> pipe, int bytesLen, Appendable target, int ringPos) {
		  try{
			  long charAndPos = ((long)ringPos)<<32;
			  long limit = ((long)ringPos+bytesLen)<<32;

			  while (charAndPos<limit) {
			      charAndPos = decodeUTF8Fast(pipe.blobRing, charAndPos, pipe.blobMask);
			      target.append((char)charAndPos);
			  }
		  } catch (IOException e) {
			  throw new RuntimeException(e);
		  }
		  return target;
	}

    /**
     * Writes decimal as ASCII to specified Pipe
     * @param readDecimalExponent TODO: unsure
     * @param readDecimalMantissa ??
     * @param outputRing Pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addDecimalAsASCII(int readDecimalExponent,	long readDecimalMantissa, Pipe<S> outputRing) {
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

	public static int safeBlobPosAdd(int pos, long value) {
	    return (int)(Pipe.BYTES_WRAP_MASK&(pos+value));
	}
	

	/**
     * All bytes even those not yet committed.
     *
     * @param ringBuffer
     */
	public static <S extends MessageSchema<S>> int bytesOfContent(Pipe<S> ringBuffer) {
		int dif = (ringBuffer.blobMask&ringBuffer.blobRingHead.byteWorkingHeadPos.value) - (ringBuffer.blobMask&PaddedInt.get(ringBuffer.blobRingTail.bytesTailPos));
		return ((dif>>31)<<ringBuffer.bitsOfBlogRing)+dif;
	}

	public static <S extends MessageSchema<S>> void validateBatchSize(Pipe<S> pipe, int size) {
		if (null != Pipe.from(pipe)) {
			int maxBatch = computeMaxBatchSize(pipe);
			if (size>maxBatch) {
				throw new UnsupportedOperationException("For the configured pipe buffer the batch size can be no larger than "+maxBatch);
			}
		}
	}

	public static <S extends MessageSchema<S>> int computeMaxBatchSize(Pipe<S> rb) {
		return computeMaxBatchSize(rb,2);//default mustFit of 2
	}

	public static <S extends MessageSchema<S>> int computeMaxBatchSize(Pipe<S> pipe, int mustFit) {
		assert(mustFit>=1);
		int maxBatchFromBytes = pipe.maxVarLen==0?Integer.MAX_VALUE:(pipe.sizeOfBlobRing/pipe.maxVarLen)/mustFit;
		int maxBatchFromPrimary = (pipe.sizeOfSlabRing/FieldReferenceOffsetManager.maxFragmentSize(from(pipe)))/mustFit;
		return Math.min(maxBatchFromBytes, maxBatchFromPrimary);
	}

    /**
     * Checks to see if the end of the pipe has been reached
     * @param pipe MessageSchema to be extended
     * @param tailPosition position of assumed end of pipe
     * @param <S> pipe to be checked
     * @return <code>true</code> if end of pipe reached, else <code>false</code>
     */
	public static <S extends MessageSchema<S>> boolean isEndOfPipe(Pipe<S> pipe, long tailPosition) {
		return tailPosition>=pipe.knownPositionOfEOF;
	}
	
	public static void publishEOF(Pipe<?>[] pipe) {
		int i = pipe.length;
		while (--i>=0) {
			if (null != pipe[i] && Pipe.isInit(pipe[i])) {
				publishEOF(pipe[i]);
			}
		}
	}
	
	public static <S extends MessageSchema<S>> void publishEOF(Pipe<S> pipe) {

		if (pipe.slabRingTail.tailPos.get()+pipe.sizeOfSlabRing>=pipe.slabRingHead.headPos.get()+Pipe.EOF_SIZE) {
	
			PaddedInt.set(pipe.blobRingHead.bytesHeadPos,pipe.blobRingHead.byteWorkingHeadPos.value);
			pipe.knownPositionOfEOF = (int)pipe.slabRingHead.workingHeadPos.value +  from(pipe).templateOffset;
			pipe.slabRing[pipe.slabMask & (int)pipe.knownPositionOfEOF]    = -1;
			pipe.slabRing[pipe.slabMask & ((int)pipe.knownPositionOfEOF+1)] = 0;
	
			pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value = pipe.slabRingHead.workingHeadPos.value + Pipe.EOF_SIZE);
		} else {
			log.error("Unable to send EOF, the outgoing pipe is 100% full, downstream stages may not get closed.\n"
					+ "To resolve this issue ensure the outgoing pipe has room for write before calling this.");
			
		}
	}

    /**
     * Copies bytes from specified location to Ring
     * @param source data to be copied
     * @param sourceloc location of data to be copied
     * @param targetloc location to copy data to
     */
	public static void copyBytesFromToRing(byte[] source, int sourceloc, int sourceMask, byte[] target, int targetloc, int targetMask, int length) {
		copyBytesFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask,	length);
	}

    /**
     * Copies ints from specified location to Ring
     * @param source data to be copied
     * @param sourceloc location of data to be copied
     * @param targetloc location to copy data to
     */
	public static void copyIntsFromToRing(int[] source, int sourceloc, int sourceMask, int[] target, int targetloc, int targetMask, int length) {
		copyIntsFromToRingMasked(source, sourceloc & sourceMask, (sourceloc + length) & sourceMask, target, targetloc & targetMask, (targetloc + length) & targetMask, length);
	}

	public static void copyBytesFromArrayToRing(byte[] source, final int sourceloc, byte[] target, int targetloc, int targetMask, int length) {
		///NOTE: the source can never wrap..				
		if (length > 0) {
			final int tStart = targetloc & targetMask;
			final int tStop = (targetloc + length) & targetMask;
			if (tStop > tStart) {
				//the source and target do not wrap
				System.arraycopy(source, sourceloc, target, tStart, length);
			} else {
				//the source does not wrap but the target does
				// done as two copies
				System.arraycopy(source, sourceloc, target, tStart, length-tStop);
				System.arraycopy(source, sourceloc + length - tStop, target, 0, tStop);
			}
		}
	}
	
	public static <S extends MessageSchema<S>, T extends MessageSchema<T>> void addByteArray(Pipe<S> source, Pipe<T> target) {
				
		int sourceMeta = Pipe.takeRingByteMetaData(source);
		int sourceLen  = Pipe.takeRingByteLen(source);
		
		Pipe.validateVarLength(target, sourceLen);
		
		Pipe.copyBytesFromToRing(Pipe.byteBackingArray(sourceMeta, source),
				                 Pipe.bytePosition(sourceMeta, source, sourceLen), 
				                 Pipe.blobMask(source), 
				                 target.blobRing, 
				                 target.blobRingHead.byteWorkingHeadPos.value, 
				                 target.blobMask, 
				                 sourceLen);
		
		Pipe.addBytePosAndLen(target, target.blobRingHead.byteWorkingHeadPos.value, sourceLen);
		target.blobRingHead.byteWorkingHeadPos.value = Pipe.BYTES_WRAP_MASK&(target.blobRingHead.byteWorkingHeadPos.value + sourceLen);
		
	}
	
	private static void copyBytesFromToRingMasked(byte[] source,
			final int rStart, final int rStop, byte[] target, final int tStart,
			final int tStop, int length) {
		if (length > 0) {
			if (tStop > tStart) {
				//do not accept the equals case because this can not work with data the same length as as the buffer
				doubleMaskTargetDoesNotWrap(source, rStart, rStop, target, tStart, length);
			} else {
				doubleMaskTargetWraps(source, rStart, rStop, target, tStart, tStop,	length);
			}
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

	@Deprecated //use the Appendables methods
	public static <S extends MessageSchema<S>> int leftConvertIntToASCII(Pipe<S> pipe, int value, int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = pipe.blobRing;
		int tmp = Math.abs(value);
		int max = idx;
		do {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			int t = tmp/10;
			int r = tmp%10;
			target[pipe.blobMask&--idx] = (byte)('0'+r);
			tmp = t;
		} while (0!=tmp);
		target[pipe.blobMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>31));

		//shift it down to the head
		int length = max-idx;
		if (idx!=pipe.blobRingHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[pipe.blobMask & (s+pipe.blobRingHead.byteWorkingHeadPos.value)] = target[pipe.blobMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

	@Deprecated //use the Appendables methods
	public static <S extends MessageSchema<S>> int leftConvertLongToASCII(Pipe<S> pipe, long value,	int idx) {
		//max places is value for -2B therefore its 11 places so we start out that far and work backwards.
		//this will leave a gap but that is not a problem.
		byte[] target = pipe.blobRing;
		long tmp = Math.abs(value);
		int max = idx;
		do {
			//do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
			long t = tmp/10;
			long r = tmp%10;
			target[pipe.blobMask&--idx] = (byte)('0'+r);
			tmp = t;
		} while (0!=tmp);
		target[pipe.blobMask& (idx-1)] = (byte)'-';
		//to make it positive we jump over the sign.
		idx -= (1&(value>>63));

		int length = max-idx;
		//shift it down to the head
		if (idx!=pipe.blobRingHead.byteWorkingHeadPos.value) {
			int s = 0;
			while (s<length) {
				target[pipe.blobMask & (s+pipe.blobRingHead.byteWorkingHeadPos.value)] = target[pipe.blobMask & (s+idx)];
				s++;
			}
		}
		return length;
	}

   public static <S extends MessageSchema<S>> int leftConvertLongWithLeadingZerosToASCII(Pipe<S> pipe, int chars, long value, int idx) {
        //max places is value for -2B therefore its 11 places so we start out that far and work backwards.
        //this will leave a gap but that is not a problem.
        byte[] target = pipe.blobRing;
        long tmp = Math.abs(value);
        int max = idx;

        do {
            //do not touch these 2 lines they make use of secret behavior in hot spot that does a single divide.
            long t = tmp/10;
            long r = tmp%10;
            target[pipe.blobMask&--idx] = (byte)('0'+r);
            tmp = t;
            chars--;
        } while (0!=tmp);
        while(--chars>=0) {
            target[pipe.blobMask&--idx] = '0';
        }

        target[pipe.blobMask& (idx-1)] = (byte)'-';
        //to make it positive we jump over the sign.
        idx -= (1&(value>>63));

        int length = max-idx;
        //shift it down to the head
        if (idx!=pipe.blobRingHead.byteWorkingHeadPos.value) {
            int s = 0;
            while (s<length) {
                target[pipe.blobMask & (s+pipe.blobRingHead.byteWorkingHeadPos.value)] = target[pipe.blobMask & (s+idx)];
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
	       log.error("Invalid encoding, low byte must have bits of 10xxxxxx but we find {}. conclusion: this data was not UTF8 encoded.",Integer.toBinaryString(source[mask&sourcePos]),new Exception("Check for pipe corruption or unprintable data"));
	       sourcePos += 1;
	       return (((long)sourcePos)<<32) | 0xFFFD; // Bad data replacement char
	    }
	    long chr = ((result << 6) | (int)(source[mask&sourcePos++] & 0x3F)); //6 bits
	    return (((long)sourcePos)<<32) | chr;
	  }

	public static <S extends MessageSchema<S>> int copyASCIIToBytes(CharSequence source, Pipe<S> rbRingBuffer) {
		return copyASCIIToBytes(source, 0, source.length(), rbRingBuffer);
	}

    /**
     * Writes ASCII to specified pipe
     * @param source characters to write
     * @param rb pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addASCII(CharSequence source, Pipe<S> rb) {
	    addASCII(source, 0, null==source ? -1 : source.length(), rb);
	}

	public static <S extends MessageSchema<S>> void addASCII(CharSequence source, int sourceIdx, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static <S extends MessageSchema<S>> void addASCII(char[] source, int sourceIdx, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, copyASCIIToBytes(source, sourceIdx, sourceCharCount, rb), sourceCharCount);
	}

	public static <S extends MessageSchema<S>> int copyASCIIToBytes(CharSequence source, int sourceIdx, final int sourceLen, Pipe<S> rbRingBuffer) {
		final int p = rbRingBuffer.blobRingHead.byteWorkingHeadPos.value;
		
	    if (sourceLen > 0) {
	    	int tStart = p & rbRingBuffer.blobMask;
	        copyASCIIToBytes2(source, sourceIdx, sourceLen, rbRingBuffer, p, rbRingBuffer.blobRing, tStart, 1+rbRingBuffer.blobMask - tStart);
	    }
		return p;
	}

	private static <S extends MessageSchema<S>> void copyASCIIToBytes2(CharSequence source, int sourceIdx,
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

    public static <S extends MessageSchema<S>> int copyASCIIToBytes(char[] source, int sourceIdx, final int sourceLen, Pipe<S> rbRingBuffer) {
		final int p = rbRingBuffer.blobRingHead.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = rbRingBuffer.blobMask;
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

    /**
     * Writes UTF8 characters to specified pipe
     * @param source characters to write
     * @param rb pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addUTF8(CharSequence source, Pipe<S> rb) {
	    addUTF8(source, null==source? -1 : source.length(), rb);
	}

    /**
     * Writes UTF8 characters to specified pipe
     * @param source characters to write
     * @param sourceCharCount character count of the source to write
     * @param rb pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addUTF8(CharSequence source, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, rb.blobRingHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,0, sourceCharCount, rb));
	}

    /**
     * Writes array of UTF8 characters to specified pipe
     * @param source characters to write
     * @param sourceCharCount character count of the source to write
     * @param rb pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addUTF8(char[] source, int sourceCharCount, Pipe<S> rb) {
		addBytePosAndLen(rb, rb.blobRingHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,sourceCharCount,rb));
	}

	/**
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
   public static <S extends MessageSchema<S>> int copyUTF8ToByte(CharSequence source, int sourceOffset, int sourceCharCount, Pipe<S> pipe) {
        if (sourceCharCount>0) {
            int byteLength = Pipe.copyUTF8ToByte(source, sourceOffset, pipe.blobRing, pipe.blobMask, pipe.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
            pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value+byteLength);
            return byteLength;
        } else {
            return 0;
        }
    }

	public static int copyUTF8ToByte(CharSequence source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {
	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source.charAt(sourceIdx+c++), target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}

	public static void xorRandomToBytes(Random r, byte[] target, int targetIdx, int count, int targetMask) {
		while (--count>=0) {
			target[targetMask& (targetIdx+count)] ^= r.nextInt(256);
		}
	}
	
	public static void xorBytesToBytes(byte[] source, int sourceIdx, int sourceMask,
			                            byte[] target, int targetIdx, int targetMask, 
			                            int count) {
		for(int i=0; i<count; i++) {
			target[targetMask & (targetIdx+count)] ^= source[sourceMask & (sourceIdx+count)];
		}
	}	
	
	
	/**
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
	public static <S extends MessageSchema<S>> int copyUTF8ToByte(char[] source, int sourceCharCount, Pipe<S> rb) {
		int byteLength = Pipe.copyUTF8ToByte(source, 0, rb.blobRing, rb.blobMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
		rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
		return byteLength;
	}

	public static <S extends MessageSchema<S>> int copyUTF8ToByte(char[] source, int sourceOffset, int sourceCharCount, Pipe<S> rb) {
	    int byteLength = Pipe.copyUTF8ToByte(source, sourceOffset, rb.blobRing, rb.blobMask, rb.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
	    rb.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(rb.blobRingHead.byteWorkingHeadPos.value+byteLength);
	    return byteLength;
	}

	private static <S extends MessageSchema<S>> int copyUTF8ToByte(char[] source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {

	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source[sourceIdx+c++], target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}

	public static <S extends MessageSchema<S>> int encodeSingleChar(int c, byte[] buffer,int mask, int pos) {

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

	private static <S extends MessageSchema<S>> int rareEncodeCase(int c, byte[] buffer, int mask, int pos) {
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

	public static <S extends MessageSchema<S>> void addByteBuffer(ByteBuffer source, Pipe<S> pipe) {
	    int bytePos = pipe.blobRingHead.byteWorkingHeadPos.value;
	    int len = -1;
	    if (null!=source) {
	    	if (source.hasRemaining()) {
	    		len = source.remaining();
	        	copyByteBuffer(source,source.remaining(),pipe);
	    	} else {
	    		len = 0;
	    	}
	    }
	    //System.out.println("len to write "+len+" text:"+  readUTF8Ring(pipe, len, new StringBuilder(), bytePos));

	    Pipe.addBytePosAndLen(pipe, bytePos, len);
	}

   public static <S extends MessageSchema<S>> void addByteBuffer(ByteBuffer source, int length, Pipe<S> rb) {
        int bytePos = rb.blobRingHead.byteWorkingHeadPos.value;
        int len = -1;
        if (null!=source && length>0) {
            len = length;
            copyByteBuffer(source,length,rb);
        }
        Pipe.addBytePosAndLen(rb, bytePos, len);
    }
	   
	public static <S extends MessageSchema<S>> void copyByteBuffer(ByteBuffer source, int length, Pipe<S> rb) {
		validateVarLength(rb, length);
		int idx = rb.blobRingHead.byteWorkingHeadPos.value & rb.blobMask;
		int partialLength = 1 + rb.blobMask - idx;
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

	public static <S extends MessageSchema<S>> void addByteArrayWithMask(final Pipe<S> outputRing, int mask, int len, byte[] data, int offset) {
		validateVarLength(outputRing, len);
		copyBytesFromToRing(data,offset,mask,outputRing.blobRing,PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),outputRing.blobMask, len);
		addBytePosAndLenSpecial(outputRing, PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),len);
		PaddedInt.set(outputRing.blobRingHead.byteWorkingHeadPos, BYTES_WRAP_MASK&(PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos) + len));
	}
	
    public static <S extends MessageSchema<S>> void setByteArrayWithMask(final Pipe<S> outputRing, int mask, int len, byte[] data, int offset, long slabPosition) {
	        validateVarLength(outputRing, len);
	        copyBytesFromToRing(data,offset,mask,outputRing.blobRing,PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),outputRing.blobMask, len);
            setBytePosAndLen(slab(outputRing), outputRing.slabMask, slabPosition, PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos), len, bytesWriteBase(outputRing));
	        PaddedInt.set(outputRing.blobRingHead.byteWorkingHeadPos, BYTES_WRAP_MASK&(PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos) + len));
	}

	public static <S extends MessageSchema<S>> int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static <S extends MessageSchema<S>> long peekLong(int[] buf, long pos, int mask) {

        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    public static <S extends MessageSchema<S>> boolean isShutdown(Pipe<S> pipe) {
    	return pipe.imperativeShutDown.get();
    }

    public static <S extends MessageSchema<S>> void shutdown(Pipe<S> pipe) {
    	if (!pipe.imperativeShutDown.getAndSet(true)) {
    		pipe.firstShutdownCaller = new PipeException("Shutdown called from this stacktrace");
    	}

    }

    /**
     * Writes a byte array to the specified pipe
     * @param source byte array to write
     * @param pipe pipe to write to
     * @param <S> MessageSchema to extend
     */
    public static <S extends MessageSchema<S>> void addByteArray(byte[] source, Pipe<S> pipe) {
    	addByteArray(source,0,source.length,pipe);
    }

    /**
     * Writes a byte array to the specified pipe
     * @param source byte array to write
     * @param sourceIdx index of the source array
     * @param sourceLen length of the array to be written TODO: max length?
     * @param pipe pipe to be written to
     * @param <S> MessageSchema to be extended
     */
    public static <S extends MessageSchema<S>> void addByteArray(byte[] source, int sourceIdx, int sourceLen, Pipe<S> pipe) {
    	assert(sourceLen>=0);
		validateVarLength(pipe, sourceLen);
		
		copyBytesFromArrayToRing(source, sourceIdx, pipe.blobRing, pipe.blobRingHead.byteWorkingHeadPos.value, pipe.blobMask, sourceLen);
		
		addBytePosAndLen(pipe, pipe.blobRingHead.byteWorkingHeadPos.value, sourceLen);
		pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value + sourceLen);
    }

    /**
     * Writes a a byte array to the specified pipe
     * @param source byte array to write
     * @param sourceIdx index of the source array
     * @param sourceLen  max? length of the array to be written
     * @param sourceMask TODO: not sure of description
     * @param pipe pipe to be written to
     * @param <S> MessageSchema to be extended
     */
    public static <S extends MessageSchema<S>> void addByteArray(byte[] source, int sourceIdx, int sourceLen, int sourceMask, Pipe<S> pipe) {

    	assert(sourceLen>=0);
    	validateVarLength(pipe, sourceLen);

    	copyBytesFromToRing(source, sourceIdx, sourceMask, pipe.blobRing, pipe.blobRingHead.byteWorkingHeadPos.value, pipe.blobMask, sourceLen);

    	addBytePosAndLen(pipe, pipe.blobRingHead.byteWorkingHeadPos.value, sourceLen);
        pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value + sourceLen);

    }

    public static <S extends MessageSchema<S>> void addNullByteArray(Pipe<S> pipe) {
        addBytePosAndLen(pipe, pipe.blobRingHead.byteWorkingHeadPos.value, -1);
    }

    /**
     * Writes an int value to the specified pipe
     * @param value int to be written
     * @param pipe pipe to be written to
     * @param <S> MessageSchema to extend
     */
    public static <S extends MessageSchema<S>> void addIntValue(int value, Pipe<S> pipe) {
         assert(pipe.slabRingHead.workingHeadPos.value <= Pipe.tailPosition(pipe)+pipe.sizeOfSlabRing);
         //TODO: not always working in deep structures, check offsets:  assert(isValidFieldTypePosition(rb, TypeMask.IntegerSigned, TypeMask.IntegerSignedOptional, TypeMask.IntegerUnsigned, TypeMask.IntegerUnsignedOptional, TypeMask.Decimal));
		 setValue(pipe.slabRing,pipe.slabMask,pipe.slabRingHead.workingHeadPos.value++,value);
	}

	private static <S extends MessageSchema<S>> boolean isValidFieldTypePosition(Pipe<S> rb, int ... expected) {
		FieldReferenceOffsetManager from = Pipe.from(rb);
		
         if (from.hasSimpleMessagesOnly && !isForDynamicSchema(rb)) {
        	 long offset =  Pipe.workingHeadPosition(rb)-Pipe.headPosition(rb);
        	 int[] starts = from.messageStarts();
        	 int j = starts.length;
        	 boolean found = false;
        	 String[] suggestions = new String[j];
        	 while (--j>=0) {
        		 
        		 //TODO: after walking over longs and strings may be off, TODO: double check this before using it again.
        		 
        		 int idx = starts[j]+1; //skip over msg id field of fixed size.
        		 int rem = (int)(offset-1);//skip over msg id
        		 
        		 while (rem>0) {
        			 rem -= from.fragDataSize[idx++];
        		 }
        		 int type = TokenBuilder.extractType(from.tokens[idx]);
        		 suggestions[j]=(TokenBuilder.tokenToString(from.tokens[idx]));
        		 
        		 int x = expected.length;
        		 while (--x>=0) {
        			 found |= type==expected[x];
        		 }        		 
        	 }
        	 if (!found) {
        		 log.error("Field type mismatch, no messages have an {} in this position perhaps you wanted one of these {}", TypeMask.toString(expected), Arrays.toString(suggestions));
        		 return false;
        	 }        
         }
         return true;
	}

    /**
     * Writes int value to specific position in the given pipe
     * @param value int to be written
     * @param pipe pipe to be written to
     * @param position position to write int
     * @param <S> MessageSchema to extend
     */
    public static <S extends MessageSchema<S>> void setIntValue(int value, Pipe<S> pipe, long position) {
        assert(pipe.slabRingHead.workingHeadPos.value <= Pipe.tailPosition(pipe)+pipe.sizeOfSlabRing);
        setValue(pipe.slabRing,pipe.slabMask,position,value);
    }
    
    public static <S extends MessageSchema<S>> void orIntValue(int value, Pipe<S> pipe, long position) {
        assert(pipe.slabRingHead.workingHeadPos.value <= Pipe.tailPosition(pipe)+pipe.sizeOfSlabRing);
        orValue(pipe.slabRing,pipe.slabMask,position,value);
    }

    //
    //TODO: URGENT, A, It may be much nicer to add a method called 'beginMessage' which does only the base work and then moves the cursor forward one.
    //         Then we can take the confirm write and it can go back and set the id. Also add asserts on all fiels that this happens first !!!
    //
    
    //TODO: How can we test that the msgIdx that is passed in is only applicable to S ?? we need a way to check this.
    
    //must be called by low-level API when starting a new message
    public static <S extends MessageSchema<S>> int addMsgIdx(final Pipe<S> pipe, int msgIdx) {
         assert(Pipe.workingHeadPosition(pipe)<(Pipe.tailPosition(pipe)+ pipe.sizeOfSlabRing  /*    pipe.slabMask*/  )) : "Working position is now writing into published(unreleased) tail "+
                Pipe.workingHeadPosition(pipe)+"<"+Pipe.tailPosition(pipe)+"+"+pipe.sizeOfSlabRing /*pipe.slabMask*/+" total "+((Pipe.tailPosition(pipe)+pipe.slabMask));
        
         assert(pipe.slabRingHead.workingHeadPos.value <= ((long)pipe.sizeOfSlabRing)+Pipe.tailPosition(pipe)) : 
                "Tail is at: "+Pipe.tailPosition(pipe)+" and Head at: "+pipe.slabRingHead.workingHeadPos.value+" but they are too far apart because the pipe is only of size: "+pipe.sizeOfSlabRing+
                "\n Double check the calls to confirmLowLevelWrite that the right size is used, and confirm that hasRoomForWrite is called.  ";
         
    	 assert(msgIdx>=0) : "Call publishEOF() instead of this method";

     	//this MUST be done here at the START of a message so all its internal fragments work with the same base position
     	 markBytesWriteBase(pipe);

   // 	 assert(rb.llwNextHeadTarget<=rb.headPos.get() || rb.workingHeadPos.value<=rb.llwNextHeadTarget) : "Unsupported mix of high and low level API.";
            	 
     	 assert(null != pipe.slabRing) : "Pipe must be init before use";
     	 
		 pipe.slabRing[pipe.slabMask & (int)pipe.slabRingHead.workingHeadPos.value++] = msgIdx;
		 
		 final int size = Pipe.from(pipe).fragDataSize[msgIdx];
	   	    
		    //////////////////////////////////////
		    //this block is here to allow high level calls to follow low level calls.
		    //////////////////////////////////////
		    if (null != pipe.ringWalker) {
		    	pipe.ringWalker.nextWorkingHead += size;
		    }//for mixing high and low calls
		    //////////////////////////////////////
		 
		 return size;
		 
	}

	public static <S extends MessageSchema<S>> void setValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] = value;
    }

	public static <S extends MessageSchema<S>> void orValue(int[] buffer, int rbMask, long offset, int value) {
        buffer[rbMask & (int)offset] |= value;
    }

    /**
     * Writes position and length of byte to add to the pipe
     * @param pipe pipe to be written to
     * @param position position of byte
     * @param length length of byte
     * @param <S> MessageSchema to be extended
     */
    public static <S extends MessageSchema<S>> void addBytePosAndLen(Pipe<S> pipe, int position, int length) {
        addBytePosAndLenSpecial(pipe,position,length);
    }

    public static <S extends MessageSchema<S>> void addBytePosAndLenSpecial(Pipe<S> targetOutput, final int startBytePos, int bytesLength) {
        PaddedLong workingHeadPos = getWorkingHeadPositionObject(targetOutput);
        setBytePosAndLen(slab(targetOutput), targetOutput.slabMask, workingHeadPos.value, 
        		         startBytePos, bytesLength, 
        		         bytesWriteBase(targetOutput));
        PaddedLong.add(workingHeadPos, 2);
    }

	public static <S extends MessageSchema<S>> void setBytePosAndLen(int[] buffer, int bufferMask, long bufferPos, int dataBlobPos, int dataBlobLen, int baseBytePos) {
	   	//negative position is written as is because the internal array does not have any offset (but it could some day)
    	//positive position is written after subtracting the rbRingBuffer.bytesHeadPos.longValue()
    	if (dataBlobPos>=0) {
    		buffer[bufferMask & (int)bufferPos] = (int)(dataBlobPos-baseBytePos) & Pipe.BYTES_WRAP_MASK; //mask is needed for the negative case, does no harm in positive case
    	} else {
    		buffer[bufferMask & (int)bufferPos] = dataBlobPos;
    	}
        buffer[bufferMask & (int)(bufferPos+1)] = dataBlobLen;
	}
	

	static <S extends MessageSchema<S>> int restorePosition(Pipe<S> pipe, int pos) {
		assert(pos>=0);
		return pos + Pipe.bytesReadBase(pipe);
	}

	
	/*
	 * WARNING: this method has side effect of moving byte pointer.
	 */
    public static <S extends MessageSchema<S>> int bytePosition(int meta, Pipe<S> pipe, int len) {
    	int pos =  restorePosition(pipe, meta & RELATIVE_POS_MASK);
        if (len>=0) {
        	Pipe.addAndGetBytesWorkingTailPosition(pipe, len);
        }        
        return pos;
    }
    
    //WARNING: this has no side effect
    public static <S extends MessageSchema<S>> int convertToPosition(int meta, Pipe<S> pipe) {
    	return restorePosition(pipe, meta & RELATIVE_POS_MASK);
    }


    public static <S extends MessageSchema> void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2, int value3) {

        long p = headCache.value;
        buffer[rbMask & (int)p++] = value1;
        buffer[rbMask & (int)p++] = value2;
        buffer[rbMask & (int)p++] = value3;
        headCache.value = p;

    }

    @Deprecated
    public static <S extends MessageSchema<S>> void addValues(int[] buffer, int rbMask, PaddedLong headCache, int value1, long value2) {

        headCache.value = setValues(buffer, rbMask, headCache.value, value1, value2);

    }

    public static <S extends MessageSchema<S>> void addDecimal(int exponent, long mantissa, Pipe<S> pipe) {
        pipe.slabRingHead.workingHeadPos.value = setValues(pipe.slabRing, pipe.slabMask, pipe.slabRingHead.workingHeadPos.value, exponent, mantissa);
    }


	static <S extends MessageSchema<S>> long setValues(int[] buffer, int rbMask, long pos, int value1, long value2) {
		buffer[rbMask & (int)pos++] = value1;
        buffer[rbMask & (int)pos++] = (int)(value2 >>> 32);
        buffer[rbMask & (int)pos++] = (int)(value2 & 0xFFFFFFFF);
		return pos;
	}

	@Deprecated //use addLongValue(value, rb)
    public static <S extends MessageSchema<S>> void addLongValue(Pipe<S> pipe, long value) {
		 addLongValue(value, pipe);
	}

    /**
     * Writes long value to the specified pipe
     * @param value long to be written
     * @param rb pipe to be written to
     * @param <S> MessageSchema to be extended
     */
	public static <S extends MessageSchema<S>> void addLongValue(long value, Pipe<S> rb) {
		 addLongValue(rb.slabRing, rb.slabMask, rb.slabRingHead.workingHeadPos, value);
	}

    public static <S extends MessageSchema<S>> void addLongValue(int[] buffer, int rbMask, PaddedLong headCache, long value) {

        long p = headCache.value;
        buffer[rbMask & (int)p] = (int)(value >>> 32);
        buffer[rbMask & (int)(p+1)] = ((int)value);
        headCache.value = p+2;

    }

    static <S extends MessageSchema<S>> int readRingByteLen(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int) (rbMask & (rbPos + fieldPos + 1))];// second int is always the length
    }

	public static <S extends MessageSchema<S>> int readRingByteLen(int idx, Pipe<S> pipe) {
		return readRingByteLen(idx,pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value);
	}

	public static <S extends MessageSchema<S>> int takeRingByteLen(Pipe<S> pipe) {
	//    assert(ring.structuredLayoutRingTail.workingTailPos.value<RingBuffer.workingHeadPosition(pipe));
		return pipe.slabRing[(int)(pipe.slabMask & (pipe.slabRingTail.workingTailPos.value++))];// second int is always the length
	}



    public static <S extends MessageSchema<S>> byte[] byteBackingArray(int meta, Pipe<S> pipe) {
        return pipe.blobRingLookup[meta>>>31];
    }

	public static <S extends MessageSchema<S>> int readRingByteMetaData(int pos, Pipe<S> pipe) {
		return readValue(pos,pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
	}

	//TODO: must always read metadata before length, easy mistake to make, need assert to ensure this is caught if happens.
	public static <S extends MessageSchema<S>> int takeRingByteMetaData(Pipe<S> pipe) {
		return readValue(0,pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value++);
	}

    static <S extends MessageSchema<S>> int readValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }

    //TODO: may want to deprecate this interface

    /**
     * Reads the data at a specific index on the given pipe and returns an int
     * @param idx index to read
     * @param pipe pipe to read from
     * @param <S> MessageSchema to extend
     * @return int value from specified index
     */
    public static <S extends MessageSchema<S>> int readValue(int idx, Pipe<S> pipe) {
    	return readValue(idx, pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
    }

    public static <S extends MessageSchema<S>> int takeInt(Pipe<S> pipe) {
    	return readValue(pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value++);
    }
    
    @Deprecated //use takeInt
    public static <S extends MessageSchema<S>> int takeValue(Pipe<S> pipe) {
    	return takeInt(pipe);
    }
    
    public static <S extends MessageSchema<S>> int readValue(int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & rbPos)];
    }
    
    public static <S extends MessageSchema<S>> Integer takeOptionalValue(Pipe<S> pipe) {
        int absent32Value = FieldReferenceOffsetManager.getAbsent32Value(Pipe.from(pipe));
        return takeOptionalValue(pipe, absent32Value);
    }

    public static <S extends MessageSchema<S>> Integer takeOptionalValue(Pipe<S> pipe, int absent32Value) {
        int temp = readValue(0, pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value++);
        return absent32Value!=temp ? new Integer(temp) : null;
    }

    public static <S extends MessageSchema<S>> long takeLong(Pipe<S> pipe) {
        
        //this assert does not always work because the head position is volatile, Not sure what should be done to resolve it.  
        //assert(ring.slabRingTail.workingTailPos.value<Pipe.workingHeadPosition(ring)) : "working tail "+ring.slabRingTail.workingTailPos.value+" but head is "+Pipe.workingHeadPosition(ring);
    	
        long result = readLong(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
    	pipe.slabRingTail.workingTailPos.value+=2;
    	return result;
    }
    
    public static <S extends MessageSchema<S>> Long takeOptionalLong(Pipe<S> pipe) {
        long absent64Value = FieldReferenceOffsetManager.getAbsent64Value(Pipe.from(pipe));
        return takeOptionalLong(pipe, absent64Value);
    }

    public static <S extends MessageSchema<S>> Long takeOptionalLong(Pipe<S> pipe, long absent64Value) {
        assert(pipe.slabRingTail.workingTailPos.value<Pipe.workingHeadPosition(pipe)) : "working tail "+pipe.slabRingTail.workingTailPos.value+" but head is "+Pipe.workingHeadPosition(pipe);
        long result = readLong(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
        pipe.slabRingTail.workingTailPos.value+=2;
        return absent64Value!=result ? new Long(result) : null;
    }

    /**
     * Reads the value at specified index on the given pipe and returns a long
     * @param idx index to read
     * @param pipe pipe to read from
     * @param <S> MessageSchema to extend
     * @return long value from specified index
     */
    public static <S extends MessageSchema<S>> long readLong(int idx, Pipe<S> pipe) {
    	return readLong(pipe.slabRing,pipe.slabMask,idx+pipe.slabRingTail.workingTailPos.value);

    }

    /**
     * Gets the index of a message in the pipe
     * @param pipe pipe to check
     * @return message index
     */
    public static <S extends MessageSchema<S>> int takeMsgIdx(Pipe<S> pipe) {
        
    	assert(PipeMonitor.monitor(pipe,
    			      pipe.slabRingTail.workingTailPos.value,
    			      Pipe.bytesReadBase(pipe)
    			));
  
    	return pipe.lastMsgIdx = readValue(pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value++);
    }
    
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected) {
        return (Pipe.contentRemaining(pipe)>0) && peekInt(pipe)==expected;    	
    }
    
    public static <S extends MessageSchema<S>> boolean peekNotMsg(Pipe<S> pipe, int expected) {
        return (Pipe.contentRemaining(pipe)>0) && peekInt(pipe)!=expected;    	
    }    

    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected1, int expected2) {
        return (Pipe.contentRemaining(pipe)>0) && (peekInt(pipe)==expected1 || peekInt(pipe)==expected2);
    }
    
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected1, int expected2, int expected3) {
        return (Pipe.contentRemaining(pipe)>0) && (peekInt(pipe)==expected1 || peekInt(pipe)==expected2 || peekInt(pipe)==expected3);
    }
    
    public static <S extends MessageSchema<S>> int peekInt(Pipe<S> pipe) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readValue(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
    }
    
    public static <S extends MessageSchema<S>> int peekInt(Pipe<S> pipe, int offset) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readValue(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value+offset);
    }
   
    public static <S extends MessageSchema<S>> long peekLong(Pipe<S> pipe, int offset) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readLong(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value+offset);
    }
    
    public static <S extends MessageSchema<S>, A extends Appendable> A peekUTF8(Pipe<S> pipe, int offset, A target) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
    	return readUTF8(pipe, target, peekInt(pipe,offset),peekInt(pipe,offset+1));
    }

    public static <S extends MessageSchema<S>> int contentRemaining(Pipe<S> pipe) {
        int result = (int)(pipe.slabRingHead.headPos.get() - pipe.slabRingTail.tailPos.get()); //must not go past add count because it is not release yet.
        assert(result>=0) : "content remaining must never be negative. problem in "+schemaName(pipe)+" pipe "; //do not add pipe.toString since it will be recursive.
        return result;
    }

    /**
     * Checks to see whether pipe has any data or not
     * @param pipe pipe to be checked
     * @param <S> MessageSchema to extend
     * @return <code>true</code> if pipe has no data else <code>false</code>
     */
    public static <S extends MessageSchema<S>> boolean isEmpty(Pipe<S> pipe) {
    	return pipe.slabRingHead.workingHeadPos.value == pipe.slabRingTail.workingTailPos.value;
    }

    public static <S extends MessageSchema<S>> int releaseReadLock(Pipe<S> pipe) {
     	  
    	
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        int bytesConsumedByFragment = takeInt(pipe);

        assert(bytesConsumedByFragment>=0) : "Bytes consumed by fragment must never be negative, was fragment written correctly?, is read positioned correctly?";
        Pipe.markBytesReadBase(pipe, bytesConsumedByFragment);  //the base has been moved so we can also use it below.
        assert(Pipe.contentRemaining(pipe)>=0) : "value "+Pipe.contentRemaining(pipe); 
        long tail = pipe.slabRingTail.workingTailPos.value;
		batchedReleasePublish(pipe, 
        		              pipe.blobRingTail.byteWorkingTailPos.value = pipe.blobReadBase, 
        		              tail);
        assert(validateInsideData(pipe, pipe.blobReadBase));
        
        return bytesConsumedByFragment;        
    }

    
    public static <S extends MessageSchema<S>> int readNextWithoutReleasingReadLock(Pipe<S> pipe) {
        int bytesConsumedByFragment = takeInt(pipe); 
        Pipe.markBytesReadBase(pipe, bytesConsumedByFragment); //the base has been moved so we can also use it below.
        assert(Pipe.contentRemaining(pipe)>=0);
        PendingReleaseData.appendPendingReadRelease(pipe.pendingReleases,
                                                    pipe.slabRingTail.workingTailPos.value, 
                                                    pipe.blobRingTail.byteWorkingTailPos.value = pipe.blobReadBase, 
                                                    bytesConsumedByFragment);
        assert(validateInsideData(pipe, pipe.blobReadBase));
        return bytesConsumedByFragment;   
    }
    

    @Deprecated //inline and use releaseReadLock(pipe)
    public static <S extends MessageSchema<S>> int releaseReads(Pipe<S> pipe) {    	
        return releaseReadLock(pipe);     
    }

    public static <S extends MessageSchema<S>> void batchedReleasePublish(Pipe<S> pipe, int blobTail, long slabTail) {
        assert(null==pipe.ringWalker || pipe.ringWalker.cursor<=0 && !PipeReader.isNewMessage(pipe.ringWalker)) : "Unsupported mix of high and low level API.  ";
        releaseBatchedReads(pipe, blobTail, slabTail);
    }
    
    static <S extends MessageSchema<S>> void releaseBatchedReads(Pipe<S> pipe, int workingBlobRingTailPosition, long nextWorkingTail) {

    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        if (decBatchRelease(pipe)<=0) { 
           setBytesTail(pipe, workingBlobRingTailPosition);
           
           //NOTE: the working tail is in use as part of the read and should not be modified
           //      this method only modifies the externally visible tail to let writers see it.
           pipe.slabRingTail.tailPos.lazySet(nextWorkingTail);
           
           beginNewReleaseBatch(pipe); 
           
           assert(validateInsideData(pipe, pipe.blobReadBase));
           
        } else {
           storeUnpublishedTail(pipe, nextWorkingTail, workingBlobRingTailPosition);            
        }
    }

    static <S extends MessageSchema<S>> void storeUnpublishedTail(Pipe<S> pipe, long workingTailPos, int byteWorkingTailPos) {
        pipe.lastReleasedBlobTail = byteWorkingTailPos;
        pipe.lastReleasedSlabTail = workingTailPos;
    }
   
        

    /**
     * Release any reads that were held back due to batching.
     * @param pipe pipe to be examined
     */
    public static <S extends MessageSchema<S>> void releaseAllBatchedReads(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        if (pipe.lastReleasedSlabTail > pipe.slabRingTail.tailPos.get()) {
            PaddedInt.set(pipe.blobRingTail.bytesTailPos,pipe.lastReleasedBlobTail);
            pipe.slabRingTail.tailPos.lazySet(pipe.lastReleasedSlabTail);
            pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
        }

        assert(debugHeadAssignment(pipe));
    }
    
    public static <S extends MessageSchema<S>> void releaseBatchedReadReleasesUpToThisPosition(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        long newTailToPublish = Pipe.getWorkingTailPosition(pipe);
        int newTailBytesToPublish = Pipe.getWorkingBlobRingTailPosition(pipe);
        
        //int newTailBytesToPublish = RingBuffer.bytesReadBase(ring);
        
        releaseBatchedReadReleasesUpToPosition(pipe, newTailToPublish, newTailBytesToPublish);
                
    }

    public static <S extends MessageSchema<S>> void releaseBatchedReadReleasesUpToPosition(Pipe<S> pipe, long newTailToPublish,  int newTailBytesToPublish) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
    	assert(newTailToPublish<=pipe.lastReleasedSlabTail) : "This new value is forward of the next Release call, eg its too large";
        assert(newTailToPublish>=pipe.slabRingTail.tailPos.get()) : "This new value is behind the existing published Tail, eg its too small ";
        
//        //TODO: These two asserts would be nice to have but the int of bytePos wraps every 2 gig causing false positives, these need more mask logic to be right
//        assert(newTailBytesToPublish<=ring.lastReleasedBytesTail) : "This new value is forward of the next Release call, eg its too large";
//        assert(newTailBytesToPublish>=ring.unstructuredLayoutRingTail.bytesTailPos.value) : "This new value is behind the existing published Tail, eg its too small ";
//        assert(newTailBytesToPublish<=ring.bytesWorkingTailPosition(ring)) : "Out of bounds should never be above working tail";
//        assert(newTailBytesToPublish<=ring.bytesHeadPosition(ring)) : "Out of bounds should never be above head";
        
        
        PaddedInt.set(pipe.blobRingTail.bytesTailPos, newTailBytesToPublish);
        pipe.slabRingTail.tailPos.lazySet(newTailToPublish);
        pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
    }

    /**
     * Low level API for publish
     * @param pipe
     */
    public static <S extends MessageSchema<S>> int publishWrites(Pipe<S> pipe) {
    	notifyPubListener(pipe.pubListeners);
       	
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
    	//happens at the end of every fragment
        int consumed = writeTrailingCountOfBytesConsumed(pipe); //increment because this is the low-level API calling

		publishWritesBatched(pipe);

		
		return consumed;
    }

	public static <S extends MessageSchema<S>> int writeTrailingCountOfBytesConsumed(Pipe<S> pipe) {
		return writeTrailingCountOfBytesConsumed(pipe, pipe.slabRingHead.workingHeadPos.value++);
	}

    public static <S extends MessageSchema<S>> int publishWrites(Pipe<S> pipe, int optionalHiddenTrailingBytes) {
    	notifyPubListener(pipe.pubListeners);
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
    	//add a few extra bytes on the end of the blob so we can "hide" information between fragments.
    	assert(optionalHiddenTrailingBytes>=0) : "only zero or positive values supported";
		PaddedInt.maskedAdd(pipe.blobRingHead.byteWorkingHeadPos, optionalHiddenTrailingBytes, Pipe.BYTES_WRAP_MASK);
    	    	
    	//happens at the end of every fragment
        int consumed = writeTrailingCountOfBytesConsumed(pipe, pipe.slabRingHead.workingHeadPos.value++); //increment because this is the low-level API calling
        assert(consumed<pipe.maxVarLen) : "When hiding data it must stay below the max var length threshold when added to the rest of the fields.";        
        
		publishWritesBatched(pipe);
		return consumed;
    }

	public static <S extends MessageSchema<S>> void notifyPubListener(Pipe<S> pipe) {
		notifyPubListener(pipe.pubListeners);
	
	}

	private static void notifyPubListener(PipePublishListener[] listeners) {
		int i = listeners.length;	
    	while (--i>=0) {
    		listeners[i].published();
    	}
	}
    
    
    public static <S extends MessageSchema<S>> void publishWritesBatched(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
        //single length field still needs to move this value up, so this is always done
    	//in most cases this is redundant
		pipe.blobWriteLastConsumedPos = pipe.blobRingHead.byteWorkingHeadPos.value;

    	publishHeadPositions(pipe);
    }


    private static <S extends MessageSchema<S>> boolean validateFieldCount(Pipe<S> pipe) {
        if (null==Pipe.from(pipe)) {
        	return true;//skip check for this schemaless use
        }
    	long lastHead = Math.max(pipe.lastPublishedSlabRingHead,  Pipe.headPosition(pipe));    	
    	int len = (int)(Pipe.workingHeadPosition(pipe)-lastHead);    	
    	int[] fragDataSize = Pipe.from(pipe).fragDataSize;
        int i = fragDataSize.length;
        boolean found = false;
    	while (--i>=0) {
    	    found |= (len==fragDataSize[i]);
    	}    	
    	if (!found) {
    	    System.err.println("there is no fragment of size "+len+" check for missing fields. "+pipe.schema.getClass().getSimpleName()); 
    	}
        return found;
    }

    /**
     * Publish any writes that were held back due to batching.
     * @param pipe
     */
    public static <S extends MessageSchema<S>> void publishAllBatchedWrites(Pipe<S> pipe) {

    	if (pipe.lastPublishedSlabRingHead>pipe.slabRingHead.headPos.get()) {
    		PaddedInt.set(pipe.blobRingHead.bytesHeadPos,pipe.lastPublishedBlobRingHead);
    		pipe.slabRingHead.headPos.lazySet(pipe.lastPublishedSlabRingHead);
    	}

		assert(debugHeadAssignment(pipe));
		pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
    }


	private static <S extends MessageSchema<S>> boolean debugHeadAssignment(Pipe<S> pipe) {

		if (0!=(PipeConfig.SHOW_HEAD_PUBLISH&pipe.debugFlags) ) {
			new Exception("Debug stack for assignment of published head position"+pipe.slabRingHead.headPos.get()).printStackTrace();
		}
		return true;
	}


	public static <S extends MessageSchema<S>> void publishHeadPositions(Pipe<S> pipe) {

		assert(pipe.slabRingHead.workingHeadPos.value >= Pipe.headPosition(pipe));
    	assert(pipe.llWrite.llrConfirmedPosition<=Pipe.headPosition(pipe) || 
    		   pipe.slabRingHead.workingHeadPos.value<=pipe.llWrite.llrConfirmedPosition) :
    			   "Possible unsupported mix of high and low level API. NextHead>head and workingHead>nextHead "+pipe+" nextHead "+pipe.llWrite.llrConfirmedPosition+"\n"+
    		       "OR the XML field types may not match the accessor methods in use.";
    	//TODO: not sure this works with structs now..
    	//assert(validateFieldCount(pipe)) : "No fragment could be found with this field count, check for missing or extra fields.";

	    //TODO: need way to test if publish was called on an input ? 
    	//      may be much easier to detect missing publish. or extra release.
    	
	    if ((--pipe.batchPublishCountDown<=0)) {
	        PaddedInt.set(pipe.blobRingHead.bytesHeadPos, pipe.blobRingHead.byteWorkingHeadPos.value);
	        pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value);
	        assert(debugHeadAssignment(pipe));
	        pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
	    } else {
	        storeUnpublishedWrites(pipe);
	    }
	}

	public static <S extends MessageSchema<S>> void storeUnpublishedWrites(Pipe<S> pipe) {
		pipe.lastPublishedBlobRingHead = pipe.blobRingHead.byteWorkingHeadPos.value;
		pipe.lastPublishedSlabRingHead = pipe.slabRingHead.workingHeadPos.value;
	}


    public static <S extends MessageSchema<S>> void abandonWrites(Pipe<S> pipe) {
        //ignore the fact that any of this was written to the ring buffer
    	pipe.slabRingHead.workingHeadPos.value = pipe.slabRingHead.headPos.longValue();
    	pipe.blobRingHead.byteWorkingHeadPos.value = PaddedInt.get(pipe.blobRingHead.bytesHeadPos);
    	storeUnpublishedWrites(pipe);
    }

    /**
     * Only use this if you know what you are doing, eg we have enough threads.
     * This method is required because a normal spin lock is not aware of the shutdown
     * of a pipe and this condition must cause an interrupt while spinning.
     * @param pipe
     */
	public static <S extends MessageSchema<S>> void spinWork(Pipe<S> pipe) {
		Thread.yield();//needed for now but re-evaluate performance impact
		if (isShutdown(pipe) || Thread.currentThread().isInterrupted()) {
			Thread.currentThread().interrupt();
			throw null!=pipe.firstShutdownCaller ? pipe.firstShutdownCaller : new PipeException("Unexpected shutdown");
		}
	}

	public static <S extends MessageSchema<S>> int blobMask(Pipe<S> pipe) {
		return pipe.blobMask;
	}
	
	public static <S extends MessageSchema<S>> int slabMask(Pipe<S> pipe) {
	    return pipe.slabMask;
	}

	public static <S extends MessageSchema<S>> long getSlabHeadPosition(Pipe<S> pipe) {
		return headPosition(pipe);
	}
	
	public static <S extends MessageSchema<S>> long headPosition(Pipe<S> pipe) {
		 return pipe.slabRingHead.headPos.get();
	}

    public static <S extends MessageSchema<S>> long workingHeadPosition(Pipe<S> pipe) {
        return PaddedLong.get(pipe.slabRingHead.workingHeadPos);
    }

    public static <S extends MessageSchema<S>> void setWorkingHead(Pipe<S> pipe, long value) {
    	//assert(pipe.slabRingHead.workingHeadPos.value<=value) : "new working head must be forward";
        PaddedLong.set(pipe.slabRingHead.workingHeadPos, value);
    }

    public static <S extends MessageSchema<S>> long addAndGetWorkingHead(Pipe<S> pipe, int inc) {
        return PaddedLong.add(pipe.slabRingHead.workingHeadPos, inc);
    }

    public static <S extends MessageSchema<S>> long getWorkingTailPosition(Pipe<S> pipe) {
        return PaddedLong.get(pipe.slabRingTail.workingTailPos);
    }

    public static <S extends MessageSchema<S>> void setWorkingTailPosition(Pipe<S> pipe, long value) {
        PaddedLong.set(pipe.slabRingTail.workingTailPos, value);
    }

    public static <S extends MessageSchema<S>> long addAndGetWorkingTail(Pipe<S> pipe, int inc) {
        return PaddedLong.add(pipe.slabRingTail.workingTailPos, inc);
    }


    public static <S extends MessageSchema<S>> boolean tryReplication(Pipe<S> pipe, 
            final long historicSlabPosition, 
            final int historicBlobPosition) {

		assert(Pipe.singleThreadPerPipeWrite(pipe.id));
				
		final int idx = (int)historicSlabPosition & pipe.slabMask;		
		final int msgIdx = Pipe.slab(pipe)[idx]; //false share as this is a dirty read
		final int slabMsgSize = Pipe.sizeOf(pipe,msgIdx);
		
		assert(Pipe.from(pipe).isValidMsgIdx(Pipe.from(pipe), msgIdx)) : "bad value "+msgIdx+" for "+pipe.schema;
		
		//first part is to protect against dirty reading		
		if ((msgIdx<Pipe.from(pipe).fragDataSize.length) 
			&& Pipe.headPosition(pipe) == Pipe.workingHeadPosition(pipe)	
			&& Pipe.hasRoomForWrite(pipe, slabMsgSize)) {
			
			//get the sizes of ints and bytes
			int blobMsgSize = Pipe.slab(pipe)[(int)(historicSlabPosition+slabMsgSize-1) & pipe.slabMask];
			
			copyFragment(pipe, 
					slabMsgSize, blobMsgSize, 
					Pipe.blob(pipe), Pipe.slab(pipe), 
					Pipe.blobMask(pipe), Pipe.slabMask(pipe),
					historicBlobPosition, idx);
			
			
			
			//Appendables.appendHexArray(System.out.append("replicate slab: "), '[', slab, historicSlabPosition, pipe.slabMask, ']', slabMsgSize).append('\n');
			
			
			//logger.info("replicate data from old:{} {} new:{} {} ",
			//historicBlobPosition, historicSlabPosition,
			//(blobPos&Pipe.blobMask(pipe)), (slabPos&Pipe.slabMask(pipe)));
			
			
			return true;
		} else {
			return false;
		}
	}

	static <S extends MessageSchema<S>> void copyFragment(Pipe<S> targetPipe, 
			final int slabMsgSize, int blobMsgSize,
			byte[] sourceBlob, int[] sourceSlab, 
			int sourceBlobMask, int sourceSlabMask, 
			int sourceBlobPos,	int sourceSlabPos) {
		
		//copy all the bytes
		Pipe.copyBytesFromToRing(sourceBlob, sourceBlobPos, sourceBlobMask, 
				                 Pipe.blob(targetPipe), Pipe.getWorkingBlobHeadPosition(targetPipe), Pipe.blobMask(targetPipe), 
				                 blobMsgSize);			
		Pipe.addAndGetBytesWorkingHeadPosition(targetPipe, blobMsgSize);
		
		//copy all the ints
		Pipe.copyIntsFromToRing(sourceSlab, sourceSlabPos, sourceSlabMask, 
				                Pipe.slab(targetPipe), 
				                (int)Pipe.headPosition(targetPipe), Pipe.slabMask(targetPipe), 
				                slabMsgSize);	
		Pipe.addAndGetWorkingHead(targetPipe, slabMsgSize-1);//one less because trailing length is not part of the data fields.
		
		Pipe.confirmLowLevelWrite(targetPipe, slabMsgSize);
		Pipe.publishWrites(targetPipe);
		
	}
    
    
	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param pipe
	 * @param workingHeadPos
	 */
	public static <S extends MessageSchema<S>> void publishWorkingHeadPosition(Pipe<S> pipe, long workingHeadPos) {
		pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value = workingHeadPos);
	}

	public static <S extends MessageSchema<S>> long tailPosition(Pipe<S> pipe) {
		return pipe.slabRingTail.tailPos.get();
	}



	/**
	 * This method is only for build transfer stages that require direct manipulation of the position.
	 * Only call this if you really know what you are doing.
	 * @param pipe
	 * @param workingTailPos
	 */
	public static <S extends MessageSchema<S>> void publishWorkingTailPosition(Pipe<S> pipe, long workingTailPos) {
		pipe.slabRingTail.tailPos.lazySet(pipe.slabRingTail.workingTailPos.value = workingTailPos);
	}
    
	public static <S extends MessageSchema<S>> void publishBlobWorkingTailPosition(Pipe<S> pipe, int blobWorkingTailPos) {
        pipe.blobRingTail.bytesTailPos.value = (pipe.blobRingTail.byteWorkingTailPos.value = blobWorkingTailPos);
    }
	
	@Deprecated
	public static <S extends MessageSchema<S>> int primarySize(Pipe<S> pipe) {
		return pipe.sizeOfSlabRing;
	}

	public static <S extends MessageSchema<S>> FieldReferenceOffsetManager from(Pipe<S> pipe) {
		assert(pipe.schema!=null);	
		return pipe.schema.from;
	}

	public static <S extends MessageSchema<S>> int cursor(Pipe<S> pipe) {
        return pipe.ringWalker.cursor;
    }

	public static <S extends MessageSchema<S>> int writeTrailingCountOfBytesConsumed(Pipe<S> pipe, final long pos) {

		final int consumed = computeCountOfBytesConsumed(pipe);

		//used by both pipe and pipe writer so ideal place to count fragments
		pipe.totalWrittenFragments++;		
		
		pipe.slabRing[pipe.slabMask & (int)pos] = consumed;
		pipe.blobWriteLastConsumedPos = pipe.blobRingHead.byteWorkingHeadPos.value;
		return consumed;
	}

	public static <S extends MessageSchema<S>> int computeCountOfBytesConsumed(Pipe<S> pipe) {
		int consumed = pipe.blobRingHead.byteWorkingHeadPos.value - pipe.blobWriteLastConsumedPos;	
		
		if (consumed<0) {			
			consumed = (1+consumed)+Integer.MAX_VALUE;			
		}	
		assert(consumed>=0) : "consumed was "+consumed;
		//log.trace("wrote {} bytes consumed to position {}",consumed,pos);
		return consumed;
	}

	public static <S extends MessageSchema<S>> IntBuffer wrappedSlabRing(Pipe<S> pipe) {
		return pipe.wrappedSlabRing;
	}

	public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingA(Pipe<S> pipe) {
		return pipe.wrappedBlobReadingRingA;
	}

    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingB(Pipe<S> pipe) {
        return pipe.wrappedBlobReadingRingB;
    }

	public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobConstBuffer(Pipe<S> pipe) {
		return pipe.wrappedBlobConstBuffer;
	}

	
	public static <S extends MessageSchema<S>> DataOutputBlobWriter<S> outputStream(Pipe<S> pipe) {
		return pipe.blobWriter;
	}

	public static <S extends MessageSchema<S>> DataOutputBlobWriter<S> openOutputStream(Pipe<S> pipe) {
		return DataOutputBlobWriter.openField(pipe.blobWriter);
	}
	
	public static <S extends MessageSchema<S>> DataInputBlobReader<S> inputStream(Pipe<S> pipe) {
		return pipe.blobReader;
	}
	
	public static <S extends MessageSchema<S>> DataInputBlobReader<S> openInputStream(Pipe<S> pipe) {
		pipe.blobReader.openLowLevelAPIField();
		return pipe.blobReader;
	}
	
	public static <S extends MessageSchema<S>> DataInputBlobReader<S> peekInputStream(Pipe<S> pipe, int offset) {
		pipe.blobReader.peekLowLevelAPIField(offset);
		return pipe.blobReader;
	}
	
	
	/////////////
	//low level API
	////////////



	@Deprecated
	public static <S extends MessageSchema<S>> boolean roomToLowLevelWrite(Pipe<S> pipe, int size) {
		return hasRoomForWrite(pipe, size);
	}

	//This holds the last known state of the tail position, if its sufficiently far ahead it indicates that
	//we do not need to fetch it again and this reduces contention on the CAS with the reader.
	//This is an important performance feature of the low level API and should not be modified.
    public static <S extends MessageSchema<S>> boolean hasRoomForWrite(Pipe<S> pipe, int size) {
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
 
        long temp = pipe.llRead.llwConfirmedPosition+size;
		return roomToLowLevelWrite(pipe, temp);
    }
    
    public static <S extends MessageSchema<S>> void presumeRoomForWrite(Pipe<S> pipe) {
    	if (!hasRoomForWrite(pipe)) {
    		log.warn("Assumed available space but not found, make pipe larger or write less {}",pipe, new Exception());    		
    		while (!hasRoomForWrite(pipe)) {
    			spinWork(pipe);
    		}
    	}
    }

    /**
     * Checks the specified pipe to see if there is room to write
     * @param pipe pipe to examine
     * @param <S> MessageSchema to extend
     * @return <code>true</code> if pipe has room to write else <code>false</code>
     */
    public static <S extends MessageSchema<S>> boolean hasRoomForWrite(Pipe<S> pipe) {
        assert(null != pipe.slabRing) : "Pipe must be init before use";
        assert(null != pipe.llRead) : "Expected pipe to be setup for low level use.";
        assert(Pipe.singleThreadPerPipeWrite(pipe.id));
       
        long temp = pipe.llRead.llwConfirmedPosition+FieldReferenceOffsetManager.maxFragmentSize(Pipe.from(pipe));
		return roomToLowLevelWrite(pipe, temp);
    }    
    
	private static <S extends MessageSchema<S>> boolean roomToLowLevelWrite(Pipe<S> pipe, long target) {
		//only does second part if the first does not pass
		return (pipe.llRead.llrTailPosCache > target) || roomToLowLevelWriteSlow(pipe, target);
	}

	private static <S extends MessageSchema<S>> boolean roomToLowLevelWriteSlow(Pipe<S> pipe, long target) {
        return (pipe.llRead.llrTailPosCache = pipe.slabRingTail.tailPos.get()  ) >= target;
	}

	public static <S extends MessageSchema<S>> long confirmLowLevelWrite(Pipe<S> output, int size) { 
	 
		assert(Pipe.singleThreadPerPipeWrite(output.id));
	    assert(size>=0) : "unsupported size "+size;
	    
	    assert((output.llRead.llwConfirmedPosition+output.slabMask) <= Pipe.workingHeadPosition(output)) : " confirmed writes must be less than working head position writes:"
	                                                +(output.llRead.llwConfirmedPosition+output.slabMask)+" workingHead:"+Pipe.workingHeadPosition(output)+
	                                                " \n CHECK that Pipe is written same fields as message defines and skips none!";
	   
	    assert(verifySize(output, size));
	    
	    return  output.llRead.llwConfirmedPosition += size;

	}

	/**
	 * Method used for moving more than one fragment at a time. Any size value will be acceptable
	 */
	public static <S extends MessageSchema<S>> long confirmLowLevelWriteUnchecked(Pipe<S> output, int size) { 
		 
		assert(Pipe.singleThreadPerPipeWrite(output.id));
	    assert(size>=0) : "unsupported size "+size;
	    
	    assert((output.llRead.llwConfirmedPosition+output.slabMask) <= Pipe.workingHeadPosition(output)) : " confirmed writes must be less than working head position writes:"
	                                                +(output.llRead.llwConfirmedPosition+output.slabMask)+" workingHead:"+Pipe.workingHeadPosition(output)+
	                                                " \n CHECK that Pipe is written same fields as message defines and skips none!";
	   	    
	    return  output.llRead.llwConfirmedPosition += size;

	}
	
	private static <S extends MessageSchema<S>> boolean verifySize(Pipe<S> output, int size) {
		
//		//TODO: not sure, this may not work with structured fields
//		try {
//			assert(Pipe.sizeOf(output, output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition]) == size) : 
//				"Did not write the same size fragment as expected, double check message. expected:"
//					+Pipe.sizeOf(output, output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition])
//					+" but was passed "+size+" for schema "+Pipe.schemaName(output)
//					+" and assumed MsgId of "+output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition];
//		} catch (ArrayIndexOutOfBoundsException aiex) {
//			//ignore, caused by some poor unit tests which need to be re-written.
//		}
		return true;
	}

	//helper method always uses the right size but that value needs to be found so its a bit slower than if you already knew the size and passed it in
	public static <S extends MessageSchema<S>> long confirmLowLevelWrite(Pipe<S> output) { 
		 
		assert(Pipe.singleThreadPerPipeWrite(output.id));
	    assert((output.llRead.llwConfirmedPosition+output.slabMask) <= Pipe.workingHeadPosition(output)) : " confirmed writes must be less than working head position writes:"
	                                                +(output.llRead.llwConfirmedPosition+output.slabMask)+" workingHead:"+Pipe.workingHeadPosition(output)+
	                                                " \n CHECK that Pipe is written same fields as message defines and skips none!";
	   
	    return  output.llRead.llwConfirmedPosition += Pipe.sizeOf(output, output.slabRing[lastConfirmedWritePosition(output)]);

	}

	public static <S extends MessageSchema<S>> int lastConfirmedWritePosition(Pipe<S> output) {
		return output.slabMask&(int)output.llRead.llwConfirmedPosition;
	}


	//do not use with high level API, is dependent on low level confirm calls.
	public static <S extends MessageSchema<S>> boolean hasContentToRead(Pipe<S> pipe, int size) {
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
        //optimized for the other method without size. this is why the -1 is there and we use > for target comparison.
        return contentToLowLevelRead2(pipe, pipe.llWrite.llrConfirmedPosition+size-1, pipe.llWrite); 
    }

    /**
     * Checks specified pipe to see if there is any data to read
     * @param pipe pipe to be examined
     * @param <S> MessageSchema to extend
     * @return <code>true</code> if there is data to read else <code>false</code>
     */
	//this method can only be used with low level api navigation loop
	//CAUTION: THIS IS NOT COMPATIBLE WITH PipeReader behavior...
    public static <S extends MessageSchema<S>> boolean hasContentToRead(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        assert(null != pipe.slabRing) : "Pipe must be init before use";
        return contentToLowLevelRead2(pipe, pipe.llWrite.llrConfirmedPosition, pipe.llWrite);

    }

	private static <S extends MessageSchema<S>> boolean contentToLowLevelRead2(Pipe<S> pipe, long target, LowLevelAPIWritePositionCache llWrite) {
		//only does second part if the first does not pass
		return (llWrite.llwHeadPosCache > target) || contentToLowLevelReadSlow(pipe, target, llWrite);
	}

	private static <S extends MessageSchema<S>> boolean contentToLowLevelReadSlow(Pipe<S> pipe, long target, LowLevelAPIWritePositionCache llWrite) {
		return (llWrite.llwHeadPosCache = pipe.slabRingHead.headPos.get()) > target;  
	}

	public static <S extends MessageSchema<S>> long confirmLowLevelRead(Pipe<S> pipe, long size) {
	    assert(size>0) : "Must have read something.";
	    assert(Pipe.singleThreadPerPipeRead(pipe.id));
	     //not sure if this assert is true in all cases
	  //  assert(input.llWrite.llwConfirmedWrittenPosition + size <= input.slabRingHead.workingHeadPos.value+Pipe.EOF_SIZE) : "size was far too large, past known data";
	  //  assert(input.llWrite.llwConfirmedWrittenPosition + size >= input.slabRingTail.tailPos.get()) : "size was too small, under known data";   
		return (pipe.llWrite.llrConfirmedPosition += size);
	}

    public static <S extends MessageSchema<S>> void setWorkingHeadTarget(Pipe<S> pipe) {
        pipe.llWrite.llrConfirmedPosition =  Pipe.getWorkingTailPosition(pipe);
    }

    public static <S extends MessageSchema<S>> int getBlobTailPosition(Pipe<S> pipe) {
	    return PaddedInt.get(pipe.blobRingTail.bytesTailPos);
	}

    @Deprecated
	public static <S extends MessageSchema<S>> int getBlobRingTailPosition(Pipe<S> pipe) {
	    return getBlobTailPosition(pipe);
	}

	public static <S extends MessageSchema<S>> void setBytesTail(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingTail.bytesTailPos, value);
    }

    public static <S extends MessageSchema<S>> int getBlobHeadPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingHead.bytesHeadPos);        
    }
	
    @Deprecated
    public static <S extends MessageSchema<S>> int getBlobRingHeadPosition(Pipe<S> pipe) {
        return getBlobHeadPosition(pipe); 
    }

    public static <S extends MessageSchema<S>> void setBytesHead(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingHead.bytesHeadPos, value);
    }

    public static <S extends MessageSchema<S>> int addAndGetBytesHead(Pipe<S> pipe, int inc) {
        return PaddedInt.add(pipe.blobRingHead.bytesHeadPos, inc);
    }

    public static <S extends MessageSchema<S>> int getWorkingBlobRingTailPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingTail.byteWorkingTailPos);
    }

   public static <S extends MessageSchema<S>> int addAndGetBytesWorkingTailPosition(Pipe<S> pipe, int inc) {
        return PaddedInt.maskedAdd(pipe.blobRingTail.byteWorkingTailPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    public static <S extends MessageSchema<S>> void setBytesWorkingTail(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingTail.byteWorkingTailPos, value);
    }
    
    public static <S extends MessageSchema<S>> int getWorkingBlobHeadPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingHead.byteWorkingHeadPos);
    }

    @Deprecated
    public static <S extends MessageSchema<S>> int getBlobWorkingHeadPosition(Pipe<S> pipe) {
        return getWorkingBlobHeadPosition(pipe);
    }
    
    public static <S extends MessageSchema<S>> int addAndGetBytesWorkingHeadPosition(Pipe<S> pipe, int inc) {
    	assert(inc>=0) : "only zero or positive values supported found "+inc;
        return PaddedInt.maskedAdd(pipe.blobRingHead.byteWorkingHeadPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    public static <S extends MessageSchema<S>> void setBytesWorkingHead(Pipe<S> pipe, int value) {
    	assert(value>=0) : "working head must be positive";
        PaddedInt.set(pipe.blobRingHead.byteWorkingHeadPos, value);
    }

    public static <S extends MessageSchema<S>> int decBatchRelease(Pipe<S> pipe) {
        return --pipe.batchReleaseCountDown;
    }

    public static <S extends MessageSchema<S>> int decBatchPublish(Pipe<S> pipe) {
        return --pipe.batchPublishCountDown;
    }

    public static <S extends MessageSchema<S>> void beginNewReleaseBatch(Pipe<S> pipe) {
        pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
    }

    public static <S extends MessageSchema<S>> void beginNewPublishBatch(Pipe<S> pipe) {
        pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
    }

    public static <S extends MessageSchema<S>> byte[] blob(Pipe<S> pipe) {        
        return pipe.blobRing;
    }
    
    public static <S extends MessageSchema<S>> int[] slab(Pipe<S> pipe) {
        return pipe.slabRing;
    }
    
    @Deprecated
    public static <S extends MessageSchema<S>> byte[] byteBuffer(Pipe<S> pipe) {        
        return blob(pipe);
    }

    @Deprecated
    public static <S extends MessageSchema<S>> int[] primaryBuffer(Pipe<S> pipe) {
        return slab(pipe);
    }

    public static <S extends MessageSchema<S>> void updateBytesWriteLastConsumedPos(Pipe<S> pipe) {
        pipe.blobWriteLastConsumedPos = Pipe.getWorkingBlobHeadPosition(pipe);
    }

    public static <S extends MessageSchema<S>> PaddedLong getWorkingTailPositionObject(Pipe<S> pipe) {
        return pipe.slabRingTail.workingTailPos;
    }

    public static <S extends MessageSchema<S>> PaddedLong getWorkingHeadPositionObject(Pipe<S> pipe) {
        return pipe.slabRingHead.workingHeadPos;
    }

    public static <S extends MessageSchema<S>> int sizeOf(Pipe<S> pipe, int msgIdx) {
    	return sizeOf(pipe.schema, msgIdx);
    }
    
    public static <S extends MessageSchema<S>> int sizeOf(S schema, int msgIdx) {
        return msgIdx>=0? schema.from.fragDataSize[msgIdx] : Pipe.EOF_SIZE;
    }

    public static <S extends MessageSchema<S>> void releasePendingReadLock(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        PendingReleaseData.releasePendingReadRelease(pipe.pendingReleases, pipe);
    }
    
    public static <S extends MessageSchema<S>> void releasePendingAsReadLock(Pipe<S> pipe, int consumed) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
    	PendingReleaseData.releasePendingAsReadRelease(pipe.pendingReleases, pipe, consumed);
    }
    
    public static <S extends MessageSchema<S>> int releasePendingCount(Pipe<S> pipe) {
    	return PendingReleaseData.pendingReleaseCount(pipe.pendingReleases);
    }
    
    public static <S extends MessageSchema<S>> int releasePendingByteCount(Pipe<S> pipe) {
     	return PendingReleaseData.pendingReleaseByteCount(pipe.pendingReleases);
    }
    
    public static <S extends MessageSchema<S>> void releaseAllPendingReadLock(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        PendingReleaseData.releaseAllPendingReadRelease(pipe.pendingReleases, pipe);
    }

    
    /**
     * Hold this position in case we want to abandon what is written
     * @param pipe pipe to be used
     */
    public static void markHead(Pipe pipe) {
        pipe.markedHeadSlab = Pipe.workingHeadPosition(pipe);
        pipe.markedHeadBlob = Pipe.getWorkingBlobHeadPosition(pipe);
    }
    
    /**
     * abandon what has been written in this fragment back to the markHead position.
     * @param pipe pipe to be used
     */
    public static void resetHead(Pipe pipe) {
        Pipe.setWorkingHead(pipe, pipe.markedHeadSlab);
        Pipe.setBytesWorkingHead(pipe, pipe.markedHeadBlob);
    }
    
    /**
     * Hold this position in case we want to re-read this single message
     * @param pipe
     */
    public static void markTail(Pipe pipe) {
        pipe.markedTailSlab = Pipe.getWorkingTailPosition(pipe);
        pipe.markedTailBlob = Pipe.getWorkingBlobRingTailPosition(pipe);
    }
    
    /**
     * abandon what has been read and move back to top of fragment to read again.
     * MUST be called before confirm of read and never after
     * @param pipe
     */
    public static void resetTail(Pipe pipe) {
        Pipe.setWorkingTailPosition(pipe, pipe.markedTailSlab);
        Pipe.setBytesWorkingTail(pipe, pipe.markedTailBlob);
    }
    
    
	public static int storeBlobWorkingHeadPosition(Pipe<?> target) {
		assert(-1 == target.activeBlobHead) : "can not store second until first is resolved";
		return target.activeBlobHead = Pipe.getWorkingBlobHeadPosition(target);				
	}
    
	public static int unstoreBlobWorkingHeadPosition(Pipe<?> target) {
		assert(-1 != target.activeBlobHead) : "can not unstore value not saved";
		int result = target.activeBlobHead;
		target.activeBlobHead = -1;
		return result;
	}



}
