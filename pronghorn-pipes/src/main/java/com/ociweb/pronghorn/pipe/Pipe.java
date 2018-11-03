package com.ociweb.pronghorn.pipe;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.PaddedAtomicLong;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ma.RunningStdDev;

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

    public static int showPipesCreatedLargerThan = -1;

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


    /**
     * Store the StructRegistry and associate it with this pipe
     * @param p pipe holding registry
     * @param recordTypeData registry to be stored
     */
	public static void structRegistry(Pipe p, StructRegistry recordTypeData) {
    	assert(null!=recordTypeData) : "must not be null";
    	assert(null==p.typeData || recordTypeData==p.typeData) :
    		"can not modify type data after setting";
    	p.typeData = recordTypeData;
    }
    
	/**
	 * Returns the struct registry held by this pipe
	 * @param p source pipe holding the registry
	 * @return the registry held by the pipe
	 */
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
	private long lastFragmentCount = 0;
	private RunningStdDev fragsPerPass = new RunningStdDev();
	

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
	
	/**
	 * Holds the thread caller for internal bookkeeping. This allows the code
	 * to assert that only a single thread is involved in the calling.
	 * @param callerLookup object holding the coller id.
	 */
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
					assert(callerId == expected) : 
						"Check your graph construction and stage constructors.\n Pipe "
							+pipeId+" must only have 1 stage therefore 1 thread reading from it. CallerId:"+callerId+" Exepected:"+expected;
				}
			}
		}
		return true;
	}
	
	/////////////////
	/////////////////
	
	/**
	 * Returns the total count of all fragments written to this pipe since creation.
	 * @param p pipe with count
	 * @return count of total fragments written
	 */
	public static long totalWrittenFragments(Pipe<?> p) {
		return p.totalWrittenFragments;
	}
    
	public static RunningStdDev totalWrittenFragmentStdDevperPass(Pipe<?> p) {
		return p.fragsPerPass;
	}
	
	/**
	 * Called to accumulate total fragments written to this pipe
	 * @param p pipe to accumulate
	 * @param sum total count to be added for this call
	 */
	public static void sumWrittenFragments(Pipe<?> p, long sum) {
		p.totalWrittenFragments+=sum;
	}
	
	////////////////////
	////////////////////
	/**
	 * Build new Pipe instance.
	 * @param config reusable PipeConfig for creating many identical pipes.
	 */
	public Pipe(PipeConfig<T> config) {
		this(config,true);
	}
    	
	/**
	 * Build new Pipe instance. Can save some object construction and memory
	 * if the high level API is not used.
	 * @param config reusable PipeConfig for creating many identical pipes.
	 * @param usingHighLevelAPI boolean used to turn off high level if its not used.
	 */
    public Pipe(PipeConfig<T> config, boolean usingHighLevelAPI) {
    	this.config = config;
    	this.usingHighLevelAPI = usingHighLevelAPI;
        byte primaryBits = config.slabBits;
        byte byteBits = config.blobBits;
        byte[] byteConstants = config.byteConst;
        this.schema = config.schema;

        assert(holdConstructionLocation());
        
        debugFlags = config.debugFlags;
                
        if ((showPipesCreatedLargerThan>0) &&	(config.totalBytesAllocated() >= showPipesCreatedLargerThan) ) {
        	if (config.totalBytesAllocated() < (1<<11)) {
        		new Exception("large pipe "+(config.totalBytesAllocated())+" B").printStackTrace();
        	} else {
        		if (config.totalBytesAllocated() < (1<<21)) {
        			new Exception("large pipe "+(config.totalBytesAllocated()>>10)+" KB").printStackTrace();
        		} else {
        			new Exception("large pipe "+(config.totalBytesAllocated()>>20)+" MB").printStackTrace();
        		}
        	}
        }
        

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
		createdStack = new Exception(config+" Pipe created "+config.totalBytesAllocated()+" bytes");
    	return true;
	}
    
    /**
     * Prints the call stack as it was when this pipe was created.
     * This is helpful for debugging as to where this pipe was created.
     */
    public void creationStack() {
    	if (null!=createdStack) {
   
    		createdStack.printStackTrace();
    	}
    }
    

	private AtomicBoolean isInBlobFieldWrite = new AtomicBoolean(false);

    //this is used along with tail position to capture the byte count
	private long totalBlobBytesRead=0;
    
	/**
	 * Bookkeeping method to track if a write is in progress to the blob.
	 * @param pipe writing
	 * @return boolean true if the blob is in the process of  a write.
	 */
    public static <S extends MessageSchema<S>> boolean isInBlobFieldWrite(Pipe<S> pipe) {
        return pipe.isInBlobFieldWrite.get();
    }
    
    /**
     * total bytes consumed by this fragment
     * @return count of bytes consumed
     */
    public long totalBlobBytesRead() {
    	return totalBlobBytesRead;
    }
    
    /**
     * Prep this pipe for writing a blob var len field.
     * Set the internal state for checking if the blob write is in progress.
     */
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

    /**
     * Mark this pipe as finished writing blob var len field.
     */
	public void closeBlobFieldWrite() {
		blobOpenStack = null;
    	//System.out.println("close stream on "+id);
        if (!isInBlobFieldWrite.compareAndSet(true, false)) {
            throw new UnsupportedOperationException("can not close blob if not open.");
        }
    }

	/**
	 * Check if the rate is limited from the consumer side of the pipe.
	 * @param pipe which is rate limited
	 * @return boolean true if consumer side is rate limited
	 */
    public static <S extends MessageSchema<S>> boolean isRateLimitedConsumer(Pipe<S> pipe) {
        return null!=pipe.regulatorConsumer;
    }
    
    /**
	 * Check if the rate is limited from the producer side of the pipe.
	 * @param pipe which is rate limited
	 * @return boolean true if producer side is rate limited
     */
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
    
    /**
     * Checks if schema matches that used by the pipe.
     * @param pipe to check schema
     * @param schema MessageSchema instance type
     * @return boolean true if the schema matches
     */
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSchema(Pipe<S> pipe, T schema) {
        return pipe.schema == schema;
    }
    
    /**
     * Checks if schema matches that used by the pipe.
     * @param pipe to check schema
     * @param schema MessageSchema class
     * @return boolean true if the schema matches
     */
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSchema(Pipe<S> pipe, Class<T> schema) {
        return schema.isInstance(pipe.schema);
    }
    
    /**
     * Checks if both pipes use the same schemas.
     * @param pipeA pipe with a schema S
     * @param pipeB pipe with a schema T
     * @return boolean true if the schema matches
     */
    public static <S extends MessageSchema<S>, T extends MessageSchema<T>> boolean isForSameSchema(Pipe<S> pipeA, Pipe<T> pipeB) {
        return pipeA.schema == pipeB.schema;
    }
    
    /**
     * Checks if pipe uses a dynamic schema which is not backed by an XML contract file.
     * @param pipe to be checked
     * @return boolean true if the schema is dynamic
     */
    public static <S extends MessageSchema<S>> boolean isForDynamicSchema(Pipe<S> pipe) {
        return pipe.schema instanceof MessageSchemaDynamic;
    }
    
    /**
     * Estimate of the total bytes consumed by this pipe
     * @param pipe to be checked
     * @return long bytes count estimate memory consumed
     */
    public static <S extends MessageSchema<S>> long estBytesAllocated(Pipe<S> pipe) {
    	if (null!=pipe && pipe.blobRing!=null && pipe.slabRing!=null) {
    		return ((long)pipe.blobRing.length) + (pipe.slabRing.length*4L) + 1024L;//1K for overhead
    	} else {
    		return 0;
    	}
    }
    
    /**
     * get name of the schema.
     * @param pipe to be checked
     * @return String name of the schema
     */
    public static <S extends MessageSchema<S>> String schemaName(Pipe<S> pipe) {
        return (null==pipe.schema ?
        		   "NoSchemaFor "+Pipe.from(pipe).name  :
           	       pipe.schema.getClass().getSimpleName());
    }
   
    
    /**
     * Back up the cursor read position. So the fragments can be read again for those not yet released.
     * 
     * @param pipe with unreleased fragments to be replayed.
     */
	public static <S extends MessageSchema<S>> void replayUnReleased(Pipe<S> pipe) {

//We must enforce this but we have a few unit tests that are in violation which need to be fixed first
//	    if (!RingBuffer.from(ringBuffer).hasSimpleMessagesOnly) {
//	        throw new UnsupportedOperationException("replay of unreleased messages is not supported unless every message is also a single fragment.");
//	    }

		if (!isReplaying(pipe)) {
			//save all working values only once if we re-enter replaying multiple times.

		    pipe.holdingSlabWorkingTail = Pipe.getWorkingTailPosition(pipe);
			pipe.holdingBlobWorkingTail = Pipe.getWorkingBlobTailPosition(pipe);

			//NOTE: we must never adjust the ringWalker.nextWorkingHead because this is replay and must not modify write position!
			pipe.ringWalker.holdingNextWorkingTail = pipe.ringWalker.nextWorkingTail;

			pipe.holdingBlobReadBase = pipe.blobReadBase;

		}

		//clears the stack and cursor position back to -1 so we assume that the next read will begin a new message
		StackStateWalker.resetCursorState(pipe.ringWalker);

		//set new position values for high and low api
		pipe.ringWalker.nextWorkingTail = pipe.slabRingTail.rollBackWorking();
		pipe.blobReadBase = pipe.blobRingTail.rollBackWorking(); //this byte position is used by both high and low api
	}

	/**
	 * Checks to see if the provided pipe is replaying.
	 * @param pipe the ringBuffer to check.
	 * @return <code>true</code> if the ringBuffer is replaying, <code>false</code> if it is not.
	 */
	public static <S extends MessageSchema<S>> boolean isReplaying(Pipe<S> pipe) {
		return Pipe.getWorkingTailPosition(pipe)<pipe.holdingSlabWorkingTail;
	}

    /**
     * Cancels replay of specified ringBuffer
     * @param pipe ringBuffer to cancel replay
     */
	public static <S extends MessageSchema<S>> void cancelReplay(Pipe<S> pipe) {
		pipe.slabRingTail.workingTailPos.value = pipe.holdingSlabWorkingTail;
		pipe.blobRingTail.byteWorkingTailPos.value = pipe.holdingBlobWorkingTail;

		pipe.blobReadBase = pipe.holdingBlobReadBase;

		pipe.ringWalker.nextWorkingTail = pipe.ringWalker.holdingNextWorkingTail;
		//NOTE while replay is in effect the head can be moved by the other (writing) thread.
	}

	private static final int INDEX_BASE_OFFSET = 4; //Room for 1 int (struct info)
	
	///////////
	//support for adding indexes onto the end of the var len blob field
	///////////
	/**
	 * Base byte position where the index begins.
	 * @param pipe where the index is written.
	 * @return int position of index
	 */
	public static <S extends MessageSchema<S>> int blobIndexBasePosition(Pipe<S> pipe) {
		if (pipe.maxVarLen<INDEX_BASE_OFFSET) {
			throw new UnsupportedOperationException("no var length for index");
		}		
		return pipe.maxVarLen-INDEX_BASE_OFFSET;
	}
	
	
	/**
	 * sets maximum batch count for releases.
	 * @param pipe to have releases batched
	 */
	public static <S extends MessageSchema<S>> void batchAllReleases(Pipe<S> pipe) {
	   pipe.batchReleaseCountDownInit = Integer.MAX_VALUE;
	   pipe.batchReleaseCountDown = Integer.MAX_VALUE;
	}


	/**
	 * sets specific batch release size, releases do not occur when called for but only when the
	 * size count of releases is reached.
	 * @param pipe to have releases batched
	 * @param size count of fragments to batch before release.
	 */
    public static <S extends MessageSchema<S>> void setReleaseBatchSize(Pipe<S> pipe, int size) {

    	validateBatchSize(pipe, size);

    	pipe.batchReleaseCountDownInit = size;
    	pipe.batchReleaseCountDown = size;
    }

    /**
     * sets specific batch publish size, publishes do not occur when called for but only when the
     * size count of publishes is reached.
     * @param pipe to have publishes batched
     * @param size count of fragments to batch befoer publish
     */
    public static <S extends MessageSchema<S>> void setPublishBatchSize(Pipe<S> pipe, int size) {

    	validateBatchSize(pipe, size);

    	pipe.batchPublishCountDownInit = size;
    	pipe.batchPublishCountDown = size;
    }
    
    /**
     * get the publish batch count
     * @param pipe with batched published count
     * @return count of fragments to be batched
     */
    public static <S extends MessageSchema<S>> int getPublishBatchSize(Pipe<S> pipe) {
        return pipe.batchPublishCountDownInit;
    }
    
    /**
     * get the release batch count
     * @param pipe with batched release count
     * @return count of fragments to be batched
     */
    public static <S extends MessageSchema<S>> int getReleaseBatchSize(Pipe<S> pipe) {
        return pipe.batchReleaseCountDownInit;
    }

    /**
     * Sets the publish batch size to the max supported by the pipe.
     * @param pipe to have publishes batched.
     */
    public static <S extends MessageSchema<S>> void setMaxPublishBatchSize(Pipe<S> pipe) {
    	int size = computeMaxBatchSize(pipe, 3);
    	pipe.batchPublishCountDownInit = size;
    	pipe.batchPublishCountDown = size;

    }

    /**
     * Sets the release batch size to the max supported by the pipe.
     * @param pipe
     */
    public static <S extends MessageSchema<S>> void setMaxReleaseBatchSize(Pipe<S> pipe) {

    	int size = computeMaxBatchSize(pipe, 3);
    	pipe.batchReleaseCountDownInit = size;
    	pipe.batchReleaseCountDown = size;

    }

    /**
     * base position to track where the var length
     * @param pipe holding bytes written
     * @return write base
     */
    public static <S extends MessageSchema<S>> int bytesWriteBase(Pipe<S> pipe) {
    	return pipe.blobWriteBase;
    }

    /**
     * Store new var len base written position.
     * @param pipe to have base marked
     */
    public static <S extends MessageSchema<S>> void markBytesWriteBase(Pipe<S> pipe) {
    	pipe.blobWriteBase = pipe.blobRingHead.byteWorkingHeadPos.value;
    }

    /**
     * get base position based on the var length data written. 
     * @param pipe to read base from
     * @return int base position
     */
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

    /**
     * store bytes consumed for fragment bookkeeping
     * @param pipe reading from
     * @param bytesConsumed int total for this read
     */
	public static <S extends MessageSchema<S>> void markBytesReadBase(Pipe<S> pipe, int bytesConsumed) {
        assert(bytesConsumed>=0) : "Bytes consumed must be positive";
        //base has future pos added to it so this value must be masked and kept as small as possible
        pipe.totalBlobBytesRead = pipe.totalBlobBytesRead+bytesConsumed;        
        pipe.blobReadBase = pipe.blobMask /*Pipe.BYTES_WRAP_MASK*/ & (pipe.blobReadBase+bytesConsumed);
        //no longer true now that we have index data...
        //assert(validateInsideData(pipe, pipe.blobReadBase)) : "consumed "+bytesConsumed+" bytes using mask "+pipe.blobMask+" new base is "+pipe.blobReadBase;
    }
    
    /**
     * auto compute and store bytes consumed for fragment bookkeeping
     * @param pipe reading from
     */
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
    
    /**
     * Helpful user readable summary of the pipe.
     * Shows where the head and tail positions are along with how full the ring is at the time of call.
     */
    public String toString() {

        int contentRem = Pipe.contentRemaining(this);
        
        if (contentRem > sizeOfSlabRing) {        	
        	log.warn("ERROR: can not have more slab content than the size of the pipe. content {} vs {}",contentRem,sizeOfSlabRing);
        }
        
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
     * Return the configuration used for this pipe, Helpful when we need to make clones of the ring which will hold same message types.
     */
    public PipeConfig<T> config() {
    	return config;
    }

    /**
     * Total count of all pipes in the system. As each pipe is created this counter is incremented.
     * @return total count of pipes in the application.
     */
    public static <S extends MessageSchema> int totalPipes() {
        return pipeCounter.get();
    }

    /**
     * Allocates all internal arrays and buffers before the Pipe can be used
     * @return Pipe initialized
     */
	public Pipe<T> initBuffers() {
		assert(!isInit(this)) : "RingBuffer was already initialized";
		if (!isInit(this)) {
			buildBuffers();
		} else {
			log.warn("Init was already called once already on this ring buffer");
		}
		return this;
    }
    
	/**
	 * Set the regulator value for the consumer of the pipe.
	 * @param pipe to be regulated
	 * @param msgPerMs messages per ms
	 * @param msgSize size of messages regulated
	 */
    public static <S extends MessageSchema<S>> void setConsumerRegulation(Pipe<S> pipe, int msgPerMs, int msgSize) {
        assert(null==pipe.regulatorConsumer) : "regulator must only be set once";
        assert(!isInit(pipe)) : "regular may only be set before scheduler has initialized the pipe";
        pipe.regulatorConsumer = new PipeRegulator(msgPerMs, msgSize);
    }
  
    /**
     * Set the regulator value for the producer of the pipe.
     * @param pipe to be regulated
     * @param msgPerMs messages per ms
     * @param msgSize size of messages regulated
     */
    public static <S extends MessageSchema<S>> void setProducerRegulation(Pipe<S> pipe, int msgPerMs, int msgSize) {
        assert(null==pipe.regulatorProducer) : "regulator must only be set once";
        assert(!isInit(pipe)) : "regular may only be set before scheduler has initialized the pipe";
        pipe.regulatorProducer = new PipeRegulator(msgPerMs, msgSize);
    } 
    
    //private final static AtomicLong totalBytes = new AtomicLong();
    
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
	        
//	        int pipeBytesTotal = sizeOfBlobRing + (4*sizeOfSlabRing);
//			int delta = pipeBytesTotal;
//	        
//	        String unit = "b";
//	        if (delta > (1<<20)) {
//	        	delta = delta>>20;
//	    	    unit = "mb";
//	        }
//	        
//			long totalMemory = totalBytes.addAndGet(delta);
//			
//			if (pipeBytesTotal > (1<<23)) {
//		        if (totalMemory > (1<<20)) {
//		        	System.out.println("total consumed "+(totalMemory>>20)+"mb  "+delta+unit+"  "+this.config()+"  "+this.config().totalBytesAllocated());
//		        } else {
//		        	System.out.println("total consumed "+totalMemory+"b  "+delta+unit+"  "+this.config()+"  "+this.config().totalBytesAllocated());
//		        }
//			}
	        
	        
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
	
    /**
     * is the pipe initialized and ready for use.
     * @param pipe
     * @return boolean true if initialized
     */
	public static <S extends MessageSchema<S>> boolean isInit(Pipe<S> pipe) {
	    //Due to the fact that no locks are used it becomes necessary to check
	    //every single field to ensure the full initialization of the object
	    //this is done as part of graph set up and as such is called rarely.
		return null!=pipe.blobRing &&
			   null!=pipe.slabRing &&
			   null!=pipe.blobRingLookup &&
			   null!=pipe.wrappedSlabRing &&
			   null!=pipe.llRead &&
			   null!=pipe.llWrite &&
			   (
			    pipe.sizeOfBlobRing == 0 || //no init of these if the blob is not used
			    (null!=pipe.wrappedBlobReadingRingA &&
		         null!=pipe.wrappedBlobReadingRingB &&
			     null!=pipe.wrappedBlobWritingRingA &&
			     null!=pipe.wrappedBlobWritingRingB
			     )
			   );
		      //blobReader and blobWriter and not checked since they call isInit on construction
	}

	/**
	 * Confirm that the bytes written are less than the space available.
	 * @param pipe to be checked.
	 * @param length written in bytes
	 * @return boolean true if the bytes written are less than the available length.
	 */
	public static <S extends MessageSchema<S>> boolean validateVarLength(Pipe<S> pipe, int length) {
		assert(length>=-1) : "invalid length value "+length;
		int newAvg = (length+pipe.varLenMovingAverage)>>1;
        if (newAvg>pipe.maxVarLen)	{
            //compute some helpful information to add to the exception
        	int bytesPerInt = (int)Math.ceil(length*Pipe.from(pipe).maxVarFieldPerUnit);
        	int bitsDif = 32 - Integer.numberOfLeadingZeros(bytesPerInt - 1);
        	Pipe.shutdown(pipe);
        	pipe.creationStack();
        	
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

        assert(Pipe.contentRemaining(this)<=sizeOfSlabRing);
        
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

    /**
     * Publish all the batched up publish calls.
     * @param pipe to have publications released.
     */
    public static void releaseReadsBatched(Pipe<MessageSchemaDynamic> pipe) {
		Pipe.batchedReleasePublish(pipe, Pipe.getWorkingBlobTailPosition(pipe),
	    		                      Pipe.getWorkingTailPosition(pipe));
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
			Pipe<S> sourcePipe, final long sourceSlabPos, int sourceBlobPos,
			Pipe<S> localTarget) {
		
		final int mask = Pipe.slabMask(sourcePipe);
		final int[] slab = Pipe.slab(sourcePipe);
		
		final int msgIdx = slab[mask&(int)sourceSlabPos];
		
		//look up the data size to copy...
		final int slabMsgSize = msgIdx!=-1 ? Pipe.from(sourcePipe).fragDataSize[msgIdx] : Pipe.EOF_SIZE;

		//this value also contains the full byte count for any index used by structures
		int blobMsgSize = slab[mask&((int)(sourceSlabPos+slabMsgSize-1))]; //min one for pos of byte count
			
		Pipe.copyFragment(localTarget,
				slabMsgSize, blobMsgSize, 
				Pipe.blob(sourcePipe), Pipe.slab(sourcePipe), 
				Pipe.blobMask(sourcePipe), Pipe.slabMask(sourcePipe), 
				sourceBlobPos, (int)sourceSlabPos);
		
		//move source pointers forward
		Pipe.addAndGetWorkingTail(sourcePipe, slabMsgSize-1);
		Pipe.addAndGetBlobWorkingTailPosition(sourcePipe, blobMsgSize);
		Pipe.confirmLowLevelRead(sourcePipe, slabMsgSize);
		Pipe.releaseReadLock(sourcePipe);		
				
		return slabMsgSize;
		
	}

	/**
	 * Build an array of Pipe instances from a set of PipeConfig.
	 * @param configs array of PipeConfig values
	 * @return array of new pipes
	 */
	public static Pipe[] buildPipes(PipeConfig[] configs) {
		int i = configs.length;
		Pipe[] result = new Pipe[i];
		while (--i>=0) {
			result[i] = new Pipe(configs[i]);
		}		
		return result;
	}


	/**
	 * Build an array of Pipe instances from a set of PipeConfig found in array of pipes.
	 * @param configs array of PipeConfig values
	 * @return array of new pipes
	 */
	public static <S extends MessageSchema<S>> Pipe<S>[] buildPipes(Pipe<S>[] pipes) {
		int i = pipes.length;
		Pipe[] result = new Pipe[i];
		while (--i>=0) {
			result[i] = new Pipe(pipes[i].config);
		}		
		return result;
	}
	
	/**
	 * Build and array of Pipe instances from a single PipeConfig
	 * @param count of new Pipe instances created
	 * @param commonConfig PipeConfig used for all the new pipes
	 * @return array of new pipes
	 */
	public static <S extends MessageSchema<S>> Pipe<S>[] buildPipes(int count, PipeConfig<S> commonConfig) {		
		Pipe[] result = new Pipe[count];
		int i = count;
		while (--i >= 0) {
			result[i] = new Pipe<S>(commonConfig);
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

	/**
	 * Write ascii text for rational value
	 * @param pipe where ascii is written
	 * @param numerator of rational to write
	 * @param denominator of rational to write
	 * @return count of bytes written.
	 */
    public static <S extends MessageSchema<S>> int addRationalAsASCII(Pipe<S> pipe, long numerator, long denominator) {
  	      
    	  validateVarLength(pipe, 21);
	      DataOutputBlobWriter<S> outputStream = Pipe.outputStream(pipe);
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
    
    /**
     * Evenly split array of pipes across pipeCount arrays of arrays of pipes.
     * @param pipeCount int target count of groups
     * @param pipes array of pipes to be split
     * @return array of array of pipes
     */
	public static <S extends MessageSchema<S>> Pipe<S>[][] splitPipes(int pipeCount, Pipe<S>[] pipes) {
		
		Pipe<S>[][] result = new Pipe[pipeCount][];
			
		int fullLen = pipes.length;
		int last = 0;
		for(int p = 1;p<pipeCount;p++) {			
			int nextLimit = (p*fullLen)/pipeCount;			
			int plen = nextLimit-last;			
		    Pipe<S>[] newPipe = new Pipe[plen];
		    System.arraycopy(pipes, last, newPipe, 0, plen);
		    result[p-1]=newPipe;
			last = nextLimit;
		}
		int plen = fullLen-last;
	    Pipe<S>[] newPipe = new Pipe[plen];
	    System.arraycopy(pipes, last, newPipe, 0, plen);
	    result[pipeCount-1]=newPipe;
				
		return result;
				
	}

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
    
    /**
     * Read this var len field and write its contents to the target OutputStream
     * @param pipe to read from
     * @param out OutputStream to write data to
     * @throws IOException
     */
	public static <S extends MessageSchema<S>> void writeFieldToOutputStream(Pipe<S> pipe, OutputStream out) throws IOException {
        int meta = Pipe.takeByteArrayMetaData(pipe);
        int length    = Pipe.takeByteArrayLength(pipe);    
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
    
    /**
     * Read from InputStream and write the bytes to the var len field.
     * @param pipe data is written into
     * @param inputStream InputStream to read data from
     * @param byteCount of bytes to be read
     * @return true if the input stream is read
     * @throws IOException
     */
    public static boolean readFieldFromInputStream(Pipe pipe, InputStream inputStream, final int byteCount) throws IOException {
        return buildFieldFromInputStream(pipe, inputStream, byteCount, Pipe.getWorkingBlobHeadPosition(pipe), Pipe.blobMask(pipe), Pipe.blob(pipe), pipe.sizeOfBlobRing);
    }

    private static boolean buildFieldFromInputStream(Pipe pipe, InputStream inputStream, final int byteCount, int startPosition, int byteMask, byte[] buffer, int sizeOfBlobRing) throws IOException {
        boolean result = copyFromInputStreamLoop(inputStream, byteCount, startPosition, byteMask, buffer, sizeOfBlobRing, 0);        
        Pipe.addBytePosAndLen(pipe, startPosition, byteCount);
        Pipe.addAndGetBlobWorkingHeadPosition(pipe, byteCount);
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
    
    /**
     * ByteBuffers which wrap the backing blob array as 2 buffers
     * @param output Pipe target
     * @return ByteBuffers array of length 2 wrapping blob array
     */
    public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(Pipe<S> output) {
    	return wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(output),output);
    }
    
    /**
     * ByteBuffers which wrap the backing blob array as 2 buffers
     * @param originalBlobPosition int position of byte data
     * @param output Pipe target
     * @return ByteBuffers array of length 2 wrapping blob array
     */
    public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(int originalBlobPosition, Pipe<S> output) {
    	return wrappedWritingBuffers(originalBlobPosition, output, output.maxVarLen);
    }

    /**
     * ByteBuffers which wrap the backing blob array as 2 buffers
     * @param originalBlobPosition int position of byte data
     * @param output Pipe target
     * @param maxLen int length which could be written 
     * @return ByteBuffers array of length 2 wrapping blob array
     */
	public static <S extends MessageSchema<S>> ByteBuffer[] wrappedWritingBuffers(int originalBlobPosition,
			 				Pipe<S> output, int maxLen) {
		
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
 
	/**
	 * Record the number of bytes written after direct manipulation of the backing array.
	 * This method is not needed if the normal access methods are used.
	 * 
	 * @param len int length of bytes written
	 * @param output Pipe target 
	 */
    public static <S extends MessageSchema<S>> void moveBlobPointerAndRecordPosAndLength(int len, Pipe<S> output) {
    	moveBlobPointerAndRecordPosAndLength(Pipe.unstoreBlobWorkingHeadPosition(output), len, output);
    }
    
    /**
	 * Record the number of bytes written after direct manipulation of the backing array.
	 * This method is not needed if the normal access methods are used.
	 * 
     * @param originalBlobPosition int head position where the write started
     * @param len int length of bytes written
     * @param output Pipe target 
     */
    public static <S extends MessageSchema<S>> void moveBlobPointerAndRecordPosAndLength(int originalBlobPosition, int len, Pipe<S> output) {
    	
    	assert(verifyHasRoomForWrite(len, output));    	
    	
    	//blob head position is moved forward
    	if (len>0) { //len can be 0 so do nothing, len can be -1 for eof also nothing to move forward
    		Pipe.addAndGetBlobWorkingHeadPosition(output, len);
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
    
	/**
	 * Wrapped byte buffer for reading the backing ring.
	 * 
	 * @param pipe Pipe source pipe
	 * @param meta int backing meta data
	 * @param len int length to be read
	 * @return ByteBufer wrapping the blob array
	 */
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

    
    /**
     * Wrapped byte buffer for reading the backing ring.
     * 
	 * @param pipe Pipe source pipe
	 * @param meta int backing meta data
	 * @param len int length to be read
	 * @return ByteBufer wrapping the blob array
     */
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

    /**
     * Wrapped byte buffer array of 2 for reading the backing ring.
     * 
	 * @param pipe Pipe source pipe
	 * @param meta int backing meta data
	 * @param len int length to be read
	 * @return ByteBufer array wrapping the blob array
     */
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
		final int endPos = position+(len>=0?len:0);
		
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
    
    /**
     * Converts chars in an array to bytes based on UTF8 encoding.
     * 
     * @param charSeq backing array of chars
     * @param charSeqOff offset where to consume chars
     * @param charSeqLength length of chars to consume
     * @param targetBuf target array
     * @param targetIdx offset where to start writing
     * @param targetMask mask for looping the target buffer
     * @return int length of encoded bytes
     */
    public static int convertToUTF8(final char[] charSeq, final int charSeqOff, final int charSeqLength, 
    		                        final byte[] targetBuf, final int targetIdx, final int targetMask) {
    	
    	int target = targetIdx;				
        int c = 0;
        while (c < charSeqLength) {
        	target = encodeSingleChar((int) charSeq[charSeqOff+c++], targetBuf, targetMask, target);
        }
        //NOTE: the above loop will keep looping around the target buffer until done and will never cause an array out of bounds.
        //      the length returned however will be larger than targetMask, this should be treated as an error.
        return target-targetIdx;//length;
    }

    /**
     * Converts chars in an array to bytes based on UTF8 encoding.
     * 
     * @param charSeq CharSequence source chars
     * @param charSeqOff source offset to read from 
     * @param charSeqLength source length to read
     * @param targetBuf target array
     * @param targetIdx offset where to start writing
     * @param targetMask mask for looping the target buffer
     * @return int length of encoded bytes
     */
    public static int convertToUTF8(final CharSequence charSeq, final int charSeqOff, final int charSeqLength, 
    		                        final byte[] targetBuf, final int targetIdx, final int targetMask) {
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

    /**
     * Debug write of this fragment to the target appendable
     * 
     * @param input Pipe source
     * @param target Appendable to write text
     * @param fragIdx fragment idx
     */
    public static <S extends MessageSchema<S>> void appendFragment(Pipe<S> input, Appendable target, int fragIdx) {
        try {

            FieldReferenceOffsetManager from = from(input);
            int fields = from.fragScriptSize[fragIdx];
            assert (fragIdx<from.tokensLen-1);//there are no single token messages so there is no room at the last position.


            int dataSize = from.fragDataSize[fragIdx];
            String msgName = from.fieldNameScript[fragIdx];
            long msgId = from.fieldIdScript[fragIdx];

            target.append(" cursor:");
            Appendables.appendValue(target, fragIdx);
            target.append(" fields: ");
            Appendables.appendValue(target, fields);
            target.append(" ");
            target.append(msgName);
            target.append(" id: ");
            Appendables.appendValue(target, msgId).append("\n");

            if (0==fields && fragIdx==from.tokensLen-1) { //this is an odd case and should not happen
                //TODO: AA length is too long and we need to detect cursor out of bounds!
                System.err.println("total tokens:"+from.tokens.length);//Arrays.toString(from.fieldNameScript));
                throw new RuntimeException("unable to convert fragment to text");
            }


            int i = 0;
            while (i<fields) {
                final int p = i+fragIdx;
                String name = from.fieldNameScript[p];
                long id = from.fieldIdScript[p];

                int token = from.tokens[p];
                int type = TokenBuilder.extractType(token);

                //fields not message name
                String value = "";
                if (i>0 || !input.ringWalker.isNewMessage) {
                    int pos = from.fragDataSize[i+fragIdx];
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
                target.append("   ").append(name).append(":");
                Appendables.appendValue(target, id);
                target.append("  ").append(value).append("\n");

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

    /**
     * Read var length field into a target ByteBuffer
     * 
     * @param pipe Pipe source
     * @param target ByteBuffer written into
     * @param meta int field meta data
     * @param len int field length in bytes
     * @return ByteBuffer target
     */
    public static <S extends MessageSchema<S>> ByteBuffer readBytes(Pipe<S> pipe, ByteBuffer target, int meta, int len) {
		if (meta >= 0) {
			return readBytesRing(pipe,len,target,restorePosition(pipe,meta));
	    } else {
	    	return readBytesConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	    }
	}
    
    /**
     * Read var length field into a DataOutputBlobWriter, eg another field.
     * 
     * @param pipe Pipe source
     * @param target DataOutputBlobWriter field to write into
     * @param meta int field meta data
     * @param len int field length in bytes
     * @return DataOutputBlobWriter target;
     */
    public static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytes(Pipe<S> pipe, 
    		                                                                     DataOutputBlobWriter<?> target, 
    		                                                                     int meta, int len) {
		if (meta >= 0) {
			return readBytesRing(pipe,len,target,restorePosition(pipe,meta));
	    } else {
	    	return readBytesConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
	    }
	}

    /**
     * Read var length field from the pipe, position must be set up first.  Data is written to target field.
     * 
     * @param pipe Pipe source
     * @param target DataOutputBlobWriter field written into
     * @return DataOutputBlobWriter target
     */
    public static <S extends MessageSchema<S>> DataOutputBlobWriter<?> readBytes(Pipe<S> pipe, 
    		                                                                     DataOutputBlobWriter<?> target) {
    	return Pipe.readBytes(pipe, target, Pipe.takeByteArrayMetaData(pipe), Pipe.takeByteArrayLength(pipe));
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
	
	/**
	 * Reads ASCII characters or an optional null at given section of a pipe
	 * @param pipe Pipe source
	 * @param target Appendable destination
	 * @param meta int meta data for the field
	 * @param len int length for the field
	 * @return Appendable target
	 */
     public static <S extends MessageSchema<S>, A extends Appendable> A readOptionalASCII(Pipe<S> pipe, A target,
    		                                                                              int meta, int len) {
        if (len<0) {
            return null;
        }
        if (meta < 0) {//NOTE: only useses const for const or default, may be able to optimize away this conditional.
            return readASCIIConst(pipe,len,target,PipeReader.POS_CONST_MASK & meta);
        } else {
            return readASCIIRing(pipe,len,target,restorePosition(pipe, meta));
        }
    }

   /**
    * skip over the next fragment and position for reading after that fragment
    * 
    * @param pipe Pipe source
    */
   public static <S extends MessageSchema<S>> void skipNextFragment(Pipe<S> pipe) {
		   
	   skipNextFragment(pipe, Pipe.takeMsgIdx(pipe));
	   
   }

    /**
     * Skips over specified section of the pipe
     * @param pipe that you're reading from
     * @param msgIdx int fragment idx already read of the current fragment to be skipped
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

	/**
	 * checks for equals bytes
	 * 
	 * @param pipe Pipe source
	 * @param expected byte[] backing array
	 * @param expectedPos int position in backing array
	 * @param meta int field meta data
	 * @param len int length of field
	 * @return boolean true if equal content
	 */
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
	
	   /**
	    * Check if both segments of bytes are equal
	    * 
	    * @param aBack byte[] backing array
	    * @param aPos int position to read from in backing array a
	    * @param aMask int mask for looping backing array a
	    * @param bBack byte[] backing array
	    * @param bPos int position to read from in backing array b
	    * @param bMask int mask for looping backing array b
	    * @param len int length of segment
	    * @return boolean true if content equals
	    */
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
	
	/**
	 * Reads UTF8 characters or a null value from the specified pipe
	 * 
	 * @param pipe Pipe source
	 * @param target Appendable destination
	 * @param meta int meta data for the field
	 * @param len int length for the field
	 * @return Appendable target
	 */
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
     * @param readDecimalExponent int exponent -63 to +64 where the point goes
     * @param readDecimalMantissa long actual value for the number
     * @param outputRing Pipe to write to
     * @param <S> MessageSchema to write to
     */
	public static <S extends MessageSchema<S>> void addDecimalAsASCII(
			                     int readDecimalExponent,	
			                     long readDecimalMantissa, 
			                     Pipe<S> outputRing) {

		DataOutputBlobWriter<S> out = outputRing.openOutputStream(outputRing);
		Appendables.appendDecimalValue(out, readDecimalMantissa, (byte)readDecimalExponent);
		DataOutputBlobWriter.closeLowLevelField(out);
				
	}

	/**
	 * safe addition to position by masking early to ensure this value does not become negative.
	 * 
	 * @param pos int original position
	 * @param value long length to be added 
	 * @return int masked position, will not be negative
	 */
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

	/**
	 * Validate batch size is not too large
	 * @param pipe Pipe target
	 * @param size int batch size
	 */
	public static <S extends MessageSchema<S>> void validateBatchSize(Pipe<S> pipe, int size) {
		if (null != Pipe.from(pipe)) {
			int maxBatch = computeMaxBatchSize(pipe);
			if (size>maxBatch) {
				throw new UnsupportedOperationException("For the configured pipe buffer the batch size can be no larger than "+maxBatch);
			}
		}
	}

	/**
	 * maximum batch size based on the Pipe configuration
	 * 
	 * @param pipe Pipe source
	 * @return int max batch size
	 */
	public static <S extends MessageSchema<S>> int computeMaxBatchSize(Pipe<S> pipe) {
		return computeMaxBatchSize(pipe,2);//default mustFit of 2
	}

	/**
	 * maximum batch size based on pipe and must fit batches count
	 * 
	 * @param pipe Pipe source
	 * @param mustFit how many batches must fit on the pipe
	 * @return max batch size
	 */
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
	
	/**
	 * Publish an EOF value to all the outgoing pipes
	 * 
	 * @param pipe Pipe[] targets
	 */
	public static void publishEOF(Pipe<?>[] pipe) {
		int i = pipe.length;
		while (--i>=0) {
			if (null != pipe[i] && Pipe.isInit(pipe[i])) {
				publishEOF(pipe[i]);
			}
		}
	}
	
	/**
	 * Publish EOF message to the target pipe
	 * 
	 * @param pipe Pipe 
	 */
	public static <S extends MessageSchema<S>> void publishEOF(Pipe<S> pipe) {

		if (pipe.slabRingTail.tailPos.get()+pipe.sizeOfSlabRing>=pipe.slabRingHead.headPos.get()+Pipe.EOF_SIZE) {
	
			PaddedInt.set(pipe.blobRingHead.bytesHeadPos,pipe.blobRingHead.byteWorkingHeadPos.value);
			pipe.knownPositionOfEOF = (int)pipe.slabRingHead.workingHeadPos.value +  from(pipe).templateOffset;
			pipe.slabRing[pipe.slabMask & (int)pipe.knownPositionOfEOF]    = -1;
			pipe.slabRing[pipe.slabMask & ((int)pipe.knownPositionOfEOF+1)] = 0;
	
			pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value = pipe.slabRingHead.workingHeadPos.value + Pipe.EOF_SIZE);
			assert(Pipe.contentRemaining(pipe)<=pipe.sizeOfSlabRing) : "distance between tail and head must not be larger than the ring, internal error. "+pipe;
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

	/**
	 * Copy bytes from a non-wrapping array into a ring buffer (an array which wraps)
	 * @param source byte[] source array
	 * @param sourceloc int source location
	 * @param target byte[] target array
	 * @param targetloc int target location
	 * @param targetMask int target mask for looping over target array
	 * @param length int length in bytes to copy
	 */
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
	
	/**
	 * directly copy byte array field from the source pipe to the target pipe
	 * @param source Pipe source positioned to the field to be copied
	 * @param target Pipe target positioned to where the field is to be written
	 */
	public static <S extends MessageSchema<S>, T extends MessageSchema<T>> void addByteArray(Pipe<S> source, Pipe<T> target) {
				
		int sourceMeta = Pipe.takeByteArrayMetaData(source);
		int sourceLen  = Pipe.takeByteArrayLength(source);
		
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

	@Deprecated //use the Appendables methods, Delete this 
	public static <S extends MessageSchema<S>> int leftConvertIntToASCII(
			Pipe<S> pipe, 
			int value, 
			int idx) {
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

	/**
	 * read the int at this index in the buffer and return it.
	 * 
	 * @param buffer int[] source array
	 * @param mask int mask for position
	 * @param index int offset to find int
	 * @return int value at that poisition
	 */
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

	/**
	 * Read long value from int buffer
	 * 
	 * @param buffer int[] backing buffer to read from
	 * @param mask int ring buffer mask to loop over buffer
	 * @param index int position in byte array
	 * @return long value
	 */
	public static long readLong(int[] buffer, int mask, long index) {
		return (((long) buffer[mask & (int)index]) << 32) | (((long) buffer[mask & (int)(index + 1)]) & 0xFFFFFFFFl);
	}


	/**
	 * Convert bytes into a single char using UTF-8.
	 * 
	 * @param source byte[] source bytes
	 * @param posAndChar long first position and char, normally high 32 are zero
	 * @param mask int source mask 
	 * @return long with High32 as BytePosition and Low32 as Char for easy access.
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

	/**
	 * copy ASCII to bytes
	 * @param source CharSequence char source
	 * @param pipe Pipe target to write bytes field
	 * @return int new position after write
	 */
	public static <S extends MessageSchema<S>> int copyASCIIToBytes(CharSequence source, Pipe<S> pipe) {
		return copyASCIIToBytes(source, 0, source.length(), pipe);
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

	/**
	 * Add these chars as ASCII values to output pipe
	 * @param source CharSequence chars
	 * @param sourceIdx position to start reading chars
	 * @param sourceCharCount count of chars to write
	 * @param pipe Pipe target to add byte field into
	 */
	public static <S extends MessageSchema<S>> void addASCII(
			CharSequence source, int sourceIdx, 
			int sourceCharCount, Pipe<S> pipe) {
		addBytePosAndLen(pipe, copyASCIIToBytes(source, sourceIdx, sourceCharCount, pipe), sourceCharCount);
	}

	/**
	 * Add these chars as ASCII values to output pipe
	 * @param source char[] char source
	 * @param sourceIdx int start position of chars
	 * @param sourceCharCount int count of chars
	 * @param pipe Pipe target to write byte field
	 */
	public static <S extends MessageSchema<S>> void addASCII(char[] source, int sourceIdx, 
			                                               int sourceCharCount, Pipe<S> pipe) {
		addBytePosAndLen(pipe, copyASCIIToBytes(source, sourceIdx, sourceCharCount, pipe), sourceCharCount);
	}

	/**
	 * Add these chars as ASCII values to output pipe
	 * @param source CharSequence source chars
	 * @param sourceIdx start position of chars to read
	 * @param sourceLen length of chars to read
	 * @param pipe Pipe target pipe
	 * @return int new position after write
	 */
	public static <S extends MessageSchema<S>> int copyASCIIToBytes(CharSequence source, int sourceIdx, final int sourceLen, Pipe<S> pipe) {
		final int p = pipe.blobRingHead.byteWorkingHeadPos.value;
		
	    if (sourceLen > 0) {
	    	int tStart = p & pipe.blobMask;
	        copyASCIIToBytes2(source, sourceIdx, sourceLen, pipe, p, pipe.blobRing, tStart, 1+pipe.blobMask - tStart);
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

	/**
	 * Copy ascii chars as bytes into a var field in the pipe
	 * @param source char[] char source
	 * @param sourceIdx int start position to read chars
	 * @param sourceLen int length to copy
	 * @param pipe Pipe target pipe to add field
	 * @return int new position after write
	 */
    public static <S extends MessageSchema<S>> int copyASCIIToBytes(char[] source, 
    		                      int sourceIdx, final int sourceLen, Pipe<S> pipe) {
		final int p = pipe.blobRingHead.byteWorkingHeadPos.value;
	    if (sourceLen > 0) {
	    	int targetMask = pipe.blobMask;
	    	byte[] target = pipe.blobRing;

	        int tStart = p & targetMask;
	        int len1 = 1+targetMask - tStart;

			if (len1>=sourceLen) {
				copyASCIIToByte(source, sourceIdx, target, tStart, sourceLen);
			} else {
			    // done as two copies
			    copyASCIIToByte(source, sourceIdx, target, tStart, 1+ targetMask - tStart);
			    copyASCIIToByte(source, sourceIdx + len1, target, 0, sourceLen - len1);
			}
	        pipe.blobRingHead.byteWorkingHeadPos.value =  BYTES_WRAP_MASK&(p + sourceLen);
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
     * @param pipe pipe to write to
     * @param <S> MessageSchema to extend
     */
	public static <S extends MessageSchema<S>> void addUTF8(CharSequence source, int sourceCharCount, Pipe<S> pipe) {
		addBytePosAndLen(pipe, pipe.blobRingHead.byteWorkingHeadPos.value, copyUTF8ToByte(source,0, sourceCharCount, pipe));
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
    * Copy CharSequence to bytes field in the pipe using UTF8 encoding.	
    * @param source CharSequence source
    * @param sourceOffset int start position for reading chars
    * @param sourceCharCount count of chars to read
    * @param pipe Pipe target
    * @return int number of bytes written (WARNING: this is different from the ASCII version which returns the position)
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

    /**
     * Copy CharSequence to bytes field in the pipe using UTF8 encoding.	
     * @param source CharSequence chars
     * @param sourceIdx int start offset to read from
     * @param target byte[] target byte array
     * @param targetMask int target mask for looping
     * @param targetIdx int target offset
     * @param charCount int count of chars to be converted
     * @return int number of bytes written (WARNING: this is different from the ASCII version which returns the position)
     */
	public static int copyUTF8ToByte(CharSequence source, int sourceIdx, byte[] target, int targetMask, int targetIdx, int charCount) {
	    int pos = targetIdx;
	    int c = 0;
	    while (c < charCount) {
	        pos = encodeSingleChar((int) source.charAt(sourceIdx+c++), target, targetMask, pos);
	    }
	    return pos - targetIdx;
	}

	/**
	 * Write xor random data into the target array
	 * @param r Random source for random values
	 * @param target byte[] backing target array
	 * @param targetIdx int targetIdx
	 * @param count int length in bytes
	 * @param targetMask ring buffer mask to loop over target
	 */
	public static void xorRandomToBytes(Random r, byte[] target, int targetIdx, int count, int targetMask) {
		while (--count>=0) {
			target[targetMask& (targetIdx+count)] ^= r.nextInt(256);
		}
	}
	
	/**
	 * Xor target bytes in place with the source bytes
	 * @param source byte[] source backing data
	 * @param sourceIdx int source start position
	 * @param sourceMask int mask ring buffer source byte[]
	 * @param target byte[] target backing array
	 * @param targetIdx int target start xor position
	 * @param targetMask int mask to loop back over target buffer
	 * @param count int total count of bytes length
	 */
	public static void xorBytesToBytes(byte[] source, int sourceIdx, int sourceMask,
			                            byte[] target, int targetIdx, int targetMask, 
			                            int count) {
		for(int i=0; i<count; i++) {
			target[targetMask & (targetIdx+count)] ^= source[sourceMask & (sourceIdx+count)];
		}
	}	
	
	
	/*
	 * WARNING: unlike the ASCII version this method returns bytes written and not the position
	 */
	
	/**
	 * Copy chars to UTF8 encoded bytes
	 * @param source char[] chars
	 * @param sourceCharCount count of chars from 0 position
	 * @param pipe Pipe target to write field
	 * @return int count of bytes copied
	 */
	public static <S extends MessageSchema<S>> int copyUTF8ToByte(char[] source, int sourceCharCount, 
			                                                      Pipe<S> pipe) {
		int byteLength = Pipe.copyUTF8ToByte(source, 0, pipe.blobRing, pipe.blobMask, pipe.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
		pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value+byteLength);
		return byteLength;
	}

	/**
	 * Copy chars to UTF8 encoded bytes
	 * @param source char[] chars
	 * @param sourceOffset start position to read chars
	 * @param sourceCharCount total count of chars read
	 * @param pipe Pipe target to write field
	 * @return int count of bytes copied
	 */
	public static <S extends MessageSchema<S>> int copyUTF8ToByte(char[] source, int sourceOffset, int sourceCharCount, Pipe<S> pipe) {
	    int byteLength = Pipe.copyUTF8ToByte(source, sourceOffset, pipe.blobRing, pipe.blobMask, pipe.blobRingHead.byteWorkingHeadPos.value, sourceCharCount);
	    pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value+byteLength);
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

	/**
	 * UTF8 Encode single char to bytes
	 * @param c int, single char
	 * @param buffer byte[] target 
	 * @param mask int mask for looping ring buffer target
	 * @param pos int index in target to write bytes
	 * @return int count of bytes written for this char
	 */
	public static <S extends MessageSchema<S>> int encodeSingleChar(int c, byte[] buffer,int mask, int pos) {

	    if (c <= 0x007F) { // less than or equal to 7 bits or 127
	        // code point 7
	        buffer[mask&pos] = (byte) c;
	        return pos+1;
	    } else {
	        return encodeCodePoint11(c, buffer, mask, pos);
	    }
	}

	private static int encodeCodePoint11(final int c, final byte[] buffer, final int mask, final int pos) {
		if (c <= 0x07FF) { // less than or equal to 11 bits or 2047
		    // code point 11
		    buffer[mask&pos] = (byte) (0xC0 | ((c >> 6) & 0x1F));
		    buffer[mask&(pos+1)] = (byte) (0x80 | (c & 0x3F));
			return pos+2;
		} else {
		    return encodeCodePoint16(c, buffer, mask, pos);
		}
		
	}

	private static int encodeCodePoint16(int c, byte[] buffer, int mask, int pos) {
		if (c <= 0xFFFF) { // less than or equal to  16 bits or 65535
			//special case logic here because we know that c > 7FF and c <= FFFF so it may hit these
			// D800 through DFFF are reserved for UTF-16 and must be encoded as an 63 (error)
			if (0xD800 != (0xF800&c)) {
				// code point 16
				buffer[mask&pos] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				buffer[mask&(pos+1)] = (byte) (0x80 | ((c >> 6) & 0x3F));
			    buffer[mask&(pos+2)] = (byte) (0x80 | (c & 0x3F));
				return pos+3;
			} else {
				buffer[mask&pos] = 63;
				return pos+1;
			}

		} else {
		    pos = encodeCodePoint21(c, buffer, mask, pos);
		    buffer[mask&pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
		    buffer[mask&pos++] = (byte) (0x80 | (c & 0x3F));
		    return pos;
		}
	}

	private static <S extends MessageSchema<S>> int encodeCodePoint21(int c, byte[] buffer, int mask, int pos) {
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

	/**
	 * add byte buffer as a field in pipe
	 * @param source ByteBuffer source with var data
	 * @param pipe Pipe target
	 */
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

	/**
	 * add byte buffer as a field in pipe
	 * @param source ByteBuffer target
	 * @param length int limits bytes from target byte buffer
	 * @param pipe Pipe target 
	 */
   public static <S extends MessageSchema<S>> void addByteBuffer(ByteBuffer source, int length, Pipe<S> pipe) {
        int bytePos = pipe.blobRingHead.byteWorkingHeadPos.value;
        int len = -1;
        if (null!=source && length>0) {
            len = length;
            copyByteBuffer(source,length,pipe);
        }
        Pipe.addBytePosAndLen(pipe, bytePos, len);
    }
	   
   /**
    * copy ByteBuffer into pipe as a field.
    * @param source ByteBuffer
    * @param length int limits the bytes copied from the source
    * @param pipe Pipe target for writing the field
    */
	public static <S extends MessageSchema<S>> void copyByteBuffer(ByteBuffer source, int length, 
			                                                       Pipe<S> pipe) {
		validateVarLength(pipe, length);
		int idx = pipe.blobRingHead.byteWorkingHeadPos.value & pipe.blobMask;
		int partialLength = 1 + pipe.blobMask - idx;
		//may need to wrap around ringBuffer so this may need to be two copies
		if (partialLength>=length) {
		    source.get(pipe.blobRing, idx, length);
		} else {
		    //read from source and write into byteBuffer
		    source.get(pipe.blobRing, idx, partialLength);
		    source.get(pipe.blobRing, 0, length - partialLength);
		}
		pipe.blobRingHead.byteWorkingHeadPos.value = BYTES_WRAP_MASK&(pipe.blobRingHead.byteWorkingHeadPos.value + length);
	}

	/**
	 * Add byte array to target pipe
	 * 
	 * @param outputRing Pipe target
	 * @param mask int mask to loop arround data target
	 * @param len int total bytes to copy
	 * @param data byte[] backing array
	 * @param offset int bytes position
	 */
	public static <S extends MessageSchema<S>> void addByteArrayWithMask(final Pipe<S> outputRing, 
												int mask, int len, byte[] data, int offset) {
		validateVarLength(outputRing, len);
		copyBytesFromToRing(data,offset,mask,outputRing.blobRing,PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),outputRing.blobMask, len);
		addBytePosAndLenSpecial(outputRing, PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),len);
		PaddedInt.set(outputRing.blobRingHead.byteWorkingHeadPos, BYTES_WRAP_MASK&(PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos) + len));
	}
	
	/**
	 * Set byte array into specific location
	 * 
	 * @param outputRing Pipe target
	 * @param mask int backing data mask
	 * @param len int count of bytes
	 * @param data byte[] backing data to copy
	 * @param offset int start at position to copy
	 * @param slabPosition int slab position to set this data
	 */
    public static <S extends MessageSchema<S>> void setByteArrayWithMask(final Pipe<S> outputRing,
    		int mask, int len, byte[] data, int offset, long slabPosition) {
	        validateVarLength(outputRing, len);
	        copyBytesFromToRing(data,offset,mask,outputRing.blobRing,PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos),outputRing.blobMask, len);
            setBytePosAndLen(slab(outputRing), outputRing.slabMask, slabPosition, PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos), len, bytesWriteBase(outputRing));
	        PaddedInt.set(outputRing.blobRingHead.byteWorkingHeadPos, BYTES_WRAP_MASK&(PaddedInt.get(outputRing.blobRingHead.byteWorkingHeadPos) + len));
	}

    /**
     * Peek an int value without moving the cursors.
     * 
     * @param buf int[] backing slap array
     * @param pos int index position to read from
     * @param mask int mask for access to backing array
     * @return int value
     */
	public static <S extends MessageSchema<S>> int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

	/**
	 * Peek a long value without moving the cursors.
	 * 
     * @param buf int[] backing slap array
     * @param pos int index position to read from
     * @param mask int mask for access to backing array
     * @return int value
	 */
    public static <S extends MessageSchema<S>> long peekLong(int[] buf, long pos, int mask) {

        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    /**
     * check if this pipe has been shutdown
     * @param pipe Pipe source to read from
     * @return boolean true if is shutdown
     */
    public static <S extends MessageSchema<S>> boolean isShutdown(Pipe<S> pipe) {
    	return pipe.imperativeShutDown.get();
    }

    /**
     * shutdown this pipe now, this should only be called by internal logic for an emergency
     * @param pipe Pipe pipe to shutdown
     */
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

    /**
     * add a null byte array in this position of the target pipe
     * @param pipe Pipe target
     */
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
    
    /**
     * Ors int value to specific position in the given pipe
     * @param value int to be ored
     * @param pipe Pipe to be written to
     * @param position position to or
     */
    public static <S extends MessageSchema<S>> void orIntValue(int value, Pipe<S> pipe, long position) {
        assert(pipe.slabRingHead.workingHeadPos.value <= Pipe.tailPosition(pipe)+pipe.sizeOfSlabRing);
        orValue(pipe.slabRing,pipe.slabMask,position,value);
    }


    /**
     * Called to start a new message when using the low level API
     * 
     * @param pipe Pipe target
     * @param msgIdx int message to be written, Find this value in the MessageSchema for the type in use
     * @return int size of the message, this value is needed to confirm writes.
     */
    public static <S extends MessageSchema<S>> int addMsgIdx(final Pipe<S> pipe, int msgIdx) {
         assert(Pipe.workingHeadPosition(pipe) < (Pipe.tailPosition(pipe)+ pipe.sizeOfSlabRing  /*    pipe.slabMask*/  )) : "Working position is now writing into published(unreleased) tail "+
                Pipe.workingHeadPosition(pipe)+"<"+Pipe.tailPosition(pipe)+"+"+pipe.sizeOfSlabRing /*pipe.slabMask*/
                +" total "+((Pipe.tailPosition(pipe)+pipe.slabMask))+"  "+pipe;
        
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

    /**
     * set an int value at this position
     * 
     * @param buffer int[] backing buffer
     * @param mask int mask
     * @param offset long offset into backing buffer
     * @param value int value to be assigned to this position
     */
	public static <S extends MessageSchema<S>> void setValue(int[] buffer, int mask, long offset, int value) {
        buffer[mask & (int)offset] = value;
    }

	/**
	 * or an int value at this position
	 * 
	 * @param buffer int[] backing buffer
	 * @param mask int mask
	 * @param offset long offset into backing buffer
	 * @param value int value to be ored at this position
	 */
	public static <S extends MessageSchema<S>> void orValue(int[] buffer, int mask, long offset, int value) {
        buffer[mask & (int)offset] |= value;
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

    /**
     * Writes the position and len for a var byte field to the pipe but it does not move the 
     * byte count forward, that is assumed to already be done.
     * @param targetOutput Pipe target
     * @param startBytePos int start position where the bytes were written
     * @param bytesLength int count length of bytes written
     */    
    public static <S extends MessageSchema<S>> void addBytePosAndLenSpecial(Pipe<S> targetOutput, final int startBytePos, int bytesLength) {
        PaddedLong workingHeadPos = getWorkingHeadPositionObject(targetOutput);
        setBytePosAndLen(slab(targetOutput), targetOutput.slabMask, workingHeadPos.value, 
        		         startBytePos, bytesLength, 
        		         bytesWriteBase(targetOutput));
        PaddedLong.add(workingHeadPos, 2);
    }

	static <S extends MessageSchema<S>> void setBytePosAndLen(int[] buffer, int bufferMask, 
			long bufferPos, int dataBlobPos, int dataBlobLen, int baseBytePos) {
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
		return pipe.blobMask & (pos + Pipe.bytesReadBase(pipe));
	}
		
	/**
	 * convert meta and len field data into a specific position field.
	 * NOTE: this method has the side effect of moving the cursor forward.
	 * 
	 * @param meta int pos and location data for thie field
	 * @param pipe Pipe pipe holding this data
	 * @param len int length for this field
	 * @return int position
	 */
    public static <S extends MessageSchema<S>> int bytePosition(int meta, Pipe<S> pipe, int len) {
    	int pos =  restorePosition(pipe, meta & RELATIVE_POS_MASK);
        if (len>=0) {
        	Pipe.addAndGetBlobWorkingTailPosition(pipe, len);
        }        
        return pos;
    }
    
    
    /**
	 * convert meta field data into a specific position field.
	 * NOTE: this method has NO side effect of moving the cursor forward
	 *       and may not be the method you want to call.
	 *       
     * @param meta int meta
     * @param pipe Pipe pipe holding this data
     * @return int position
     */
    public static <S extends MessageSchema<S>> int convertToPosition(int meta, Pipe<S> pipe) {
    	return restorePosition(pipe, meta & RELATIVE_POS_MASK);
    }


    /**
     * add 3 integer values in a single call
     * @param buffer byte[] target backing array
     * @param mask int mask to loop over ring buffer
     * @param headCache head position 
     * @param value1 int value 1
     * @param value2 int value 2
     * @param value3 int value 3
     */
    public static <S extends MessageSchema> void addValue(int[] buffer, int mask, PaddedLong headCache, int value1, int value2, int value3) {

        long p = headCache.value;
        buffer[mask & (int)p++] = value1;
        buffer[mask & (int)p++] = value2;
        buffer[mask & (int)p++] = value3;
        headCache.value = p;

    }

    /**
     * add Decimal value to pipe
     * @param exponent int exponent -63 to +63
     * @param mantissa long raw value
     * @param pipe Pipe target
     */
    public static <S extends MessageSchema<S>> void addDecimal(int exponent, long mantissa, Pipe<S> pipe) {
        pipe.slabRingHead.workingHeadPos.value = setValues(pipe.slabRing, pipe.slabMask, pipe.slabRingHead.workingHeadPos.value, exponent, mantissa);
    }

	static <S extends MessageSchema<S>> long setValues(int[] buffer, int rbMask, long pos, int value1, long value2) {
		buffer[rbMask & (int)pos++] = value1;
        buffer[rbMask & (int)pos++] = (int)(value2 >>> 32);
        buffer[rbMask & (int)pos++] = (int)(value2 & 0xFFFFFFFF);
		return pos;
	}

	/**
     * Writes long value to the specified pipe
     * @param value long to be written
     * @param pipe pipe to be written to
     * @param <S> MessageSchema to be extended
     */
	public static <S extends MessageSchema<S>> void addLongValue(long value, Pipe<S> pipe) {
		 addLongValue(pipe.slabRing, pipe.slabMask, pipe.slabRingHead.workingHeadPos, value);
	}

	
	/**
	 * Writes long value to the specified pipe
	 * @param buffer int[] target backing array
	 * @param mask int for looping over target buffer
	 * @param headCache PaddedLong position of head
	 * @param value long value 
	 */
    public static <S extends MessageSchema<S>> void addLongValue(
    		int[] buffer, int mask, PaddedLong headCache, long value) {

        long p = headCache.value;
        buffer[mask & (int)p] = (int)(value >>> 32);
        buffer[mask & (int)(p+1)] = ((int)value);
        headCache.value = p+2;

    }

    static <S extends MessageSchema<S>> int readByteArrayLength(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int) (rbMask & (rbPos + fieldPos + 1))];// second int is always the length
    }

    /**
     * Read the length portion of the var length field
     * @param idx int position of field
     * @param pipe Pipe source pipe
     * @return int length
     */
	public static <S extends MessageSchema<S>> int readByteArrayLength(int idx, Pipe<S> pipe) {
		return readByteArrayLength(idx,pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value);
	}

	/**
	 * consume the length for the var length byte field
	 * @param pipe Pipe source
	 * @return int length
	 */
	public static <S extends MessageSchema<S>> int takeByteArrayLength(Pipe<S> pipe) {
	//    assert(ring.structuredLayoutRingTail.workingTailPos.value<RingBuffer.workingHeadPosition(pipe));
		return pipe.slabRing[(int)(pipe.slabMask & (pipe.slabRingTail.workingTailPos.value++))];// second int is always the length
	}

	@Deprecated
	public static <S extends MessageSchema<S>> int takeRingByteLen(Pipe<S> pipe) {
		return takeByteArrayLength(pipe);	
	}

    /**
     * Given the meta data for the var length field return the backing array.
     * @param meta int meta value
     * @param pipe Pipe source
     * @return byte[] backing array
     */
    public static <S extends MessageSchema<S>> byte[] byteBackingArray(int meta, Pipe<S> pipe) {
        return pipe.blobRingLookup[meta>>>31];
    }

    @Deprecated
	public static <S extends MessageSchema<S>> int readRingByteMetaData(int pos, Pipe<S> pipe) {
		return readByteArraMetaData(pos, pipe);
	}
	
    /**
     * Read meta data for a given var length field at this position
     * @param pos int index position
     * @param pipe Pipe source
     * @return int meta
     */
	public static <S extends MessageSchema<S>> int readByteArraMetaData(int pos, Pipe<S> pipe) {
		return readIntValue(pos,pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
	}

	@Deprecated
	public static <S extends MessageSchema<S>> int takeRingByteMetaData(Pipe<S> pipe) {
		return takeByteArrayMetaData(pipe);
	}
	
	/**
	 * Consume the meta data for the var length byte field.  This must always be read BEFORE the length.
	 * @param pipe Pipe source
	 * @return int meta
	 */
	public static <S extends MessageSchema<S>> int takeByteArrayMetaData(Pipe<S> pipe) {
		//NOTE: must always read metadata before length, easy mistake to make, need assert to ensure this is caught if happens.
		return readIntValue(0,pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value++);
	}

    static <S extends MessageSchema<S>> int readIntValue(int fieldPos, int[] rbB, int rbMask, long rbPos) {
        return rbB[(int)(rbMask & (rbPos + fieldPos))];
    }

    /**
     * Reads the data at a specific index on the given pipe and returns an int
     * @param idx index to read
     * @param pipe pipe to read from
     * @param <S> MessageSchema to extend
     * @return int value from specified index
     */
    public static <S extends MessageSchema<S>> int readIntValue(int idx, Pipe<S> pipe) {
    	return readIntValue(idx, pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
    }
    
    @Deprecated
    public static <S extends MessageSchema<S>> int readValue(int idx, Pipe<S> pipe) {
    	return readIntValue(idx,pipe);
    }

    /**
     * Read int and move the cursor forward
     * @param pipe Pipe source
     * @return int value
     */
    public static <S extends MessageSchema<S>> int takeInt(Pipe<S> pipe) {
    	return readValue(pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value++);
    }
    
    @Deprecated //use takeInt
    public static <S extends MessageSchema<S>> int takeValue(Pipe<S> pipe) {
    	return takeInt(pipe);
    }
   
    /**
     * read int value at the given position
     * @param backing int[] backing buffer
     * @param mask int looping mask
     * @param position int position index
     * @return int value
     */
    public static <S extends MessageSchema<S>> int readIntValue(int[] backing, int mask, long position) {
        return backing[(int)(mask & position)];
    }
    
    @Deprecated
    public static <S extends MessageSchema<S>> int readValue(int[] rbB, int rbMask, long rbPos) {
        return readIntValue(rbB, rbMask, rbPos);
    }
    
    /**
     * take optional Integer, this may be null. null is defined as a specific int value in FROM
     * @param pipe Pipe source
     * @return Integer value
     */
    public static <S extends MessageSchema<S>> Integer takeOptionalValue(Pipe<S> pipe) {
        int absent32Value = FieldReferenceOffsetManager.getAbsent32Value(Pipe.from(pipe));
        return takeOptionalValue(pipe, absent32Value);
    }

    /**
     * take optional Integer, this may be null. null is defined as specific int value passed in.
     * @param pipe Pipe source
     * @param absent32Value int value mapped to null
     * @return Integer value
     */
    public static <S extends MessageSchema<S>> Integer takeOptionalValue(Pipe<S> pipe, int absent32Value) {
        int temp = readIntValue(0, pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value++);
        return absent32Value!=temp ? new Integer(temp) : null;
    }

    /**
     * Read the long position and move the cursor forward.
     * @param pipe Pipe source
     * @return long value
     */
    public static <S extends MessageSchema<S>> long takeLong(Pipe<S> pipe) {
        
        //this assert does not always work because the head position is volatile, Not sure what should be done to resolve it.  
        //assert(ring.slabRingTail.workingTailPos.value<Pipe.workingHeadPosition(ring)) : "working tail "+ring.slabRingTail.workingTailPos.value+" but head is "+Pipe.workingHeadPosition(ring);
    	
        long result = readLong(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value);
    	pipe.slabRingTail.workingTailPos.value+=2;
    	return result;
    }
    
    /**
     * Take optional Long which may be null. Null is defined as specific long value in FROM
     * @param pipe Ping source
     * @return Long value
     */
    public static <S extends MessageSchema<S>> Long takeOptionalLong(Pipe<S> pipe) {
        long absent64Value = FieldReferenceOffsetManager.getAbsent64Value(Pipe.from(pipe));
        return takeOptionalLong(pipe, absent64Value);
    }

    /**
     * Take optional Long which may be null. Null is defined as specific long value passed in.
     * @param pipe Pipe source
     * @param absent64Value long value mapped to null
     * @return Long value
     */
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
    
    /**
     * peek message and determine if it matches the expected value
     * @param pipe Pipe source
     * @param expected int message idx
     * @return boolean true if there is a message and if it matches expected
     */
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected) {
        return (Pipe.contentRemaining(pipe)>0) && peekInt(pipe)==expected;    	
    }
    
    /**
     * peek message and determine if it does not match the expected value
     * @param pipe Pipe source
     * @param expected int message idx
     * @return boolean true if there is a message but it is not the expected value
     */
    public static <S extends MessageSchema<S>> boolean peekNotMsg(Pipe<S> pipe, int expected) {
        return (Pipe.contentRemaining(pipe)>0) && peekInt(pipe)!=expected;    	
    }    

    /**
     * peek message and determine if it matches one of 2 potential values
     * @param pipe Pipe source
     * @param expected1 int message idx
     * @param expected2 int message idx
     * @return boolean true if there is a message and it matches one of the two expected values.
     */
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected1, int expected2) {
        return (Pipe.contentRemaining(pipe)>0) && (peekInt(pipe)==expected1 || peekInt(pipe)==expected2);
    }
    
    /**
     * peek message and determine if it matches one of 3 potential values
     * @param pipe Pipe source
     * @param expected1 int message idx
     * @param expected2 int message idx
     * @param expected3 int message idx
     * @return boolean true if there is a message and it matches one of the three expected values
     */
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected1, int expected2, int expected3) {
        return (Pipe.contentRemaining(pipe)>0) && (peekInt(pipe)==expected1 || peekInt(pipe)==expected2 || peekInt(pipe)==expected3);
    }
    
    /**
     * peek message and determine if it matches one of 3 potential values
     * @param pipe Pipe source
     * @param expected1 int message idx
     * @param expected2 int message idx
     * @param expected3 int message idx
     * @param expected4 int message idx
     * @return boolean true if there is a message and it matches one of the three expected values
     */
    public static <S extends MessageSchema<S>> boolean peekMsg(Pipe<S> pipe, int expected1, int expected2, int expected3, int expected4) {
        return (Pipe.contentRemaining(pipe)>0) && 
        		 (peekInt(pipe)==expected1 
        		  || peekInt(pipe)==expected2 
        		  || peekInt(pipe)==expected3
        		  || peekInt(pipe)==expected4);
    }
    
    /**
     * peek the int value at this position
     * @param pipe Pipe source
     * @return int value
     */
    public static <S extends MessageSchema<S>> int peekInt(Pipe<S> pipe) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readIntValue(pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value);
    }
    
    /**
     * peek the int value at this relative offset from where the cursor is upon call
     * @param pipe Pipe source
     * @param offset int relative position of field 
     * @return int value
     */
    public static <S extends MessageSchema<S>> int peekInt(Pipe<S> pipe, int offset) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readIntValue(pipe.slabRing, pipe.slabMask, pipe.slabRingTail.workingTailPos.value+offset);
    }
   
    /**
     * peek the long value at this relative offset from where the cursor is upon call
     * @param pipe Pipe source
     * @param offset int relative position of field
     * @return long value
     */
    public static <S extends MessageSchema<S>> long peekLong(Pipe<S> pipe, int offset) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
        return readLong(pipe.slabRing,pipe.slabMask,pipe.slabRingTail.workingTailPos.value+offset);
    }
    
    /**
     * peek UTF8 text at this relative offset from where the cursor is upon call
     * @param pipe Pipe source
     * @param offset int relative position of field
     * @param target Appendable destination for text
     * @return Appendable target
     */
    public static <S extends MessageSchema<S>, A extends Appendable> A peekUTF8(Pipe<S> pipe, int offset, A target) {
    	assert((Pipe.contentRemaining(pipe)>0)) : "results would not be repeatable";
    	return readUTF8(pipe, target, peekInt(pipe,offset),peekInt(pipe,offset+1));
    }

    /**
     * Content remaining on Pipe to be consumed. Uses published head and tail so if this is not zero then
     * there is a full message to be consumed.
     * 
     * @param pipe Pipe source
     * @return int value
     */
    public static <S extends MessageSchema<S>> int contentRemaining(Pipe<S> pipe) {
        int result = (int)(pipe.slabRingHead.headPos.get() - pipe.slabRingTail.tailPos.get()); //must not go past add count because it is not release yet.
        assert(result>=0) : "content remaining must never be negative. problem in "+schemaName(pipe)+" pipe "; //do not add pipe.toString since it will be recursive.
        return result;
    }

    /**
     * Checks to see whether pipe has any data or not. If not empty does NOT mean it has a full message.
     * We may have data on the pipe which can not be cleared or read, in that case it is NOT empty.
     * @param pipe pipe to be checked
     * @return <code>true</code> if pipe has no data else <code>false</code>
     */
    public static boolean isEmpty(Pipe<?> pipe) {
    	return (pipe.slabRingHead.workingHeadPos.value == pipe.slabRingTail.workingTailPos.value) && 
    		   (Pipe.headPosition(pipe) == Pipe.tailPosition(pipe));
    	
    	
    }

    /**
     * Release read lock on this fragment. Allows the producer to write over this location.
     * @param pipe Pipe target
     * @return int total byte count consumed by fragment
     */
    public static <S extends MessageSchema<S>> int releaseReadLock(Pipe<S> pipe) {
    	
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        int bytesConsumedByFragment = takeInt(pipe);

        assert(bytesConsumedByFragment>=0) : "Bytes consumed by fragment must never be negative, was fragment written correctly?, is read positioned correctly?";
        Pipe.markBytesReadBase(pipe, bytesConsumedByFragment);  //the base has been moved so we can also use it below.
        long tail = pipe.slabRingTail.workingTailPos.value;
		batchedReleasePublish(pipe, 
        		              pipe.blobRingTail.byteWorkingTailPos.value = pipe.blobReadBase, 
        		              tail);
        assert(validateInsideData(pipe, pipe.blobReadBase));
        
        return bytesConsumedByFragment;        
    }

    /**
     * Move cursor forward to next fragment for reading but does NOT release the previous fragment
     * to be written over.  This is helpful in cases where multiple messages must be held until a 
     * transaction is complete.
     * 
     * @param pipe Pipe target
     * @return int count of bytes consumed by previous fragment
     */
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

    /**
     * If batch is not yet full this new publish is stored else this is added and released with the rest
     * @param pipe Pipe target
     * @param blobTail int blobTail position to release
     * @param slabTail int slabTail postiion to release
     */
    public static <S extends MessageSchema<S>> void batchedReleasePublish(Pipe<S> pipe, int blobTail, long slabTail) {
        assert(null==pipe.ringWalker || pipe.ringWalker.cursor<=0 && !PipeReader.isNewMessage(pipe.ringWalker)) : "Unsupported mix of high and low level API.  ";
        releaseBatchedReads(pipe, blobTail, slabTail);
    }
    
    static <S extends MessageSchema<S>> void releaseBatchedReads(Pipe<S> pipe, int workingBlobRingTailPosition, long nextWorkingTail) {

    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        if (decBatchRelease(pipe)<=0) { 
           setBlobTailPosition(pipe,workingBlobRingTailPosition);
           
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
     * @param pipe Pipe to be examined
     */
    public static <S extends MessageSchema<S>> void releaseAllBatchedReads(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        if (pipe.lastReleasedSlabTail > pipe.slabRingTail.tailPos.get()) {
            PaddedInt.set(pipe.blobRingTail.bytesTailPos,pipe.lastReleasedBlobTail);
            pipe.slabRingTail.tailPos.lazySet(pipe.lastReleasedSlabTail);
            pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
        }

    }
    
    /**
     * Release all batched read releases up to the current position. Eg all things read can now be written over.
     * @param pipe Pipe target
     */
    public static <S extends MessageSchema<S>> void releaseBatchedReadReleasesUpToThisPosition(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        releaseBatchedReadReleasesUpToPosition(pipe, Pipe.getWorkingTailPosition(pipe), Pipe.getWorkingBlobTailPosition(pipe));
                
    }

    /**
     * Release all batched read releases up to the passed in position.
     * @param pipe Pipe target
     * @param newTailToPublish long new tail
     * @param newTailBytesToPublish int new bytes total
     */
    public static <S extends MessageSchema<S>> void releaseBatchedReadReleasesUpToPosition(Pipe<S> pipe, long newTailToPublish,  int newTailBytesToPublish) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
    	assert(newTailToPublish<=pipe.lastReleasedSlabTail) : "This new value is forward of the next Release call, eg its too large";
        assert(newTailToPublish>=pipe.slabRingTail.tailPos.get()) : "This new value is behind the existing published Tail, eg its too small ";

        PaddedInt.set(pipe.blobRingTail.bytesTailPos, newTailBytesToPublish);
        pipe.slabRingTail.tailPos.lazySet(newTailToPublish);
        assert(Pipe.contentRemaining(pipe)<=pipe.sizeOfSlabRing) : "distance between tail and head must not be larger than the ring, internal error. "+pipe;
        pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
    }

    /**
     * Low level API for publish for new fragment. Must be called in order for consumer to see fragment.
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

    /**
     * Records the count of bytes consumed by this fragment into the pipe. This is part of the internal
     * bookkeeping which allows for skipping over messages when reqired.
     * 
     * @param pipe Pipe target
     * @return int count of bytes
     */
	public static <S extends MessageSchema<S>> int writeTrailingCountOfBytesConsumed(Pipe<S> pipe) {
		return writeTrailingCountOfBytesConsumed(pipe, pipe.slabRingHead.workingHeadPos.value++);
	}

	/**
	 * Publish writes for this fragment so consumer stage can see it. Adds optional hidden trailing bytes
	 * between the messages. This advanced feature allows for extra meta data to be passed as long as 
	 * consuming stage knows its there and should be read. If it does not then the data is ignored.
	 * 
	 * @param pipe Pipe source
	 * @param optionalHiddenTrailingBytes int count of bytes to add after message
	 * @return int total count of bytes consumed by this fragment including optional count.
	 */
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

    /**
     * Notify any listeners to thsi pipe that a publication has been made. 
     * This feature is only used in corner cases where a stage needs to watch a number of pipes
     * and only has work if one of them has new content. Polling many pipes is slower than this 
     * notification listener however polling one or two pipes is quicker than this pattern.
     * 
     * @param pipe Pipe observed pipe
     */
	public static <S extends MessageSchema<S>> void notifyPubListener(Pipe<S> pipe) {
		notifyPubListener(pipe.pubListeners);
	
	}

	private static void notifyPubListener(PipePublishListener[] listeners) {
		int i = listeners.length;	
    	while (--i>=0) {
    		listeners[i].published();
    	}
	}
    
    /**
     * Publish all the batched writes so they can all be seen by the consumer stage
     * @param pipe Pipe target
     */	
    public static <S extends MessageSchema<S>> void publishWritesBatched(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
        //single length field still needs to move this value up, so this is always done
    	//in most cases this is redundant
		pipe.blobWriteLastConsumedPos = pipe.blobRingHead.byteWorkingHeadPos.value;

    	publishHeadPositions(pipe);
    }


    /**
     * Used by assert any time we need to check the validity of what has been written to the pipe.
     * @param pipe Pipe target
     * @return boolean true if the message has a valid field count
     */
    public static <S extends MessageSchema<S>> boolean validateFieldCount(Pipe<S> pipe) {
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

		pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
    }


    /**
     * Internal method to publish head positions as part of publish writes process
     * @param pipe Pipe target
     */
	public static <S extends MessageSchema<S>> void publishHeadPositions(Pipe<S> pipe) {

		assert(pipe.slabRingHead.workingHeadPos.value >= Pipe.headPosition(pipe));
    	assert(pipe.llWrite.llrConfirmedPosition<=Pipe.headPosition(pipe) || 
    		   pipe.slabRingHead.workingHeadPos.value<=pipe.llWrite.llrConfirmedPosition) :
    			   "Possible unsupported mix of high and low level API. NextHead>head and workingHead>nextHead "+pipe+" nextHead "+pipe.llWrite.llrConfirmedPosition+"\n"+
    		       "OR the XML field types may not match the accessor methods in use.";
    	//not working for some tests...
    	//assert(validateFieldCount(pipe)) : "No fragment could be found with this field count, check for missing or extra fields.";

	    if ((--pipe.batchPublishCountDown<=0)) {
	        PaddedInt.set(pipe.blobRingHead.bytesHeadPos, pipe.blobRingHead.byteWorkingHeadPos.value);
	        pipe.slabRingHead.headPos.lazySet(pipe.slabRingHead.workingHeadPos.value);
	        assert(Pipe.contentRemaining(pipe)<=pipe.sizeOfSlabRing) :
	        	  "distance between tail and head must not be larger than the ring, internal error. "+pipe;
	        pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
	    } else {
	        storeUnpublishedWrites(pipe);
	    }
	}

	/**
	 * Store the writes for publish later.
	 * @param pipe Pipe targe
	 */
	public static <S extends MessageSchema<S>> void storeUnpublishedWrites(Pipe<S> pipe) {
		pipe.lastPublishedBlobRingHead = pipe.blobRingHead.byteWorkingHeadPos.value;
		pipe.lastPublishedSlabRingHead = pipe.slabRingHead.workingHeadPos.value;
	}


	/**
	 * Abandon the fragment written so far so a different fragment can be written or 
	 * non at all.
	 * 
	 * @param pipe
	 */
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

	/**
	 * Direct access to the mask for looping the blob.
	 * @param pipe
	 * @return the mask value
	 */
	public static <S extends MessageSchema<S>> int blobMask(Pipe<S> pipe) {
		return pipe.blobMask;
	}
	
	/**
	 * Direct access to the mask for looping the slab
	 * @param pipe
	 * @return the mask value
	 */
	public static <S extends MessageSchema<S>> int slabMask(Pipe<S> pipe) {
	    return pipe.slabMask;
	}

	/**
	 * Direct access to the head position of the slab
	 * @param pipe Pipe source
	 * @return long slab head
	 */
	public static <S extends MessageSchema<S>> long getSlabHeadPosition(Pipe<S> pipe) {
		return headPosition(pipe);
	}
	
	/**
	 * Direct access to the head position of the slab
	 * @param pipe Pipe source
	 * @return long slab head
	 */
	public static <S extends MessageSchema<S>> long headPosition(Pipe<S> pipe) {
		 assert(pipe.slabRingHead.headPos.get()>=0L) : "head position will never be negative";
		 return pipe.slabRingHead.headPos.get();
	}

	/**
	 * Direct access to the working head position of the slab
	 * @param pipe Pipe source
	 * @return long slab working head, not yet published and actively used
	 */
    public static <S extends MessageSchema<S>> long workingHeadPosition(Pipe<S> pipe) {
        return PaddedLong.get(pipe.slabRingHead.workingHeadPos);
    }

    /**
     * Directly set the new working head position for the slab
     * @param pipe Pipe target
     * @param value long new head position for slab
     */
    public static <S extends MessageSchema<S>> void setWorkingHead(Pipe<S> pipe, long value) {
        PaddedLong.set(pipe.slabRingHead.workingHeadPos, value);
    }

    /**
     * Directly increment the working head position for the slab
     * @param pipe Pipe target
     * @param inc int step to increment head by
     * @return long new head slab position after inc
     */
    public static <S extends MessageSchema<S>> long addAndGetWorkingHead(Pipe<S> pipe, int inc) {
        return PaddedLong.add(pipe.slabRingHead.workingHeadPos, inc);
    }

    /**
     * Direct access to working tail position of the slab
     * @param pipe Pipe target
     * @return long new working tail position, not yet published, activily used
     */
    public static <S extends MessageSchema<S>> long getWorkingTailPosition(Pipe<S> pipe) {
        return PaddedLong.get(pipe.slabRingTail.workingTailPos);
    }

    /**
     * Direct access to set the working tail position
     * @param pipe Pipe target
     * @param value long new working tail position.
     */
    public static <S extends MessageSchema<S>> void setWorkingTailPosition(Pipe<S> pipe, long value) {
        PaddedLong.set(pipe.slabRingTail.workingTailPos, value);
    }

    /**
     * Direct access to increment working tail position
     * @param pipe Pipe target
     * @param inc int step to increment working tail
     * @return long new working tail position after increment addition
     */
    public static <S extends MessageSchema<S>> long addAndGetWorkingTail(Pipe<S> pipe, int inc) {
        return PaddedLong.add(pipe.slabRingTail.workingTailPos, inc);
    }

    /**
     * Replicate slab and blob data from an older fragment inside this same pipe up to the end.
     * This may be used to re-send a message without having to build it another time.
     * 
     * @param pipe Pipe source
     * @param historicSlabPosition long position
     * @param historicBlobPosition int position
     * @return boolean true if this replication was done
     */
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
		Pipe.addAndGetBlobWorkingHeadPosition(targetPipe, blobMsgSize);
		
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
		assert(Pipe.contentRemaining(pipe)<=pipe.sizeOfSlabRing) : "distance between tail and head must not be larger than the ring, internal error. "+pipe;
	}

	/**
	 * Direct access to the published tail position
	 * @param pipe Pipe target
	 * @return long tail position
	 */
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
		assert(Pipe.contentRemaining(pipe)<=pipe.sizeOfSlabRing) : "distance between tail and head must not be larger than the ring, internal error. "+pipe;
	}
    
	/**
	 * Direct access to publish working tail position
	 * @param pipe Pipe target
	 * @param blobWorkingTailPos int new working tail position
	 */
	public static <S extends MessageSchema<S>> void publishBlobWorkingTailPosition(Pipe<S> pipe, int blobWorkingTailPos) {
        pipe.blobRingTail.bytesTailPos.value = (pipe.blobRingTail.byteWorkingTailPos.value = blobWorkingTailPos);
    }
	
	@Deprecated
	public static <S extends MessageSchema<S>> int primarySize(Pipe<S> pipe) {
		return pipe.sizeOfSlabRing;
	}

	/**
	 * Field reference offset manager for the schema used by this pipe.
	 * 
	 * This data structure contains all the arrays generated by the schema XML and allows for 
	 * deep access to schema details.
	 * 
	 * @param pipe Pipe target
	 * @return FieldReferenceOffsetManager aka FROM
	 */
	public static <S extends MessageSchema<S>> FieldReferenceOffsetManager from(Pipe<S> pipe) {
		assert(pipe.schema!=null);	
		return pipe.schema.from;
	}

	static <S extends MessageSchema<S>> int writeTrailingCountOfBytesConsumed(Pipe<S> pipe, final long pos) {

		final int consumed = computeCountOfBytesConsumed(pipe);

		//used by both pipe and pipe writer so ideal place to count fragments
		pipe.totalWrittenFragments++;		
		
		pipe.slabRing[pipe.slabMask & (int)pos] = consumed;
		pipe.blobWriteLastConsumedPos = pipe.blobRingHead.byteWorkingHeadPos.value;
		return consumed;
	}

	/**
	 * Total count of bytes consumed since start of this fragment
	 * @param pipe Pipe pipe
	 * @return int count of bytes
	 */
	public static <S extends MessageSchema<S>> int computeCountOfBytesConsumed(Pipe<S> pipe) {
		int consumed = pipe.blobRingHead.byteWorkingHeadPos.value - pipe.blobWriteLastConsumedPos;	
		
		if (consumed<0) {			
			consumed = (1+consumed)+Integer.MAX_VALUE;			
		}	
		assert(consumed>=0) : "consumed was "+consumed;
		//log.trace("wrote {} bytes consumed to position {}",consumed,pos);
		return consumed;
	}

	/**
	 * Wrapped slab as an IntBuffer
	 * @param pipe Pipe target
	 * @return IntBuffer wrapped slab
	 */
	public static <S extends MessageSchema<S>> IntBuffer wrappedSlabRing(Pipe<S> pipe) {
		return pipe.wrappedSlabRing;
	}

	/**
	 * Wrapped blob as a ByteBuffer 
	 * @param pipe
	 * @return ByteBuffer a
	 */
	public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingA(Pipe<S> pipe) {
		return pipe.wrappedBlobReadingRingA;
	}

	/**
	 * Wrapped blob as a ByteBuffer
	 * @param pipe Pipe target
	 * @return ByteBuffer b
	 */
    public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobRingB(Pipe<S> pipe) {
        return pipe.wrappedBlobReadingRingB;
    }

    /**
     * Wrapped constant buffer as ByteBuffer
     * @param pipe Pipe target
     * @return ByteBuffer b
     */
	public static <S extends MessageSchema<S>> ByteBuffer wrappedBlobConstBuffer(Pipe<S> pipe) {
		return pipe.wrappedBlobConstBuffer;
	}

	/**
	 * get the output stream associated with this pipe
	 * @param pipe Pipe target
	 * @return DataOutputBlobWriter
	 */
	public static <S extends MessageSchema<S>> DataOutputBlobWriter<S> outputStream(Pipe<S> pipe) {
		return pipe.blobWriter;
	}

	/**
	 * get the output stream associated with this pipe and open it
	 * @param pipe Pipe target
	 * @return DataOutputBlobWriter
	 */
	public static <S extends MessageSchema<S>> DataOutputBlobWriter<S> openOutputStream(Pipe<S> pipe) {
		return DataOutputBlobWriter.openField(pipe.blobWriter);
	}
	
	/**
	 * get the input stream associated with this pipe
	 * @param pipe Pipe target
	 * @return DataInputBlobReader
	 */
	public static <S extends MessageSchema<S>> DataInputBlobReader<S> inputStream(Pipe<S> pipe) {
		return pipe.blobReader;
	}
	
	/**
	 * get the input stream associated with this Pipe and open it
	 * @param pipe Pipe target
	 * @return DataInputBlobReader
	 */
	public static <S extends MessageSchema<S>> DataInputBlobReader<S> openInputStream(Pipe<S> pipe) {
		pipe.blobReader.openLowLevelAPIField();
		return pipe.blobReader;
	}
	
	/**
	 * get the input stream but do not move the cursor position.
	 * reads from this stream should not be closed since we do not want to cause side effect.
	 * 
	 * @param pipe Pipe target
	 * @param offset int relative index to where this offset
	 * @return DataInputBlobReader
	 */
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

	
	/**
	 * Checks if outgoing pipe has room for low level fragment write of the given size.
	 * @param pipe Pipe target
	 * @param size int size from as defined in FROM
	 * @return boolean true if there is room
	 */
    public static <S extends MessageSchema<S>> boolean hasRoomForWrite(Pipe<S> pipe, int size) {
    	assert(Pipe.singleThreadPerPipeWrite(pipe.id));
    	//This holds the last known state of the tail position, if its sufficiently far ahead it indicates that
    	//we do not need to fetch it again and this reduces contention on the CAS with the reader.
    	//This is an important performance feature of the low level API and should not be modified.
 
        return roomToLowLevelWrite(pipe, pipe.llRead.llwConfirmedPosition+size);
    }
    
    /**
     * Check if there is room to write the largest known message. If not spin until there is room.
     * This method will log a warning if there was no room.
     * Only use this method if you have already checked that there is room for the write and treat this
     * call as a special kind of assert which will confirm that room is always checked for.
     * 
     * @param pipe Pipe target
     */
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

	/**
	 * Confirm low level write of fragment of the provided size.  This MUST be called after writing every
	 * fragment in order to move the cursor forward to write the next and update the internal bookkeping with
	 * the number of bytes written.
	 * 
	 * @param output Pipe target
	 * @param size int size of fragment as defined in FROM, also returned when addMsgIdx is called
	 * @return long new position
	 */
	public static <S extends MessageSchema<S>> long confirmLowLevelWrite(Pipe<S> output, int size) { 
	 
		assert(Pipe.singleThreadPerPipeWrite(output.id));
	    assert(size>=0) : "unsupported size "+size;
	    
	    assert((output.llRead.llwConfirmedPosition+output.slabMask) <= Pipe.workingHeadPosition(output)) : " confirmed writes must be less than working head position writes:"
	                                                +(output.llRead.llwConfirmedPosition+output.slabMask)+" workingHead:"+Pipe.workingHeadPosition(output)+
	                                                " \n CHECK that Pipe is written same fields as message defines and skips none!";

	    return  output.llRead.llwConfirmedPosition += size;

	}

	/**
	 * Method used for moving more than one fragment at a time. Any size value will be acceptable
	 * @param output Pipe target
	 * @param size int size of fragment as defined in FROM, also returned when addMsgIdx is called
	 * @return long new position
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
		
		try {
			assert(Pipe.sizeOf(output, output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition]) == size) : 
				"Did not write the same size fragment as expected, double check message. expected:"
					+Pipe.sizeOf(output, output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition])
					+" but was passed "+size+" for schema "+Pipe.schemaName(output)
					+" and assumed MsgId of "+output.slabRing[output.slabMask&(int)output.llRead.llwConfirmedPosition];
		} catch (ArrayIndexOutOfBoundsException aiex) {
			//ignore, caused by some poor unit tests which need to be re-written.
		}
		return true;
	}

	/**
	 * Confirm low level write, Must be called after every fragment write to move cursor to next fragment to write	
	 * @param output
	 * @return long position
	 */
	public static <S extends MessageSchema<S>> long confirmLowLevelWrite(Pipe<S> output) { 
		//helper method always uses the right size but that value needs to be found so its a bit slower than if you already knew the size and passed it in
		 
		assert(Pipe.singleThreadPerPipeWrite(output.id));
	    assert((output.llRead.llwConfirmedPosition+output.slabMask) <= Pipe.workingHeadPosition(output)) : " confirmed writes must be less than working head position writes:"
	                                                +(output.llRead.llwConfirmedPosition+output.slabMask)+" workingHead:"+Pipe.workingHeadPosition(output)+
	                                                " \n CHECK that Pipe is written same fields as message defines and skips none!";
	   
	    return  output.llRead.llwConfirmedPosition += Pipe.sizeOf(output, output.slabRing[lastConfirmedWritePosition(output)]);

	}

	/**
	 * get last confirmed write position in the slab
	 * @param output Pipe target
	 * @return int slab position
	 */
	public static <S extends MessageSchema<S>> int lastConfirmedWritePosition(Pipe<S> output) {
		return output.slabMask&(int)output.llRead.llwConfirmedPosition;
	}

    /**
     * Low level API only for checking if there is content to read of the size provided.
     * @param pipe Pipe source
     * @param size int message size looking for
     * @return boolean true if there is content
     */
	public static <S extends MessageSchema<S>> boolean hasContentToRead(Pipe<S> pipe, int size) {
		//do not use with high level API, is dependent on low level confirm calls.
		assert(Pipe.singleThreadPerPipeRead(pipe.id));
        //optimized for the other method without size. this is why the -1 is there and we use > for target comparison.
        return contentToLowLevelRead2(pipe, pipe.llWrite.llrConfirmedPosition+size-1, pipe.llWrite); 
    }

    /**
     * Checks specified pipe to see if there is any data to read.
     * NOTE: only works with low-level API.
     * 
     * @param pipe pipe to be examined
     * @param <S> MessageSchema to extend
     * @return <code>true</code> if there is data to read else <code>false</code>
     */
    public static <S extends MessageSchema<S>> boolean hasContentToRead(Pipe<S> pipe) {
    	//this method can only be used with low level api navigation loop
    	//CAUTION: THIS IS NOT COMPATIBLE WITH PipeReader behavior...
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

	/**
	 * Confirm low level read of message of this size, Must be called after read to move cursor forward for next read.
	 * @param pipe Pipe target
	 * @param size long size as defined in FROM
	 * @return long new position
	 */
	public static <S extends MessageSchema<S>> long confirmLowLevelRead(Pipe<S> pipe, long size) {
	    assert(size>0) : "Must have read something.";
	    assert(Pipe.singleThreadPerPipeRead(pipe.id));
	     //not sure if this assert is true in all cases
	  //  assert(input.llWrite.llwConfirmedWrittenPosition + size <= input.slabRingHead.workingHeadPos.value+Pipe.EOF_SIZE) : "size was far too large, past known data";
	  //  assert(input.llWrite.llwConfirmedWrittenPosition + size >= input.slabRingTail.tailPos.get()) : "size was too small, under known data";   
		return (pipe.llWrite.llrConfirmedPosition += size);
	}

	/**
	 * Direct access to get blob tail position.
	 * @param pipe Pipe source
	 * @return int tail position
	 */
    public static <S extends MessageSchema<S>> int getBlobTailPosition(Pipe<S> pipe) {
	    return PaddedInt.get(pipe.blobRingTail.bytesTailPos);
	}

    @Deprecated
	public static <S extends MessageSchema<S>> int getBlobRingTailPosition(Pipe<S> pipe) {
	    return getBlobTailPosition(pipe);
	}

    /**
     * Direct access to set blob tail position
     * @param pipe Pipe target
     * @param value int tail position
     */
	public static <S extends MessageSchema<S>> void setBlobTailPosition(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingTail.bytesTailPos, value);
    }
	
	@Deprecated
	public static <S extends MessageSchema<S>> void setBytesTail(Pipe<S> pipe, int value) {
        setBlobTailPosition(pipe,value);
    }

	/**
	 * Direct access to read blob head position.
	 * @param pipe Pipe target
	 * @return int head position
	 */
    public static <S extends MessageSchema<S>> int getBlobHeadPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingHead.bytesHeadPos);        
    }
	
    /**
     * Direct access to set blob head position
     * @param pipe Pipe target
     * @param value int new position
     */
    public static <S extends MessageSchema<S>> void setBlobHeadPosition(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingHead.bytesHeadPos, value);
    }

    /**
     * Direct access to add and get Blob head position
     * @param pipe Pipe target
     * @param inc value to increment
     * @return int position
     */
    public static <S extends MessageSchema<S>> int addAndGetBlobHeadPosition(Pipe<S> pipe, int inc) {
        return PaddedInt.add(pipe.blobRingHead.bytesHeadPos, inc);
    }

    /**
     * Direct access to working blob tail position
     * @param pipe Pipe target
     * @return new tail position
     */
    public static <S extends MessageSchema<S>> int getWorkingBlobTailPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingTail.byteWorkingTailPos);
    }

    /**
     * Direct access to add and get working tail position
     * @param pipe Pipe target
     * @param inc value to add
     * @return int position after add
     */
    public static <S extends MessageSchema<S>> int addAndGetBlobWorkingTailPosition(Pipe<S> pipe, int inc) {
        return PaddedInt.maskedAdd(pipe.blobRingTail.byteWorkingTailPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    /**
     * Direct method to set the blob working tail.
     * @param pipe Pipe target
     * @param value int working tail position
     */
    public static <S extends MessageSchema<S>> void setBlobWorkingTail(Pipe<S> pipe, int value) {
        PaddedInt.set(pipe.blobRingTail.byteWorkingTailPos, value);
    }
    
    /**
     * Direct method to get the working blob head position
     * @param pipe Pipe source
     * @return int working blob head position
     */
    public static <S extends MessageSchema<S>> int getWorkingBlobHeadPosition(Pipe<S> pipe) {
        return PaddedInt.get(pipe.blobRingHead.byteWorkingHeadPos);
    }

    @Deprecated
    public static <S extends MessageSchema<S>> int getBlobWorkingHeadPosition(Pipe<S> pipe) {
        return getWorkingBlobHeadPosition(pipe);
    }
    
    /**
     * Direct access method for incrementing blob working head position
     * @param pipe Pipe target
     * @param inc step to add
     * @return new working head position
     */
    public static <S extends MessageSchema<S>> int addAndGetBlobWorkingHeadPosition(Pipe<S> pipe, int inc) {
    	assert(inc>=0) : "only zero or positive values supported found "+inc;
        return PaddedInt.maskedAdd(pipe.blobRingHead.byteWorkingHeadPos, inc, Pipe.BYTES_WRAP_MASK);
    }

    /**
     * Direct access method for setting blob working head position
     * @param pipe Pipe target
     * @param value int working head position
     */
    public static <S extends MessageSchema<S>> void setBlobWorkingHead(Pipe<S> pipe, int value) {
    	assert(value>=0) : "working head must be positive";
        PaddedInt.set(pipe.blobRingHead.byteWorkingHeadPos, value);
    }

    /**
     * decrement batch release count down
     * @param pipe Pipe target
     * @return int batch release count
     */
    public static <S extends MessageSchema<S>> int decBatchRelease(Pipe<S> pipe) {
        return --pipe.batchReleaseCountDown;
    }

    /**
     * decrement batch publish count down
     * @param pipe Pipe target
     * @return int batch publish count
     */
    public static <S extends MessageSchema<S>> int decBatchPublish(Pipe<S> pipe) {
        return --pipe.batchPublishCountDown;
    }

    /**
     * start new batch for release
     * @param pipe Pipe target
     */
    public static <S extends MessageSchema<S>> void beginNewReleaseBatch(Pipe<S> pipe) {
        pipe.batchReleaseCountDown = pipe.batchReleaseCountDownInit;
    }

    /**
     * start new batch for publish
     * @param pipe Pipe target
     */
    public static <S extends MessageSchema<S>> void beginNewPublishBatch(Pipe<S> pipe) {
        pipe.batchPublishCountDown = pipe.batchPublishCountDownInit;
    }

    /**
     * Direct access to blob backing array.
     * @param pipe Pipe source
     * @return byte[] backing
     */
    public static <S extends MessageSchema<S>> byte[] blob(Pipe<S> pipe) {        
        return pipe.blobRing;
    }
    
    /**
     * Direct access to slab backing array
     * @param pipe Pipe source
     * @return int[] backing
     */
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

    static <S extends MessageSchema<S>> void updateBytesWriteLastConsumedPos(Pipe<S> pipe) {
        pipe.blobWriteLastConsumedPos = Pipe.getWorkingBlobHeadPosition(pipe);
    }

    /**
     * Direct access to working tail position object
     * @param pipe Pipe source
     * @return PaddedLong working tail position object
     */
    public static <S extends MessageSchema<S>> PaddedLong getWorkingTailPositionObject(Pipe<S> pipe) {
        return pipe.slabRingTail.workingTailPos;
    }

    /**
     * Direct access to working head position object
     * @param pipe Pipe source
     * @return PaddedLong working head position object
     */
    public static <S extends MessageSchema<S>> PaddedLong getWorkingHeadPositionObject(Pipe<S> pipe) {
        return pipe.slabRingHead.workingHeadPos;
    }

    /**
     * returns size of this message as defined in the FROM and schema in this pipe.
     * @param pipe Pipe source
     * @param msgIdx int message idx
     * @return int size of message as defined by FROM
     */
    public static <S extends MessageSchema<S>> int sizeOf(Pipe<S> pipe, int msgIdx) {
    	return sizeOf(pipe.schema, msgIdx);
    }
    
    /**
     * returns size of this message as defined in the provided schema.
     * @param schema MessageSchema holding the FROM
     * @param msgIdx int message idx
     * @return int size of message as defined by FROM
     */
    public static <S extends MessageSchema<S>> int sizeOf(S schema, int msgIdx) {
        return msgIdx>=0 ? schema.from.fragDataSize[msgIdx] : Pipe.EOF_SIZE;
    }

    /**
     * Release for write the old fragment read which are pending release. eg not previously released
     * @param pipe Pipe target
     */
    public static <S extends MessageSchema<S>> void releasePendingReadLock(Pipe<S> pipe) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
        PendingReleaseData.releasePendingReadRelease(pipe.pendingReleases, pipe);
    }
    
    /**
     * Release for write the old fragments which total up to the provided consumed number of bytes.
     * @param pipe Pipe target
     * @param consumed int bytes consumed so far allowing these fragments to be released.
     */
    public static <S extends MessageSchema<S>> void releasePendingAsReadLock(Pipe<S> pipe, int consumed) {
    	assert(Pipe.singleThreadPerPipeRead(pipe.id));
    	PendingReleaseData.releasePendingAsReadRelease(pipe.pendingReleases, pipe, consumed);
    }
    
    /**
     * Count of how many fragments are pending release.
     * @param pipe Pipe source
     * @return int count of fragments
     */
    public static <S extends MessageSchema<S>> int releasePendingCount(Pipe<S> pipe) {
    	return PendingReleaseData.pendingReleaseCount(pipe.pendingReleases);
    }
    
    /**
     * Count of how many bytes are pending in the total of all pending release fragments
     * @param pipe
     * @return int count of bytes
     */
    public static <S extends MessageSchema<S>> int releasePendingByteCount(Pipe<S> pipe) {
     	return PendingReleaseData.pendingReleaseByteCount(pipe.pendingReleases);
    }
    
    /**
     * Release all pending fragments for release
     * @param pipe
     */
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
        Pipe.setBlobWorkingHead(pipe, pipe.markedHeadBlob);
        //must always be false when we return to the beginning of the fragment
        pipe.isInBlobFieldWrite.set(false);
    }
    
    /**
     * Hold this position in case we want to re-read this single message
     * @param pipe
     */
    public static void markTail(Pipe pipe) {
        pipe.markedTailSlab = Pipe.getWorkingTailPosition(pipe);
        pipe.markedTailBlob = Pipe.getWorkingBlobTailPosition(pipe);
    }
    
    /**
     * abandon what has been read and move back to top of fragment to read again.
     * MUST be called before confirm of read and never after
     * @param pipe
     */
    public static void resetTail(Pipe pipe) {
        Pipe.setWorkingTailPosition(pipe, pipe.markedTailSlab);
        Pipe.setBlobWorkingTail(pipe, pipe.markedTailBlob);
    }
    
    /**
     * Store working head position for use later when field position is written
     * @param target
     * @return int new working head position
     */
	public static int storeBlobWorkingHeadPosition(Pipe<?> target) {
		assert(-1 == target.activeBlobHead) : "can not store second until first is resolved";
		return target.activeBlobHead = Pipe.getWorkingBlobHeadPosition(target);				
	}
    
	/**
	 * Clear the stored blob working head position since we are writing it to the slab.
	 * 
	 * @param target
	 * @return int returns old position before it was cleared
	 */
	public static int unstoreBlobWorkingHeadPosition(Pipe<?> target) {
		assert(-1 != target.activeBlobHead) : "can not unstore value not saved";
		int result = target.activeBlobHead;
		target.activeBlobHead = -1;
		return result;
	}

	public void markConsumerPassDone() {
		
		long thisPass = totalWrittenFragments-lastFragmentCount;
		if (thisPass>0) {
			RunningStdDev.sample(fragsPerPass, thisPass);
			lastFragmentCount = totalWrittenFragments;
		}
	}


}
