package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;

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
    
    //TODO: A, change interfaces to use reactive streams design
    
    //TODO: C, load shedding features,  Writer may decide to back up instead of release the headPos if this messsage is "less imporant", Reader may decide to "route" message to null or low priority ring buffer.
    //TODO: C, all overload situations must communicate the specific problem cause back out as far as possible
    //TODO: C, dev ops tool to empty (drain) buffers and record the loss.
    

    public static class PaddedLong {
        public long value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    public static class PaddedInt {
        public int value = 0, padding1, padding2, padding3, padding4, padding5, padding6, padding7;
    }
    
    public final int[] buffer;
    public final int mask;
    public final PaddedLong workingHeadPos = new PaddedLong();
    public final PaddedLong workingTailPos = new PaddedLong();

    public final AtomicLong tailPos = new PaddedAtomicLong(); // producer is allowed to write up to tailPos
    public final AtomicLong headPos = new PaddedAtomicLong(); // consumer is allowed to read up to headPos
    
    
    //grouping fragments together gives a clear advantage but more latency
    //as this gets near half the primary bits size the performance drops off
    //tuning values here can greatly help throughput but should remain <= 1/3 of primary bits
    public final int chunkBits = 4; 
    public final int chunkMask = (1<<chunkBits)-1;
    
    public final int maxSize;

    final int maxByteSize;
    public final int byteMask;
    public final byte[] byteBuffer;
    public PaddedInt addBytePos = new PaddedInt();//TODO: remove from tail byte position is missing.
    
    //defined externally and never changes
    final byte[] constByteBuffer;
    final byte[][] bufferLookup;

    //TODO: A, X use stack of fragment start offsets for each fragment until full message is completed.
    //TODO: B, first offset 0 points to the constants after the ring buffer.
    
    //Need to know when the new template starts
    //each fragment size must be known and looked up
    public FieldReferenceOffsetManager from;
    
    // 0 - constants?
    // 1 - start of fragment
    // 2 - sequence fragment
    
    
    final int[] activeFragmentStack;
    int   activeFragmentStackHead = 0;
               
    // end of moveNextFields

    static final int JUMP_MASK = 0xFFFFF;
    public final FASTRingBufferConsumer consumerData;
    
    

    public FASTRingBuffer(byte primaryBits, byte byteBits,
    		              byte[] byteConstants, FieldReferenceOffsetManager from) {
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
        
        this.from = from;
        this.activeFragmentStack = new int[from.maximumFragmentStackDepth];
                
        this.consumerData = new FASTRingBufferConsumer(-1, false, false, -1, -1,
                                                        -1, 0, new int[from.maximumFragmentStackDepth], -1, -1, from, mask);
    }

    

    //TODO: B, must add way of selecting what field to skip writing for the consumer.
    
    /**
     * Empty and restore to original values.
     */
    public void reset() {
        workingHeadPos.value = 0;
        workingTailPos.value = 0;
        tailPos.set(0);
        headPos.set(0); //System.err.println("reset()");
        addBytePos.value = 0;
        
        FASTRingBufferConsumer.reset(consumerData);
        

    }
    
    public static boolean canMoveNext(FASTRingBuffer ringBuffer) { 
        FASTRingBufferConsumer ringBufferConsumer = ringBuffer.consumerData; 
        
        //check if we are only waiting for the ring buffer to clear
        if (ringBufferConsumer.waiting) {
 //       	System.err.println("waiting");
            //only here if we already checked headPos against moveNextStop at least once and failed.
            
            ringBufferConsumer.setBnmHeadPosCache(ringBuffer.headPos.longValue());
            ringBufferConsumer.waiting = (ringBufferConsumer.getWaitingNextStop()>(ringBufferConsumer.getBnmHeadPosCache() ));
            return !(ringBufferConsumer.waiting);
        }
             
        //finished reading the previous fragment so move the working tail position forward for next fragment to read
    //    System.err.println("xx"+ringBuffer.workingTailPos.value+"  "+ringBufferConsumer.activeFragmentDataSize);
        final long cashWorkingTailPos = ringBuffer.workingTailPos.value +  ringBufferConsumer.activeFragmentDataSize;
        ringBuffer.workingTailPos.value = cashWorkingTailPos;
        ringBufferConsumer.activeFragmentDataSize = (0);
        
        
        if (ringBufferConsumer.getMessageId()<0) {     
        //	System.err.println("aaa");
            return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
        } else {
        //	System.err.println("bbb");
            return beginFragment(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
        }
        
    }

    ///TODO: B, add optional groups to this implementation

    private static boolean beginFragment(FASTRingBuffer ringBuffer, FASTRingBufferConsumer ringBufferConsumer, final long cashWorkingTailPos) {
        ringBufferConsumer.setNewMessage(false);
        int lastCursor = ringBufferConsumer.cursor;
        int fragStep = ringBufferConsumer.from.fragScriptSize[lastCursor]; //script jump 
        ringBufferConsumer.cursor = (ringBufferConsumer.cursor + fragStep);

        
        //////////////
        ////Never call these when we jump back for loop
        //////////////
        if (sequenceLengthDetector(ringBuffer, fragStep, ringBufferConsumer)) {
            //detecting end of message
            int token;//do not set before cursor is checked to ensure it is not after the script length
            if ((ringBufferConsumer.cursor>=ringBufferConsumer.from.tokensLen) ||
                    ((((token = ringBufferConsumer.from.tokens[ringBufferConsumer.cursor]) >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)==TypeMask.Group &&
                    0==(token & (OperatorMask.Group_Bit_Seq<< TokenBuilder.SHIFT_OPER)) && //TODO: B, would be much better with end of MSG bit
                    0!=(token & (OperatorMask.Group_Bit_Close<< TokenBuilder.SHIFT_OPER)))) {
                
                return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);

            }
        }
        
        //save the index into these fragments so the reader will be able to find them.
        //if this fragment is not the same as the last must increment on the stack
        if (lastCursor != ringBufferConsumer.cursor) {
            ringBuffer.activeFragmentStack[++ringBuffer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
        } else {
            ringBuffer.activeFragmentStack[ringBuffer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
        }
        
        
        return checkForContent(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
    }

    private static boolean checkForContent(FASTRingBuffer ringBuffer, FASTRingBufferConsumer ringBufferConsumer, long cashWorkingTailPos) {
        //after alignment with front of fragment, may be zero because we need to find the next message?
        ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]);//save the size of this new fragment we are about to read
        
        //do not let client read fragment if it is not fully in the ring buffer.
    //    new Exception(cashWorkingTailPos+"  "+ ringBufferConsumer.activeFragmentDataSize).printStackTrace();
        ringBufferConsumer.setWaitingNextStop(cashWorkingTailPos+ringBufferConsumer.activeFragmentDataSize);
        
        //
        if (ringBufferConsumer.getWaitingNextStop()>ringBufferConsumer.getBnmHeadPosCache()) {
            ringBufferConsumer.setBnmHeadPosCache(ringBuffer.headPos.longValue());
            if (ringBufferConsumer.getWaitingNextStop()>ringBufferConsumer.getBnmHeadPosCache()) {
                ringBufferConsumer.waiting = (true);
                return false;
            }
        }
                        
        return true;
    }

    
    //TODO: C, need to get messageId when its the only message and so not written to the ring buffer.
    //TODO: C, need to step over the preamble? but how?
    
    private static boolean beginNewMessage(FASTRingBuffer ringBuffer, FASTRingBufferConsumer ringBufferConsumer, long cashWorkingTailPos) {
        
        //if we can not start to read the next message because it does not have the template id yet
        //then fail fast and do not move the tailPos yet until we know it can be read        
        long needStop = cashWorkingTailPos + 1; //NOTE: do not make this bigger or hangs are likely
        if (needStop>=ringBufferConsumer.getBnmHeadPosCache() ) {  
            ringBufferConsumer.setBnmHeadPosCache(ringBuffer.headPos.longValue());
            if (needStop>=ringBufferConsumer.getBnmHeadPosCache()) {
                ringBufferConsumer.setMessageId(-1);
              return false; 
            }
        }
              
        //keep the queue fill size for Little's law 
        //also need to keep messages per second data
        FASTRingBufferConsumer.recordRates(ringBufferConsumer, needStop);
        
        //Start new stack of fragments because this is a new message
        ringBuffer.activeFragmentStackHead = 0;
        ringBuffer.activeFragmentStack[ringBuffer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
        
        
        //Now beginning a new message so release the previous one from the ring buffer
        //This is the only safe place to do this and it must be done before we check for space needed by the next record.
        ringBuffer.tailPos.lazySet(cashWorkingTailPos); 
               
        ringBufferConsumer.setMessageId(FASTRingBufferReader.readInt(ringBuffer,  ringBufferConsumer.from.templateOffset)); //jumps over preamble to find templateId
   //     System.err.println("messageId :"+ringBufferConsumer.getMessageId());
        //start new message, can not be seq or optional group or end of message.
        ringBufferConsumer.cursor = (ringBufferConsumer.from.starts[ringBufferConsumer.getMessageId()]);
        ringBufferConsumer.setNewMessage(true);
        
        //////
        ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]);//save the size of this new fragment we are about to read
        return true;
       // return checkForContent(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
    }
    
    //only called after moving forward.
    private static boolean sequenceLengthDetector(FASTRingBuffer ringBuffer, int jumpSize, FASTRingBufferConsumer consumerData) {
        if(0==consumerData.cursor) {
            return false;
        }
        int endingToken = consumerData.from.tokens[consumerData.cursor-1];
        
        //if last token of last fragment was length then begin new sequence
        int type = (endingToken >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
        if (TypeMask.GroupLength == type) {
            int seqLength = FASTRingBufferReader.readInt(ringBuffer, -1); //length is always at the end of the fragment.
            if (seqLength == 0) {
//                int jump = (TokenBuilder.MAX_INSTANCE&from.tokens[cursor-jumpSize])+2;
                int fragJump = consumerData.from.fragScriptSize[consumerData.cursor+1]; //script jump  //TODO: not sure this is right whenthey are nested?
//                System.err.println(jump+" vs "+fragJump);
         //       System.err.println("******************** jump over seq");
                //TODO: B, need to build a test case, this does not appear in the current test data.
                //do nothing and jump over the sequence
                //there is no data in the ring buffer so do not adjust position
                consumerData.cursor = (consumerData.cursor + (fragJump&JUMP_MASK));
                //done so move to the next item
                
                return true;
            } else {
                assert(seqLength>=0) : "The previous fragment has already been replaced or modified and it was needed for the length counter";
                consumerData.getSeqStack()[consumerData.incSeqStackHead()]=seqLength;
                //this is the first run so we are already positioned at the top   
            }
            return false;   
            
        }
                
        
        //if last token of last fragment was seq close then subtract and move back.
        if (TypeMask.Group==type && 
            0 != (endingToken & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)) &&
            0 != (endingToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))            
                ) {
            //check top of the stack
            if (--consumerData.getSeqStack()[consumerData.getSeqStackHead()]>0) {
                int jump = (TokenBuilder.MAX_INSTANCE&endingToken)+1;
               consumerData.cursor = (consumerData.cursor - jump);
               return false;
            } else {
                //done, already positioned to continue
                consumerData.setSeqStackHead(consumerData.getSeqStackHead() - 1);                
                return true;
            }
        }                   
        return true;
    }
    
  

    // TODO: C, add map method which can take data from one ring buffer and
    // populate another.

    // TODO: C, Promises/Futures/Listeners as possible better fit to stream
    // processing?
    // TODO: C, look at adding reduce method in addition to filter.


    public static int peek(int[] buf, long pos, int mask) {
        return buf[mask & (int)pos];
    }

    public static long peekLong(int[] buf, long pos, int mask) {
        
        return (((long) buf[mask & (int)pos]) << 32) | (((long) buf[mask & (int)(pos + 1)]) & 0xFFFFFFFFl);

    }

    // TODO: Z, add consumer/Iterator to go from ring buffer to Object stream
    // TODO: Z, Map templates to methods for RMI of void methods(eg. one direction).
    // TODO: Z, add map toIterator method for consuming ring buffer by java8 streams.

    public static void addLocalHeapValue(int heapId, int sourceLen, int rbMask, int[] rbB, PaddedLong rbPos, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        //int rbMask, int[] rbB  PaddedLong rbPos
        final int p = rbRingBuffer.addBytePos.value;
        if (sourceLen > 0) {
            rbRingBuffer.addBytePos.value = LocalHeap.copyToRingBuffer(heapId, rbRingBuffer.byteBuffer, p, rbRingBuffer.byteMask, byteHeap);
        }
        addValue(rbB, rbMask, rbPos, p);
        addValue(rbB, rbMask, rbPos, sourceLen);
    }

    public static void addByteArray(byte[] source, int sourceIdx, int sourceLen, FASTRingBuffer rbRingBuffer) {
        final int p = rbRingBuffer.addBytePos.value;
        if (sourceLen > 0) {
            int targetMask = rbRingBuffer.byteMask;
            LocalHeap.copyToRingBuffer(rbRingBuffer.byteBuffer, p, targetMask, sourceIdx, sourceLen, source);
            rbRingBuffer.addBytePos.value = p + sourceLen;
        }
        addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, p);
        addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingHeadPos, sourceLen);
    }
    
    

    // TODO: D, Callback interface for setting the offsets used by the clients, Generate list of FieldId static offsets for use by static reader based on templateId.
 



    public void removeForward2(long pos) {
        workingTailPos.value = pos;
        tailPos.lazySet(pos);
    }

    //TODO: B: (optimization)finish the field lookup so the constants need not be written to the loop! 
    //TODO: B: build custom add value for long and decimals to avoid second ref out to pos.value
    //TODO: X, back off write if with in cache line distance of tail (full queue case)

   
    //we are only allowed 12% of the time or so for doing this write.
    //this pushes only ~5gbs but if we had 100% it would scale to 45gbs
    //so this is not the real bottleneck and given the compression ratio of the test data
    //we can push 1gbs more of compressed data for each 10% of cpu freed up.
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value) {
        buffer[rbMask & (int)headCache.value++] = value;
    } 
    
    //long p = headCache.value; //TODO: code gen may want to replace this
    // buffer[rbMask & (int)p] = value; //TODO: code gen replace rbMask with constant may help remove check
    //  headCache.value = p+1;
    
    
    public static void addValue(int[] buffer, int rbMask, PaddedLong headCache, int value1, int value2) {
        
        long p = headCache.value; 
        buffer[rbMask & (int)p] = value1; //TODO: code gen replace rbMask with constant may help remove check
        buffer[rbMask & (int)(p+1)] = value2; //TODO: code gen replace rbMask with constant may help remove check
        headCache.value = p+2;
        
    } 
    
    
    // fragment is ready for consumption
    public static final void unBlockFragment(AtomicLong head, PaddedLong headCache) {
     
     //   assert(headCache.value>head.get()) : "Can not set the cache smaller than head";        
        head.lazySet(headCache.value);
    }
    
    //TODO: X, Will want to add local cache of atomic in unBlock in order to not lazy set twice because it is called for every close.
    //Called once for every group close, even when nested
    //TODO: B, write padding message if this unblock is the only fragment in the queue.
    
    
    public static void dump(FASTRingBuffer rb) {
                       
        // move the removePosition up to the addPosition
        // new Exception("WARNING THIS IS NO LONGER COMPATIBLE WITH PUMP CALLS").printStackTrace();
        rb.tailPos.lazySet(rb.workingTailPos.value = rb.workingHeadPos.value);
    }

    // WARNING: consumer of these may need to loop around end of buffer !!
    // these are needed for fast direct READ FROM here

    public static int readRingByteLen(int fieldPos, int[] rbB, int rbMask, PaddedLong rbPos) {
        return rbB[rbMask & (int)(rbPos.value + fieldPos + 1)];// second int is always the length
    }
    
    public static int readRingBytePosition(int rawPos) {
        return rawPos&0x7FFFFFFF;//may be negative when it is a constant but lower bits are always position
    }    

    public static byte[] readRingByteBuffers(int rawPos, FASTRingBuffer rbRingBuffer) {
        return rbRingBuffer.bufferLookup[rawPos>>>31];
    }

    public static int readRingByteRawPos(int fieldPos, int[] rbB, int rbMask, PaddedLong rbPos) {
        return rbB[(int)(rbMask & (rbPos.value + fieldPos))];
    }
    

    public int readRingByteMask() {
        return byteMask;
    }

//    public final int availableCapacity() {
//        return maxSize - (int)(headPos.longValue() - tailPos.longValue());
//    }

    
    public static int contentRemaining(FASTRingBuffer rb) {
        return (int)(rb.headPos.longValue() - rb.tailPos.longValue()); //must not go past add count because it is not release yet.
    }

    public int fragmentSteps() {
        return from.fragScriptSize[consumerData.cursor];
    }

    public static void publishWrites(AtomicLong hp, PaddedLong wrkHdPos) {
        hp.lazySet(wrkHdPos.value);
    }
    public static void abandonWrites(AtomicLong hp, PaddedLong wrkHdPos) {    
        //ignore the fact that any of this was written to the ring buffer
        wrkHdPos.value = hp.longValue();
    }
    

    //TODO: B, Double check that asserts have been removed because they bump up the byte code size and prevent inline
    
    public static long spinBlock(AtomicLong atomicLong, long lastCheckedValue, long targetValue) {
        do {
            lastCheckedValue = atomicLong.longValue();
        } while ( lastCheckedValue < targetValue);
        return lastCheckedValue;
    }



}
