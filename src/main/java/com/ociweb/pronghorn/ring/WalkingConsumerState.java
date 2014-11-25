package com.ociweb.pronghorn.ring;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.pronghorn.ring.util.Histogram;

public class WalkingConsumerState {
    private int msgIdx=-1;
    private boolean isNewMessage;
    public boolean waiting;
    private long waitingNextStop;
    private long bnmHeadPosCache;
    public int cursor;
    public int activeFragmentDataSize;
    private int[] seqStack;
    private int seqStackHead;
    public long tailCache;
    public final FieldReferenceOffsetManager from;
    
    public final Histogram queueFill;
    public final Histogram timeBetween;
    //keep the queue fill size for Little's law 
    //MeanResponseTime = MeanNumberInSystem / MeanThroughput
    private long lastTime = 0;
    private final int rateAvgBit = 5;
    private final int rateAvgCntInit = 1<<rateAvgBit;
    private int rateAvgCnt = rateAvgCntInit;
	final int[] activeFragmentStack;
	int   activeFragmentStackHead = 0;
    
	
	public WalkingConsumerState(int mask, FieldReferenceOffsetManager from) {
		this(-1, false, false, -1, -1, -1, 0, new int[from.maximumFragmentStackDepth], -1, -1, from, mask);
	}
	
	
    private WalkingConsumerState(int messageId, boolean isNewMessage, boolean waiting, long waitingNextStop,
                                    long bnmHeadPosCache, int cursor, int activeFragmentDataSize, int[] seqStack, int seqStackHead,
                                    long tailCache, FieldReferenceOffsetManager from, int rbMask) {
    	if (null==from) {
    		throw new UnsupportedOperationException();
    	}
        this.msgIdx = messageId;
        this.isNewMessage = isNewMessage;
        this.waiting = waiting;
        this.waitingNextStop = waitingNextStop;
        this.bnmHeadPosCache = bnmHeadPosCache;
        this.cursor = cursor;
        this.activeFragmentDataSize = activeFragmentDataSize;
        this.seqStack = seqStack;
        this.seqStackHead = seqStackHead;
        this.tailCache = tailCache;
        this.from = from;
        this.queueFill  =  new Histogram(10000, rbMask>>1, 0, rbMask+1);
        this.timeBetween = new Histogram(10000, 20, 0, 10000000000l);
        this.activeFragmentStack = new int[from.maximumFragmentStackDepth];
        
    }

    public static void recordRates(WalkingConsumerState ringBufferConsumer, long newTailPos) {
        
        //MeanResponseTime = MeanNumberInSystem / MeanThroughput
                
        if ((--ringBufferConsumer.rateAvgCnt)<0) {
        
            Histogram.sample(ringBufferConsumer.bnmHeadPosCache - newTailPos, ringBufferConsumer.queueFill);
            long now = System.nanoTime();
            if (ringBufferConsumer.lastTime>0) {
                Histogram.sample(now-ringBufferConsumer.lastTime, ringBufferConsumer.timeBetween);
            }
            ringBufferConsumer.lastTime = now;
            
            ringBufferConsumer.rateAvgCnt = ringBufferConsumer.rateAvgCntInit;
        }
        
    }
    
    public static long responseTime(WalkingConsumerState ringBufferConsumer) {
        //Latency in ns
       // System.err.println("inputs:" +ringBufferConsumer.queueFill.valueAtPercent(.5)+"x"+ringBufferConsumer.timeBetween.valueAtPercent(.5)); 
        return (ringBufferConsumer.queueFill.valueAtPercent(.5)*ringBufferConsumer.timeBetween.valueAtPercent(.5))>>ringBufferConsumer.rateAvgBit;
        
    }
    
    public int getMsgIdx() {
        return msgIdx;
    }

    public void setMsgIdx(int idx) {
        this.msgIdx = idx;
    }

    public boolean isNewMessage() {
        return isNewMessage;
    }

    public void setNewMessage(boolean isNewMessage) {
        this.isNewMessage = isNewMessage;
    }


    public long getWaitingNextStop() {
        return waitingNextStop;
    }

    public void setWaitingNextStop(long waitingNextStop) {
        this.waitingNextStop = waitingNextStop;
    }

    public long getBnmHeadPosCache() {
        return bnmHeadPosCache;
    }

    public void setBnmHeadPosCache(long bnmHeadPosCache) {
        this.bnmHeadPosCache = bnmHeadPosCache;
    }

    public int[] getSeqStack() {
        return seqStack;
    }

    public void setSeqStack(int[] seqStack) {
        this.seqStack = seqStack;
    }

    public int getSeqStackHead() {
        return seqStackHead;
    }
    
    public int incSeqStackHead() {
        return ++seqStackHead;
    }

    public void setSeqStackHead(int seqStackHead) {
        this.seqStackHead = seqStackHead;
    }


    public int fragmentSteps(RingBuffer fastRingBuffer) {
	    return from.fragScriptSize[cursor];
	}

	public static boolean canMoveNext(RingBuffer ringBuffer) { 
	    WalkingConsumerState ringBufferConsumer = ringBuffer.consumerData; //TODO: B, should probably remove this to another object
	    
	    //check if we are only waiting for the ring buffer to clear
	    if (ringBufferConsumer.waiting) {
	        //only here if we already checked headPos against moveNextStop at least once and failed.
	        
	        ringBufferConsumer.setBnmHeadPosCache(RingBuffer.headPosition(ringBuffer));
	        ringBufferConsumer.waiting = (ringBufferConsumer.getWaitingNextStop()>(ringBufferConsumer.getBnmHeadPosCache() ));
	        return !(ringBufferConsumer.waiting);
	    }
	         
	    //finished reading the previous fragment so move the working tail position forward for next fragment to read
	    final long cashWorkingTailPos = RingBuffer.getWorkingTailPosition(ringBuffer) +  ringBufferConsumer.activeFragmentDataSize;
	    RingBuffer.setWorkingTailPosition(ringBuffer, cashWorkingTailPos);
	    ringBufferConsumer.activeFragmentDataSize = 0;
	
	    if (ringBufferConsumer.msgIdx<0) {  
	        return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	    } else {
	        return beginFragment(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	    }
	    
	}

	static boolean beginFragment(RingBuffer ringBuffer, WalkingConsumerState ringBufferConsumer, final long cashWorkingTailPos) {
	    ringBufferConsumer.setNewMessage(false);
	    
	    ///TODO: B, add optional groups to this implementation
	    int lastCursor = ringBufferConsumer.cursor;
	    int fragStep = ringBufferConsumer.from.fragScriptSize[lastCursor]; //script jump 
	    ringBufferConsumer.cursor = (ringBufferConsumer.cursor + fragStep);
	
	    
	    //////////////
	    ////Never call these when we jump back for loop
	    //////////////
	    if (sequenceLengthDetector(ringBuffer, fragStep, ringBufferConsumer)) {//TokenBuilder.t
	        //detecting end of message
	        int token;//do not set before cursor is checked to ensure it is not after the script length
	        if ((ringBufferConsumer.cursor>=ringBufferConsumer.from.tokensLen) ||
	                ((((token = ringBufferConsumer.from.tokens[ringBufferConsumer.cursor]) >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)==TypeMask.Group &&
	                	0==(token & (OperatorMask.Group_Bit_Seq<< TokenBuilder.SHIFT_OPER)) //&& //TODO: B, would be much better with end of MSG bit
	                	)) {
	        //	System.err.println("DDD");
	            return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	
	        }
	    }
	    
	    //save the index into these fragments so the reader will be able to find them.
	    //if this fragment is not the same as the last must increment on the stack
	    if (lastCursor != ringBufferConsumer.cursor) {
	    	ringBufferConsumer.activeFragmentStack[++ringBufferConsumer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
	    } else {
	    	ringBufferConsumer.activeFragmentStack[ringBufferConsumer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
	    }
	    
	    
	    return checkForContent(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	}

	static boolean beginNewMessage(RingBuffer ringBuffer, WalkingConsumerState ringBufferConsumer, long cashWorkingTailPos) {
		ringBufferConsumer.setMsgIdx(-1);
	
		//Now beginning a new message so release the previous one from the ring buffer
		//This is the only safe place to do this and it must be done before we check for space needed by the next record.
		RingBuffer.setWorkingTailPosition(ringBuffer, cashWorkingTailPos);
		RingBuffer.releaseReadLock(ringBuffer);
		
		    	
	    //if we can not start to read the next message because it does not have the template id yet      
	    long needStop = cashWorkingTailPos + 1; //NOTE: do not make this bigger or hangs are likely
	    if (needStop>ringBufferConsumer.getBnmHeadPosCache() ) {  
	        ringBufferConsumer.setBnmHeadPosCache(RingBuffer.headPosition(ringBuffer));
	        if (needStop>ringBufferConsumer.getBnmHeadPosCache()) {
	            ringBufferConsumer.setMsgIdx(-1);
	          return false; 
	        }
	    }
	          
	    //keep the queue fill size for Little's law 
	    //also need to keep messages per second data
	    recordRates(ringBufferConsumer, needStop);
	    
	    //Start new stack of fragments because this is a new message
	    ringBufferConsumer.activeFragmentStackHead = 0;
	    ringBufferConsumer.activeFragmentStack[ringBufferConsumer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
	           
	    ringBufferConsumer.setMsgIdx(RingReader.readInt(ringBuffer,  ringBufferConsumer.from.templateOffset)); //jumps over preamble to find templateId
	    //start new message, can not be seq or optional group or end of message.
	    
    	ringBufferConsumer.cursor = ringBufferConsumer.getMsgIdx(); //this is from the stream not the ring buffer.
	    ringBufferConsumer.setNewMessage(true);
	    
	    //////
	    ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]);//save the size of this new fragment we are about to read
	    return true;
	}

		//only called after moving forward.
	    static boolean sequenceLengthDetector(RingBuffer ringBuffer, int jumpSize, WalkingConsumerState consumerData) {
	        if(0==consumerData.cursor) {
	            return false;
	        }
	        int endingToken = consumerData.from.tokens[consumerData.cursor-1];
	        
	        //if last token of last fragment was length then begin new sequence
	        int type = (endingToken >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
	        if (TypeMask.GroupLength == type) {
	            int seqLength = RingReader.readInt(ringBuffer, -1); //length is always at the end of the fragment.
	            if (seqLength == 0) {
	//                int jump = (TokenBuilder.MAX_INSTANCE&from.tokens[cursor-jumpSize])+2;
	                int fragJump = consumerData.from.fragScriptSize[consumerData.cursor+1]; //script jump  //TODO: not sure this is right when they are nested?
	//                System.err.println(jump+" vs "+fragJump);
	         //       System.err.println("******************** jump over seq");
	                //TODO: B, need to build a test case, this does not appear in the current test data.
	                //do nothing and jump over the sequence
	                //there is no data in the ring buffer so do not adjust position
	                consumerData.cursor = (consumerData.cursor + (fragJump&RingBuffer.JUMP_MASK));
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

	static boolean checkForContent(RingBuffer ringBuffer, WalkingConsumerState ringBufferConsumer, long cashWorkingTailPos) {
	    //after alignment with front of fragment, may be zero because we need to find the next message?
	    ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]);//save the size of this new fragment we are about to read
	    
	    //do not let client read fragment if it is not fully in the ring buffer.
	    ringBufferConsumer.setWaitingNextStop(cashWorkingTailPos+ringBufferConsumer.activeFragmentDataSize);
	    
	    //
	    if (ringBufferConsumer.getWaitingNextStop()>ringBufferConsumer.getBnmHeadPosCache()) {
	        ringBufferConsumer.setBnmHeadPosCache(RingBuffer.headPosition(ringBuffer));
	        if (ringBufferConsumer.getWaitingNextStop()>ringBufferConsumer.getBnmHeadPosCache()) {
	            ringBufferConsumer.waiting = true;
	            return false;
	        }
	    }                        
	    return true;
	}

	public static void reset(WalkingConsumerState consumerData) {
        consumerData.waiting = (false);
        consumerData.setWaitingNextStop(-1);
        consumerData.setBnmHeadPosCache(-1);
        consumerData.tailCache=-1;
        
        /////
        consumerData.cursor = (-1);
        consumerData.setSeqStackHead(-1);
        
        consumerData.setMsgIdx(-1);
        consumerData.setNewMessage(false);
        consumerData.activeFragmentDataSize = (0);
        
    }

   
}