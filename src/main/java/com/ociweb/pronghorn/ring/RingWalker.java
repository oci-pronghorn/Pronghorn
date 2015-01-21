package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class RingWalker {
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
    
    //TODO: AA, need unit test and finish implementation of the stack
	final int[] activeFragmentStack;
	int   activeFragmentStackHead = 0;
	
	private long cachedTailPosition = 0;
	
	private int batchReleaseCountDown = 0;
	private int batchReleaseCountDownInit = 0;
	private int batchPublishCountDown = 0;
	private int batchPublishCountDownInit = 0;;
    
	
	public RingWalker(int mask, FieldReferenceOffsetManager from) {
		this(-1, false, false, -1, -1, -1, 0, new int[from.maximumFragmentStackDepth], -1, -1, from, mask);
	}
	
	
    private RingWalker(int messageId, boolean isNewMessage, boolean waiting, long waitingNextStop,
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
        this.activeFragmentStack = new int[from.maximumFragmentStackDepth];
        
    }

    public static void setReleaseBatchSize(RingBuffer rb, int size) {
    	rb.consumerData.batchReleaseCountDownInit = size;
    	rb.consumerData.batchReleaseCountDown = size;    	
    }
    
    public static void setPublishBatchSize(RingBuffer rb, int size) {
    	rb.consumerData.batchPublishCountDownInit = size;
    	rb.consumerData.batchPublishCountDown = size;    	
    }
       
  
    public static int getMsgIdx(RingBuffer rb) {
		return rb.consumerData.msgIdx;
	}
    
    public static int getMsgIdx(RingWalker rw) {
		return rw.msgIdx;
	}

    public static void setMsgIdx(RingWalker rw, int idx) {
		assert(idx<rw.from.fragDataSize.length) : "Corrupt stream, expected message idx < "+rw.from.fragDataSize.length+" however found "+idx;
		assert(idx>-3);
        rw.msgIdx = idx;
	}

    public static boolean isNewMessage(RingWalker rw) {
		return rw.isNewMessage;
	}

    public void setNewMessage(boolean isNewMessage) {
        this.isNewMessage = isNewMessage;
    }

    public static long getWaitingNextStop(RingWalker ringWalker) {
        return ringWalker.waitingNextStop;
    }  

    public static void setWaitingNextStop(RingWalker ringWalker, long waitingNextStop) {
    	ringWalker.waitingNextStop = waitingNextStop;
    }

    public static long getBnmHeadPosCache(RingWalker ringWalker) {
        return ringWalker.bnmHeadPosCache;
    }

    public static void setBnmHeadPosCache(RingWalker ringWalker, long bnmHeadPosCache) {
        ringWalker.bnmHeadPosCache = bnmHeadPosCache;
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

	public static boolean tryReadFragment(RingBuffer ringBuffer) { 
	    RingWalker ringBufferConsumer = ringBuffer.consumerData; //TODO: B, should probably remove this to another object
	    
	    //check if we are only waiting for the ring buffer to clear
	    boolean waiting = ringBufferConsumer.waiting; 
	    if (waiting) {
	        //only here if we already checked headPos against moveNextStop at least once and failed.
	        
	    	waiting = (ringBufferConsumer.waitingNextStop>(ringBufferConsumer.bnmHeadPosCache ));
	        if (waiting) {
	        	//only update the cache with this CAS call if we are still waiting for data
	        	ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer);
	        	waiting = (RingWalker.getWaitingNextStop(ringBufferConsumer)>(ringBufferConsumer.bnmHeadPosCache ));	
	        }
	        ringBufferConsumer.waiting = waiting;
	        return !(waiting);
	    }
	         
	    //finished reading the previous fragment so move the working tail position forward for next fragment to read
	    final long cashWorkingTailPos = RingBuffer.getWorkingTailPosition(ringBuffer) +  ringBufferConsumer.activeFragmentDataSize;
	    RingBuffer.setWorkingTailPosition(ringBuffer, cashWorkingTailPos);
	    ringBufferConsumer.activeFragmentDataSize = 0;
	
	    if (ringBufferConsumer.msgIdx<0) {  
	        return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	    } else {
	    	//TODO: this is getting called in simple cases where it should not be.
	        return beginFragment(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
	    }
	    
	}

	static boolean beginFragment(RingBuffer ringBuffer, RingWalker ringBufferConsumer, final long cashWorkingTailPos) {
	    ringBufferConsumer.setNewMessage(false);
	    
	    //TODO: this should not be called for this simple case    System.err.println("fragment    ");
	    
	    ///TODO: B, add optional groups to this implementation
	    int lastCursor = ringBufferConsumer.cursor;
	    int fragStep = ringBufferConsumer.from.fragScriptSize[lastCursor]; //script jump 
	    ringBufferConsumer.cursor = (lastCursor + fragStep);
	
	    
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
	    if (0 != fragStep) {//lastCursor != ringBufferConsumer.cursor) {
	    	ringBufferConsumer.activeFragmentStack[++ringBufferConsumer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
	    } else {
	    	ringBufferConsumer.activeFragmentStack[ringBufferConsumer.activeFragmentStackHead] = ringBuffer.mask&(int)cashWorkingTailPos;
	    }
	    
	    
	    //after alignment with front of fragment, may be zero because we need to find the next message?
		ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]);//save the size of this new fragment we are about to read
		
		//do not let client read fragment if it is not fully in the ring buffer.
		ringBufferConsumer.waitingNextStop = cashWorkingTailPos+ringBufferConsumer.activeFragmentDataSize;
		
		//
		if (ringBufferConsumer.waitingNextStop>ringBufferConsumer.bnmHeadPosCache) {
		    ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer);
		    if (ringBufferConsumer.waitingNextStop>ringBufferConsumer.bnmHeadPosCache) {
		        ringBufferConsumer.waiting = true;
		        if (RingBuffer.isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
					throw new RingBufferException("Unexpected shutdown");
				}
		        return false;
		    }
		}                        
		return true;
	}

	static boolean beginNewMessage(RingBuffer ringBuffer, RingWalker ringBufferConsumer, long cashWorkingTailPos) {
	
		//Now beginning a new message so release the previous one from the ring buffer
		//This is the only safe place to do this and it must be done before we check for space needed by the next record.
		RingBuffer.setWorkingTailPosition(ringBuffer, cashWorkingTailPos);

		
		if ((--ringBufferConsumer.batchReleaseCountDown<=0)) {
			RingBuffer.releaseReadLock(ringBuffer);
			ringBufferConsumer.batchReleaseCountDown = ringBufferConsumer.batchReleaseCountDownInit;
		}
		
	    //if we can not start to read the next message because it does not have the template id yet      
	    long needStop = cashWorkingTailPos + 1; //NOTE: do not make this bigger or hangs are likely
	    if (needStop>ringBufferConsumer.bnmHeadPosCache ) {  
	        ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer);
	        if (needStop>ringBufferConsumer.bnmHeadPosCache) {
	        	//this value is never valid so we use it to mark that we are waiting for data
				ringBufferConsumer.msgIdx = -2;
	            if (RingBuffer.isShutDown(ringBuffer) || Thread.currentThread().isInterrupted()) {
	    			throw new RingBufferException("Unexpected shutdown");
	    		}
	            return false; 
	        }
	    }
	          
	    //keep the queue fill size for Little's law 
	    //also need to keep messages per second data
	    ringBufferConsumer.setNewMessage(true);
	    
	    //Start new stack of fragments because this is a new message
	    ringBufferConsumer.activeFragmentStackHead = 0;
	    ringBufferConsumer.activeFragmentStack[0] = ringBuffer.mask&(int)cashWorkingTailPos;
	      
	    int msgIdx = RingReader.readInt(ringBuffer,  ringBufferConsumer.from.templateOffset); //jumps over preamble to find templateId   
		ringBufferConsumer.msgIdx = msgIdx;
    	if (msgIdx < 0) {
    		//this is commonly used as the end of file marker
    		return true;
    	}
	    
	    //start new message, can not be seq or optional group or end of message.
	    
    	ringBufferConsumer.cursor = msgIdx; 
	    
	    //////
	    ringBufferConsumer.activeFragmentDataSize = (ringBufferConsumer.from.fragDataSize[msgIdx]);//save the size of this new fragment we are about to read
	    return true;
	}

		//only called after moving forward.
	    static boolean sequenceLengthDetector(RingBuffer ringBuffer, int jumpSize, RingWalker ringWalker) {
	        if(0==ringWalker.cursor) {
	            return false;
	        }
	        int endingToken = ringWalker.from.tokens[ringWalker.cursor-1];
	        
	        //if last token of last fragment was length then begin new sequence
	        int type = (endingToken >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
	        if (TypeMask.GroupLength == type) {
	            int seqLength = RingReader.readInt(ringBuffer, -1); //length is always at the end of the fragment.
	            if (seqLength == 0) {
	//                int jump = (TokenBuilder.MAX_INSTANCE&from.tokens[cursor-jumpSize])+2;
	                int fragJump = ringWalker.from.fragScriptSize[ringWalker.cursor+1]; //script jump  //TODO: not sure this is right when they are nested?
	//                System.err.println(jump+" vs "+fragJump);
	         //       System.err.println("******************** jump over seq");
	                //TODO: B, need to build a test case, this does not appear in the current test data.
	                //do nothing and jump over the sequence
	                //there is no data in the ring buffer so do not adjust position
	                ringWalker.cursor = (ringWalker.cursor + (fragJump&RingBuffer.JUMP_MASK));
	                //done so move to the next item
	                
	                return true;
	            } else {
	                assert(seqLength>=0) : "The previous fragment has already been replaced or modified and it was needed for the length counter";
	                ringWalker.getSeqStack()[ringWalker.incSeqStackHead()]=seqLength;
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
	            if (--ringWalker.getSeqStack()[ringWalker.getSeqStackHead()]>0) {
	                int jump = (TokenBuilder.MAX_INSTANCE&endingToken)+1;
	               ringWalker.cursor = (ringWalker.cursor - jump);
	               return false;
	            } else {
	                //done, already positioned to continue
	                ringWalker.setSeqStackHead(ringWalker.getSeqStackHead() - 1);                
	                return true;
	            }
	        }                   
	        return true;
	    }

	public static void reset(RingWalker consumerData) {
        consumerData.waiting = (false);
        RingWalker.setWaitingNextStop(consumerData,(long) -1);
        RingWalker.setBnmHeadPosCache(consumerData,(long) -1);
        consumerData.tailCache=-1;
        
        /////
        consumerData.cursor = (-1);
        consumerData.setSeqStackHead(-1);
        
        RingWalker.setMsgIdx(consumerData,-1);
        consumerData.setNewMessage(false);
        consumerData.activeFragmentDataSize = (0);
        
    }

	public static boolean isNewMessage(RingBuffer ring) {
		return isNewMessage(ring.consumerData);
	}


	public static int messageIdx(RingBuffer ring) {
		return getMsgIdx(ring.consumerData);
	}

    /**
     * Non blocking call to write fragment to ring buffer.
     * Returns false if there is not enough room for the fragment.
     * 
     * @param ring
     * @param cursorPosition
     * @return
     */
	public static boolean tryWriteFragment(RingBuffer ring, int cursorPosition) {
		
		int fullCount = ring.maxSize - RingBuffer.from(ring).fragDataSize[cursorPosition];

		boolean hasRoom = fullCount >=  (ring.workingHeadPos.value - ring.consumerData.cachedTailPosition) ;
		if (!hasRoom) {
			//only if there is no room should we hit the CAS tailPos and then try again.
			ring.consumerData.cachedTailPosition = ring.tailPos.longValue();
			hasRoom = fullCount >=  (ring.workingHeadPos.value - ring.consumerData.cachedTailPosition) ;		
		}
			
		if (hasRoom) {
									
		      //TODO: this is too complex and will be simplified, only write if this is the beginning of a message.
			  //       do as single combined mask and check, do the same for the tryRead rewrite logic.
			  if (0 == cursorPosition ||
					  (0 !=	(RingBuffer.from(ring).tokens[cursorPosition] & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) && 
				        (RingBuffer.from(ring).tokens[cursorPosition] & (TokenBuilder.MASK_TYPE<<TokenBuilder.SHIFT_TYPE ))==(TypeMask.Group<<TokenBuilder.SHIFT_TYPE)) {

				 // System.err.println("write id");

				  //add template loc in prep for write
				  RingWriter.writeInt(ring, cursorPosition); //TODO: AA,  this is moving the position and probably a very bad idea as it has side effect
				  
			  }
	
		}
		return hasRoom;
	}
	
	public static void blockWriteFragment(RingBuffer ring, int cursorPosition) {
		
		int fullCount = ring.maxSize - RingBuffer.from(ring).fragDataSize[cursorPosition];
	
		
		ring.consumerData.cachedTailPosition = spinBlockOnTail(ring.consumerData.cachedTailPosition, 
				                                           ring.workingHeadPos.value-fullCount,
				                                              ring);
							
	      //TODO: this is too complex and will be simplified, only write if this is the beginning of a message. 
		  if (0 == cursorPosition ||
				  (0 !=	(RingBuffer.from(ring).tokens[cursorPosition] & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) && 
			        (RingBuffer.from(ring).tokens[cursorPosition] & (TokenBuilder.MASK_TYPE<<TokenBuilder.SHIFT_TYPE ))==(TypeMask.Group<<TokenBuilder.SHIFT_TYPE)) {
	
			 // System.err.println("write id");
	
			  //add template loc in prep for write
			  RingWriter.writeInt(ring, cursorPosition); //TODO: AA,  this is moving the position and probably a very bad idea as it has side effect
			  
		  }

	}

	
	/**
	 * Blocking call however it will also call run on other work each time it runs out of work todo.
	 * 
	 * @param ring
	 * @param cursorPosition
	 * @param otherWork
	 */
	public static void blockWriteFragment(RingBuffer ring, int cursorPosition, Runnable otherWork) {
		while (!tryWriteFragment(ring, cursorPosition)) {
			if (RingBuffer.isShutDown(ring) || Thread.currentThread().isInterrupted()) {
    			throw new RingBufferException("Unexpected shutdown");
    		}
			otherWork.run();
        }
	}


	public static void publishWrites(RingBuffer outputRing) {
		RingWalker ringBufferConsumer = outputRing.consumerData;
		if ((--ringBufferConsumer.batchPublishCountDown<=0)) {
			RingBuffer.publishWrites(outputRing);
			ringBufferConsumer.batchPublishCountDown = ringBufferConsumer.batchPublishCountDownInit;
		}
		 
	}
	

   
}