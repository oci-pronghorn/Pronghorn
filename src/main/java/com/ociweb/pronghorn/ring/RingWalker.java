package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
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
    
    @Deprecated
    public int activeFragmentDataSize;
    
    public long nextWorkingTail; //NOTE: assumes that ring tail also starts at zero
    public long nextWorkingHead;;
    
    private int[] seqStack;
    private int seqStackHead;
    public long tailCache;
    public final FieldReferenceOffsetManager from;
    
    //TODO: AA, need to add error checking to caputre the case when something on the stack has fallen off the ring
    //      This can be a simple assert when we move the tail that it does not go past the value in stack[0];
	final long[] activeReadFragmentStack;
	final long[] activeWriteFragmentStack; 

	
	private long cachedTailPosition = 0;
	
	private int batchReleaseCountDown = 0;
	private int batchReleaseCountDownInit = 0;
	private int batchPublishCountDown = 0;
	private int batchPublishCountDownInit = 0;
	private int nextCursor = -1;
    
	
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
        this.activeWriteFragmentStack = new long[from.maximumFragmentStackDepth];
        this.activeReadFragmentStack = new long[from.maximumFragmentStackDepth];
        
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

    //New re-design of try read fragment. Incomplete and in progress
    //TODO: move the preamble to after the ID, possibly add ID to the front of all fragments
    
    //this impl only works for simple case where every message is one fragment. 
    public static boolean tryReadFragmentSimple(RingBuffer ringBuffer) { 
    	//assert (0 == RingBuffer.from(ringBuffer).templateOffset) : "Preamble is not supported in this method";
    	
    	//NOTE: if the stackHead is not zero then we are still in fragments?
    	//NOTE: if the cursor position depth?
    	
    	
    	//common var used so pull it out
    	final RingWalker ringBufferConsumer = ringBuffer.consumerData; 

    	if (FieldReferenceOffsetManager.isTemplateStart(RingBuffer.from(ringBuffer), ringBufferConsumer.nextCursor)) {
    		
	    	///
	    	//check the ring buffer looking for new message	
			//return false if we don't have enough data to read the first id and therefore the message
			long tmpNextWokingTail = ringBufferConsumer.nextWorkingTail;
			long target = 1 + tmpNextWokingTail; //One for the template ID NOTE: Caution, this simple implementation does NOT support preamble
			if (ringBufferConsumer.bnmHeadPosCache >= target) { 
				prepReadMessage(ringBuffer, ringBufferConsumer, tmpNextWokingTail);
			} else {
				//only update the cache with this CAS call if we are still waiting for data
				if ((ringBufferConsumer.bnmHeadPosCache = ringBuffer.headPos.get()) >= target) {
					prepReadMessage(ringBuffer, ringBufferConsumer, tmpNextWokingTail);
				} else {
					//rare slow case where we dont find any data
					ringBufferConsumer.isNewMessage = false; 
					return false;					
				}
			}		
			
			//TODO: optimmize the logic of this conditional.
	        if (TypeMask.GroupLength == ((ringBufferConsumer.from.tokens[ringBufferConsumer.cursor + ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor] -1] >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) {
	        	beginNewSequence(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]+target-2);
	        } 
	        return true;//exit early because we do not have any nested closed groups to check for
			   
        } else {
        	
			//this fragment does not start a message
		    ringBufferConsumer.isNewMessage = false;
		    
	    	///
	    	//check the ring buffer looking for full next fragment
			//return false if we don't have enough data 
		    ringBufferConsumer.cursor = ringBufferConsumer.nextCursor;		    
		    final int scriptFragSize = ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor];
			long tmpNextWokingTail = ringBufferConsumer.nextWorkingTail;
			final long target = ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor] + tmpNextWokingTail; //One for the template ID NOTE: Caution, this simple implementation does NOT support preamble
			if (ringBufferConsumer.bnmHeadPosCache >= target) {
				prepReadFragment(ringBuffer, ringBufferConsumer, scriptFragSize, tmpNextWokingTail, target);
			} else {
				//only update the cache with this CAS call if we are still waiting for data
				if ((ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer)) >= target) {
					prepReadFragment(ringBuffer, ringBufferConsumer, scriptFragSize, tmpNextWokingTail, target);
				} else {
					ringBufferConsumer.isNewMessage = false; 
					return false;
				}
			}
        }
   
    	return true;
    	
    }


	private static void prepReadFragment(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer, final int scriptFragSize,
			long tmpNextWokingTail, final long target) {
		//
		//from the last known fragment move up the working tail position to this new fragment location
		RingBuffer.setWorkingTailPosition(ringBuffer, tmpNextWokingTail);
		//save the index into these fragments so the reader will be able to find them.
		ringBufferConsumer.activeReadFragmentStack[ringBufferConsumer.from.fragDepth[ringBufferConsumer.cursor]] =tmpNextWokingTail;

		
		int endingToken = ringBufferConsumer.from.tokens[ringBufferConsumer.cursor + scriptFragSize -1];

		//need to shortcut this

		// the goup and group length will match the pattern 10?00
		// so we will mask with                             11011 
		// and this must equal                              10000
		// if it does not then we have one of the other fields
				
//	        if ( (endingToken &  ( 0x1B <<TokenBuilder.SHIFT_TYPE)) != ( 0x10<<TokenBuilder.SHIFT_TYPE ) ) {
//	        	 ringBufferConsumer.nextWorkingTail = target;//save the size of this new fragment we are about to read 
//	        	 ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
//	        } else {
//	        
//	        	 if ( (endingToken &  ( 0x04 <<TokenBuilder.SHIFT_TYPE)) != 0 ) {
//	        		 //TypeMask.GroupLength
//	        		 ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
//			         beginNewSequence(ringBuffer, ringBufferConsumer, target-1);
//	        	 } else  if (
//				            0 != (endingToken & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)) &&
//				            0 != (endingToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))            
//				                ) {
//				                continueSequence(ringBufferConsumer, scriptFragSize);
//					        }  else {		        	
//					        	//this was not a sequence so the next point is found by a simple addition of the fragSize
//					        	ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
//					        }
			
			
		
			int type = (endingToken >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
		    //if last token of last fragment was length then begin new sequence
		    if (TypeMask.GroupLength == type) {
		    	ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
		    	beginNewSequence(ringBuffer, ringBufferConsumer, target-1);
		    } else {
		    	
		    	
		        //if last token of last fragment was seq close then subtract and move back.
		        if (TypeMask.Group==type &&  //TODO: AAA, too complext must simplify, end of FROM class has example
		            0 != (endingToken & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)) &&
		            0 != (endingToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))            
		                ) {
		                continueSequence(ringBufferConsumer, scriptFragSize);
			        }  else {		        	
			        	//this was not a sequence so the next point is found by a simple addition of the fragSize
			        	ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
			        }
		    	
		    }
		    ringBufferConsumer.nextWorkingTail = target;//save the size of this new fragment we are about to read 
		 	
	}


	private static void autoReturnFromCloseGroups(
			final RingWalker ringBufferConsumer) {
		//While the next cursor is a close group keep incrementing to get out of this nesting.
    	//TODO: must be an easier faster way to do this.
    	boolean isClosingGroup = false;
    	do {				  
    		if (ringBufferConsumer.nextCursor >= ringBufferConsumer.from.tokens.length) {
    			break;
    		}
    		int token = ringBufferConsumer.from.tokens[ringBufferConsumer.nextCursor ];
    		isClosingGroup = (TypeMask.Group==((token >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) && 
    				         (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
  	    	if (isClosingGroup) {
    		   ringBufferConsumer.nextCursor++;
    		}
    	} while (isClosingGroup);
	}


	private static void continueSequence(final RingWalker ringBufferConsumer, int scriptFragSize) {
		//check top of the stack
		if (--ringBufferConsumer.seqStack[ringBufferConsumer.seqStackHead]>0) {		            	
			//stay on cursor location we are counting down.
		    ringBufferConsumer.nextCursor = ringBufferConsumer.cursor;
		} else {
			ringBufferConsumer.seqStackHead = ringBufferConsumer.seqStackHead - 1; 
			
			//this was not a sequence so the next point is found by a simple addition of the fragSize
			ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize;
			autoReturnFromCloseGroups(ringBufferConsumer);
		}
	}


	private static void beginNewSequence(RingBuffer ringBuffer,	final RingWalker ringBufferConsumer, long pos) {
		int seqLength = ringBuffer.buffer[ringBuffer.mask & (int)(pos)];
		
		if (seqLength > 0) {
			//System.err.println("started stack with:"+seqLength);
			ringBufferConsumer.seqStack[++ringBufferConsumer.seqStackHead]=seqLength;
		
		} else {
			//jump over and skip this altogether, the next thing at working tail will be later in the script
			ringBufferConsumer.nextCursor += ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.nextCursor];
			autoReturnFromCloseGroups(ringBufferConsumer);
		}
	}


	private static void prepReadMessage(RingBuffer ringBuffer, RingWalker ringBufferConsumer, long tmpNextWokingTail) {
		//we now have enough room to read the id
		//for this simple case we always have a new message
		ringBufferConsumer.isNewMessage = true; 
		
		//
		//from the last known fragment move up the working tail position to this new fragment location
		ringBuffer.workingTailPos.value = tmpNextWokingTail;//+1; //TODO: AAAA, testing this to jump over the tempalte ID.
		
		//
		//batched release of the old positions back to the producer
		//could be done every time but batching reduces contention
		//this batching is only done per-message so the fragments can remain and be read
		if ((--ringBufferConsumer.batchReleaseCountDown>0)) {	
			prepReadMessage2(ringBuffer, ringBufferConsumer, tmpNextWokingTail);
		} else {
			RingBuffer.releaseReadLock(ringBuffer);
			ringBufferConsumer.batchReleaseCountDown = ringBufferConsumer.batchReleaseCountDownInit;
			prepReadMessage2(ringBuffer, ringBufferConsumer, tmpNextWokingTail);
		}
	}


	private static void prepReadMessage2(RingBuffer ringBuffer, RingWalker ringBufferConsumer, long tmpNextWokingTail) {
		//
		//Start new stack of fragments because this is a new message
		ringBufferConsumer.activeReadFragmentStack[0] = tmpNextWokingTail;		    
		
		final int msgIdx = ringBuffer.buffer[ringBuffer.mask & (int)(tmpNextWokingTail + ringBufferConsumer.from.templateOffset)];
		ringBufferConsumer.msgIdx = msgIdx;

		//System.err.println("pos:"+msgIdx+"  "+ringBufferConsumer.from.fragDataSize.length);
		
		int[] fragDataSize = ringBufferConsumer.from.fragDataSize;
		if (msgIdx >= 0 && msgIdx < fragDataSize.length) {
			//assert that we can read the fragment size. if not we get a partial fragment failure.
			assert(ringBuffer.headPos.get() >= (ringBufferConsumer.nextWorkingTail + fragDataSize[msgIdx])) : "Partial fragment detected";
			
			ringBufferConsumer.nextCursor = msgIdx + ringBufferConsumer.from.fragScriptSize[msgIdx];	    		    			    		
			ringBufferConsumer.nextWorkingTail = tmpNextWokingTail + fragDataSize[msgIdx];//save the size of this new fragment we are about to read  		    		
			ringBufferConsumer.cursor = msgIdx;  
			
		} else {
			//this is commonly used as the end of file marker    		
			ringBufferConsumer.nextWorkingTail = tmpNextWokingTail+1;
		}
	}
    
    //slow and probably broken with a particular race condition when the queue is given spurts of nothing.
	public static boolean tryReadFragment(RingBuffer ringBuffer) { 
		
		//if (true) {//TODO: AAA testing this 
			return tryReadFragmentSimple(ringBuffer);
//		}
//		
//	    RingWalker ringBufferConsumer = ringBuffer.consumerData; //TODO: B, should probably remove this to another object
//	    
//	    //check if we are only waiting for the ring buffer to clear
//	    boolean waiting = ringBufferConsumer.waiting; 
//	    if (waiting) {
//	        //only here if we already checked headPos against moveNextStop at least once and failed.
//	        
//	    	waiting = (ringBufferConsumer.waitingNextStop>(ringBufferConsumer.bnmHeadPosCache ));
//	        if (waiting) {
//	        	//only update the cache with this CAS call if we are still waiting for data
//	        	ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer);
//	        	waiting = (RingWalker.getWaitingNextStop(ringBufferConsumer)>(ringBufferConsumer.bnmHeadPosCache ));	
//	        }
//	        ringBufferConsumer.waiting = waiting;
//	        return !(waiting);
//	    }
//	         
//	    //finished reading the previous fragment so move the working tail position forward for next fragment to read
//	    final long cashWorkingTailPos = RingBuffer.getWorkingTailPosition(ringBuffer) +  ringBufferConsumer.activeFragmentDataSize;
//	    RingBuffer.setWorkingTailPosition(ringBuffer, cashWorkingTailPos);
//	    ringBufferConsumer.activeFragmentDataSize = 0;
//	
//	    //FieldReferenceOffsetManager.isTemplateStart(RingBuffer.from(ringBuffer), cursorPosition)
//	    
//	    if (ringBufferConsumer.msgIdx<0) {  
//	        return beginNewMessage(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
//	    } else {
//	    	//TODO: this is getting called in simple cases where it should not be.
//	        return beginFragment(ringBuffer, ringBufferConsumer, cashWorkingTailPos);
//	    }
	    
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
	    if (sequenceLengthDetector(ringBuffer, fragStep, ringBufferConsumer, lastCursor)) {//TokenBuilder.t
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
	    ringBufferConsumer.activeReadFragmentStack[ringBufferConsumer.from.fragDepth[ringBufferConsumer.cursor]] =cashWorkingTailPos;

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
	          

	    ringBufferConsumer.setNewMessage(true);
	    
	    //Start new stack of fragments because this is a new message
	    ringBufferConsumer.activeReadFragmentStack[0] = cashWorkingTailPos;
	    int msgIdx = RingBuffer.readInt(ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingTailPos.value+ringBufferConsumer.from.templateOffset); //jumps over preamble to find templateId   
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
	    static boolean sequenceLengthDetector(RingBuffer ringBuffer, int jumpSize, RingWalker ringWalker, int lastCursor) {
	        if(0==ringWalker.cursor) {
	            return false;
	        }
	        int endingToken = ringWalker.from.tokens[ringWalker.cursor-1];
	        
	        //if last token of last fragment was length then begin new sequence
	        int type = (endingToken >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
	        if (TypeMask.GroupLength == type) {
	        	
	        	int seqLength = RingBuffer.readInt(ringBuffer.buffer, ringBuffer.mask, (int)ringBuffer.workingTailPos.value-1);
	       // 	System.err.println("reading seq lengh:"+seqLength);
	            //int seqLengthOld = RingReader.readInt(ringBuffer, -1); //length is always at the end of the fragment.
	            //System.err.println(seqLengthNew+" vs "+seqLength);
	            
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
	                ringWalker.seqStack[ringWalker.incSeqStackHead()]=seqLength;
	                //this is the first run so we are already positioned at the top   
	            }
	            return false;   
	            
	        } else
	                
	        
	        //if last token of last fragment was seq close then subtract and move back.
	        if (TypeMask.Group==type && 
	            0 != (endingToken & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)) &&
	            0 != (endingToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))            
	                ) {
	            //check top of the stack
	            if (--ringWalker.seqStack[ringWalker.seqStackHead]>0) {	            	
	               ringWalker.cursor = lastCursor;
	               return false;
	            } else {
	                //done, already positioned to continue
	                ringWalker.setSeqStackHead(ringWalker.seqStackHead - 1);     
	                
	                
	                //done seq at 44  53 and 23
	                
	                //System.err.println("done seq at "+lastCursor+" "+ringWalker.cursor);
	                
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
        consumerData.nextCursor = (-1);
        consumerData.setSeqStackHead(-1);
        consumerData.nextWorkingHead=0;
        consumerData.nextWorkingTail=0;
        
        
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

    /*
	 * Return true if there is room for the desired fragment in the output buffer.
	 * Places working head in place for the first field to be written (eg after the template Id, which is written by this method)
	 * 
	 */
	public static boolean tryWriteFragment(RingBuffer ring, int cursorPosition) {
		
		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		int fragSize = from.fragDataSize[cursorPosition];
		long target = ring.consumerData.nextWorkingHead - (ring.maxSize - fragSize);

		boolean hasRoom = ring.consumerData.cachedTailPosition >=  target;
		//try again and update the cache with the newest value
		if (hasRoom) {
			prepWriteFragment(ring, cursorPosition, from, fragSize);
		} else {
			//only if there is no room should we hit the CAS tailPos and then try again.
			hasRoom = (ring.consumerData.cachedTailPosition = ring.tailPos.longValue()) >=  target;		
			if (hasRoom) {		
				prepWriteFragment(ring, cursorPosition, from, fragSize);
			}
		}
			
		return hasRoom;
	}


	private static void prepWriteFragment(RingBuffer ring, int cursorPosition,	FieldReferenceOffsetManager from, int fragSize) {
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead;
		if (FieldReferenceOffsetManager.isTemplateStart(from, cursorPosition)) {				 
			//Start new stack of fragments because this is a new message
			ring.consumerData.activeWriteFragmentStack[0] = ring.consumerData.nextWorkingHead;
			ring.buffer[ring.mask &(int)(ring.consumerData.nextWorkingHead + from.templateOffset)] = cursorPosition;
			ring.workingHeadPos.value++;//add one so readers using this can assume the template id is written
		 } else {
			//this fragment does not start a new message but its start position must be recorded for usage later
			ring.consumerData.activeWriteFragmentStack[from.fragDepth[cursorPosition]]=ring.consumerData.nextWorkingHead;
		 }
		
		ring.consumerData.nextWorkingHead = ring.consumerData.nextWorkingHead + fragSize;
	}

	public static void blockingFlush(RingBuffer ring) {
		
		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead;
		ring.consumerData.cachedTailPosition = spinBlockOnTail(ring.consumerData.cachedTailPosition, ring.workingHeadPos.value - (ring.maxSize - 1), ring);
		RingWriter.writeInt(ring, from.templateOffset, -1);
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead = ring.consumerData.nextWorkingHead + 1 ;
		RingBuffer.publishWrites(ring);
	}
	
	/*
	 * blocks until there is enough room for the requested fragment on the output ring.
	 * if the fragment needs a template id it is written and the workingHeadPosition is set to the first field. 
	 */
	public static void blockWriteFragment(RingBuffer ring, int cursorPosition) {
		
		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		
		RingWalker consumerData = ring.consumerData;
		int fragSize = from.fragDataSize[cursorPosition];
		consumerData.cachedTailPosition = spinBlockOnTail(consumerData.cachedTailPosition, consumerData.nextWorkingHead - (ring.maxSize - fragSize), ring);
		
		prepWriteFragment(ring, cursorPosition, from, fragSize);
	}
	
	
	public static void publishWrites(RingBuffer outputRing) {
		RingWalker ringBufferConsumer = outputRing.consumerData;
		outputRing.workingHeadPos.value = ringBufferConsumer.nextWorkingHead;
		if ((--ringBufferConsumer.batchPublishCountDown<=0)) {
			RingBuffer.publishWrites(outputRing);
			ringBufferConsumer.batchPublishCountDown = ringBufferConsumer.batchPublishCountDownInit;
		}
		 
	}
	

   
}