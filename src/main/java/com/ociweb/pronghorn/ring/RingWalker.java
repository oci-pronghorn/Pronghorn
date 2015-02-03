package com.ociweb.pronghorn.ring;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;

import java.util.Arrays;

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
	int nextCursor = -1;
    
	
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
        this.seqStack = seqStack;
        this.seqStackHead = seqStackHead;
        this.tailCache = tailCache;
        this.from = from;
        this.activeWriteFragmentStack = new long[from.maximumFragmentStackDepth];
        this.activeReadFragmentStack = new long[from.maximumFragmentStackDepth];
        
    }

    public static void setReleaseBatchSize(RingBuffer rb, int size) {
    	if (true) {
    		size = 0;
    	}
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


    //TODO: may want to move preamble to after the ID, it may be easier to reason about.
    
    //this impl only works for simple case where every message is one fragment. 
    public static boolean tryReadFragment(RingBuffer ringBuffer) { 
		if (FieldReferenceOffsetManager.isTemplateStart(RingBuffer.from(ringBuffer), ringBuffer.consumerData.nextCursor)) {    		
	    	return prepReadMessage(ringBuffer, ringBuffer.consumerData);			   
        } else {   
			return prepReadFragment(ringBuffer, ringBuffer.consumerData);
        }
    }


	private static boolean prepReadFragment(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer) {
		//this fragment does not start a message
		ringBufferConsumer.isNewMessage = false;
		
		///
		//check the ring buffer looking for full next fragment
		//return false if we don't have enough data 
		ringBufferConsumer.cursor = ringBufferConsumer.nextCursor;
		final long target = ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor] + ringBufferConsumer.nextWorkingTail; //One for the template ID NOTE: Caution, this simple implementation does NOT support preamble
		if (ringBufferConsumer.bnmHeadPosCache >= target) {
			prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBufferConsumer.bnmHeadPosCache = RingBuffer.headPosition(ringBuffer)) >= target) {
				prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
			} else {
				ringBufferConsumer.isNewMessage = false; 
				return false;
			}
		}
		return true;
	}


	private static boolean prepReadMessage(RingBuffer ringBuffer, RingWalker ringBufferConsumer) {
	
		///
		//check the ring buffer looking for new message	
		//return false if we don't have enough data to read the first id and therefore the message
		if (ringBufferConsumer.bnmHeadPosCache >= 1 + ringBufferConsumer.nextWorkingTail) { 
			prepReadMessage(ringBuffer, ringBufferConsumer, ringBufferConsumer.nextWorkingTail);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBufferConsumer.bnmHeadPosCache = ringBuffer.headPos.get()) >= 1 + ringBufferConsumer.nextWorkingTail) {
				prepReadMessage(ringBuffer, ringBufferConsumer, ringBufferConsumer.nextWorkingTail);
			} else {
				//rare slow case where we dont find any data
				ringBufferConsumer.isNewMessage = false; 
				return false;					
			}
		}
      return true;//exit early because we do not have any nested closed groups to check for
	}


	private static void prepReadFragment(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer, final int scriptFragSize,
			long tmpNextWokingTail, final long target) {

		//from the last known fragment move up the working tail position to this new fragment location
		ringBuffer.workingTailPos.value = tmpNextWokingTail;
		//save the index into these fragments so the reader will be able to find them.
		ringBufferConsumer.activeReadFragmentStack[ringBufferConsumer.from.fragDepth[ringBufferConsumer.cursor]] =tmpNextWokingTail;

		//TODO: AAA still testing, needed so the release happens as frequently as the publish in an attempt to fix the relative string locations.
		if ((--ringBufferConsumer.batchReleaseCountDown<=0)) {	
			RingBuffer.releaseReadLock(ringBuffer);
			ringBufferConsumer.batchReleaseCountDown = ringBufferConsumer.batchReleaseCountDownInit;
		}

		// the group and group length will match the pattern 10?00
		// so we will mask with                              11011 
		// and this must equal                               10000
		// if it does not then we have one of the other fields  group len 10100  and group is 10000
				
		int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize) -1;
		prepReadFragment2(ringBuffer, ringBufferConsumer, tmpNextWokingTail, target, lastScriptPos, ringBufferConsumer.from.tokens[lastScriptPos]);	
        
//		int sum = 0;
////		int j = ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor];
////		while (--j>=0) {
////			
////							
////			sum +=ringBufferConsumer.from.tokens[j+ ringBufferConsumer.cursor];
////			
////			
////		}
//		sum +=ringBuffer.buffer[ringBuffer.mask & (int)(ringBufferConsumer.nextWorkingTail-1)];
//		if (sum!=0) {
//		//	System.err.println("error");
//		}
////		 
////		if (ringBufferConsumer.from.hasVarLengthFields) {
////			//the last field of this fragment will contain the total length for all var length fields
////			ringBuffer.byteWorkingTailPos.value += ringBuffer.buffer[ringBuffer.mask & (int)(ringBufferConsumer.nextWorkingTail-1)];
////		}		

	}


	private static void prepReadFragment2(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer, long tmpNextWokingTail,
			final long target, int lastScriptPos, int lastTokenOfFragment) {
		//                                                                                                                         11011    must not equal               10000
        if ( (lastTokenOfFragment &  ( 0x1B <<TokenBuilder.SHIFT_TYPE)) != ( 0x10<<TokenBuilder.SHIFT_TYPE ) ) {
        	 ringBufferConsumer.nextWorkingTail = target;//save the size of this new fragment we are about to read 
        } else {
        	 openOrCloseSequenceWhileInsideFragment(ringBuffer,	ringBufferConsumer, tmpNextWokingTail, target, lastScriptPos, lastTokenOfFragment);
        	 ringBufferConsumer.nextWorkingTail = target;
        }
	}


	private static void openOrCloseSequenceWhileInsideFragment(
			RingBuffer ringBuffer, final RingWalker ringBufferConsumer,
			long tmpNextWokingTail, final long target, int lastScriptPos,
			int lastTokenOfFragment) {
		//this is a group or groupLength that has appeared while inside a fragment that does not start a message

		 //this single bit on indicates that this starts a sequence length  00100
		 if ( (lastTokenOfFragment &  ( 0x04 <<TokenBuilder.SHIFT_TYPE)) != 0 ) {
			 //this is a groupLength Sequence that starts inside of a fragment 
			 beginNewSequence(ringBufferConsumer, ringBuffer.buffer[(int)(ringBufferConsumer.from.fragDataSize[lastScriptPos] + tmpNextWokingTail)&ringBuffer.mask]);
		 } else if (//if this is a closing sequence group.
					 (lastTokenOfFragment & ( (OperatorMask.Group_Bit_Seq|OperatorMask.Group_Bit_Close) <<TokenBuilder.SHIFT_OPER)) == ((OperatorMask.Group_Bit_Seq|OperatorMask.Group_Bit_Close)<<TokenBuilder.SHIFT_OPER)          
		            ) {
		                continueSequence(ringBufferConsumer);
			        }
		 
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


	private static void continueSequence(final RingWalker ringBufferConsumer) {
		//check top of the stack
		if (--ringBufferConsumer.seqStack[ringBufferConsumer.seqStackHead]>0) {		            	
			//stay on cursor location we are counting down.
		    ringBufferConsumer.nextCursor = ringBufferConsumer.cursor;
		} else {
			ringBufferConsumer.seqStackHead = ringBufferConsumer.seqStackHead - 1; 
			
			//this was not a sequence so the next point is found by a simple addition of the fragSize
			autoReturnFromCloseGroups(ringBufferConsumer);
		}
	}


	private static void beginNewSequence(final RingWalker ringBufferConsumer, int seqLength) {
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
		ringBuffer.workingTailPos.value = tmpNextWokingTail;
		//TODO: AAAAA,  make same change here as we have on writer so this value is ready to be set upon release.
		//              this will not be compatible with using the low level API that will be forced to do something else.
		
		ringBuffer.bytesTailPos.lazySet(ringBuffer.byteWorkingTailPos.value);
				
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
		
		ringBufferConsumer.msgIdx = ringBuffer.buffer[ringBuffer.mask & (int)(tmpNextWokingTail + ringBufferConsumer.from.templateOffset)];
		int[] fragDataSize = ringBufferConsumer.from.fragDataSize;

		if (ringBufferConsumer.msgIdx >= 0 && ringBufferConsumer.msgIdx < fragDataSize.length) {
			//assert that we can read the fragment size. if not we get a partial fragment failure.
			assert(ringBuffer.headPos.get() >= (ringBufferConsumer.nextWorkingTail + fragDataSize[ringBufferConsumer.msgIdx])) : "Partial fragment detected at "+ringBuffer.headPos.get()+" needs "+fragDataSize[ringBufferConsumer.msgIdx]+" for msgIdx:"+ringBufferConsumer.msgIdx;
			    		
			ringBufferConsumer.nextWorkingTail = tmpNextWokingTail + fragDataSize[ringBufferConsumer.msgIdx];//save the size of this new fragment we are about to read  		    		
			ringBufferConsumer.cursor = ringBufferConsumer.msgIdx;  
			
			int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.msgIdx + ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.msgIdx]) -1;
			if (TypeMask.GroupLength == ((ringBufferConsumer.from.tokens[lastScriptPos] >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) {
				//Can not assume end of message any more.
				beginNewSequence(ringBufferConsumer, ringBuffer.buffer[(int)(ringBufferConsumer.from.fragDataSize[lastScriptPos] + tmpNextWokingTail)&ringBuffer.mask]);
			} 
			
			//CODE THIS AS A WALKER FIRST AND THEN CHECK THE PEROFMRNACE IMPACT BEFORE CHAING THE MESSAGE FORMAT
//			
//			int sum = 0;
////			int j = ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor];
////			while (--j>=0) {
////				
////								
////				sum +=ringBufferConsumer.from.tokens[j+ ringBufferConsumer.cursor];
////				
////				
////			}
//			sum +=ringBuffer.buffer[ringBuffer.mask & (int)(ringBufferConsumer.nextWorkingTail-1)];
//			if (sum!=0) {
//				//System.err.println("error");
//			}
////			
////			if (ringBufferConsumer.from.hasVarLengthFields) {
////				//the last field of this fragment will contain the total length for all var length fields
////				ringBuffer.byteWorkingTailPos.value += ringBuffer.buffer[ringBuffer.mask & (int)(ringBufferConsumer.nextWorkingTail-1)];
////			}			
			
		} else {
			//rare so we can afford some extra checking at this point 
			if (ringBufferConsumer.msgIdx > fragDataSize.length) {
				//this is very large so it is probably bad data, catch it now and send back a meaningful error
				int limit = (ringBuffer.mask & (int)(tmpNextWokingTail + ringBufferConsumer.from.templateOffset))+1;
				throw new UnsupportedOperationException("Bad msgId:"+ringBufferConsumer.msgIdx+
						" encountered at last absolute position:"+(tmpNextWokingTail + ringBufferConsumer.from.templateOffset)+
						" recent primary ring context:"+Arrays.toString( Arrays.copyOfRange(ringBuffer.buffer, Math.max(0, limit-10), limit )));
			}		
			
			//this is commonly used as the end of file marker    		
			ringBufferConsumer.nextWorkingTail = tmpNextWokingTail+1;
		}
	}
    
 
	public static void reset(RingWalker consumerData, int ringPos) {
        consumerData.waiting = (false);
        RingWalker.setWaitingNextStop(consumerData,(long) -1);
        RingWalker.setBnmHeadPosCache(consumerData,(long) -1);
        consumerData.tailCache=-1;
        
        /////
        consumerData.cursor = (-1);
        consumerData.nextCursor = (-1);
        consumerData.setSeqStackHead(-1);
        consumerData.nextWorkingHead=ringPos;
        consumerData.nextWorkingTail=ringPos;        
        
        RingWalker.setMsgIdx(consumerData,-1);
        consumerData.setNewMessage(false);
        
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
	
	/*
	 * blocks until there is enough room for the requested fragment on the output ring.
	 * if the fragment needs a template id it is written and the workingHeadPosition is set to the first field. 
	 */
	public static void blockWriteFragment(RingBuffer ring, int cursorPosition) {

		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		
		RingWalker consumerData = ring.consumerData;
		int fragSize = from.fragDataSize[cursorPosition];
		consumerData.cachedTailPosition = spinBlockOnTail(consumerData.cachedTailPosition, consumerData.nextWorkingHead - (ring.maxSize - fragSize), ring);
		//TODO: what is the next working head set to??
		prepWriteFragment(ring, cursorPosition, from, fragSize);
	}
	

	private static void prepWriteFragment(RingBuffer ring, int cursorPosition,	FieldReferenceOffsetManager from, int fragSize) {
		
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead;
		if (FieldReferenceOffsetManager.isTemplateStart(from, cursorPosition)) {			

			//each time some bytes were written in the previous fragment this value was incremented.		
			//now it becomes the base value for all byte writes
			//similar to publish except for bytes
			ring.bytesHeadPos.lazySet(ring.byteWorkingHeadPos.value);			
						
			//Start new stack of fragments because this is a new message
			ring.consumerData.activeWriteFragmentStack[0] = ring.consumerData.nextWorkingHead;
			ring.buffer[ring.mask &(int)(ring.consumerData.nextWorkingHead + from.templateOffset)] = cursorPosition;

		 } else {
			//this fragment does not start a new message but its start position must be recorded for usage later
			ring.consumerData.activeWriteFragmentStack[from.fragDepth[cursorPosition]]=ring.consumerData.nextWorkingHead;
		 }
		ring.consumerData.nextWorkingHead = ring.consumerData.nextWorkingHead + fragSize;
		
        
    	//TODO: AAA, caution!!, when using the tryWrite form the working position is pre set and must not be incremented!!
    	//               perhaps an assert high bit could be used to detect this situation'
    			
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead;
	}

	public static void blockingFlush(RingBuffer ring) {
		
		FieldReferenceOffsetManager from = RingBuffer.from(ring);
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead;
		ring.consumerData.cachedTailPosition = spinBlockOnTail(ring.consumerData.cachedTailPosition, ring.workingHeadPos.value - (ring.maxSize - 1), ring);
		ring.bytesHeadPos.lazySet(ring.byteWorkingHeadPos.value);
		ring.buffer[ring.mask &((int)ring.consumerData.nextWorkingHead +  from.templateOffset)] = -1;		
		ring.workingHeadPos.value = ring.consumerData.nextWorkingHead = ring.consumerData.nextWorkingHead + 1 ;
		RingBuffer.publishWrites(ring);
	}
	

	
	public static void publishWrites(RingBuffer outputRing) {
		RingWalker ringBufferConsumer = outputRing.consumerData;
		outputRing.workingHeadPos.value = ringBufferConsumer.nextWorkingHead;
    	
		assert(outputRing.consumerData.nextWorkingHead<=outputRing.headPos.get() || outputRing.workingHeadPos.value<=outputRing.consumerData.nextWorkingHead) : "Unsupported mix of high and low level API.";
    	
		if ((--ringBufferConsumer.batchPublishCountDown<=0)) {
			RingBuffer.publishWrites(outputRing);
			ringBufferConsumer.batchPublishCountDown = ringBufferConsumer.batchPublishCountDownInit;
		}
		 
	}
	

   
}