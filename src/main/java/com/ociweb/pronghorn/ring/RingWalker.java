package com.ociweb.pronghorn.ring;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class RingWalker {
    int msgIdx=-1;
    int msgIdxPrev =-1; //for debug
    boolean isNewMessage;
    public boolean waiting;
    public int cursor;
            
    public long nextWorkingTail; 
    public long nextWorkingHead; //This is NOT the same as the low level head cache, this is for writing side of the ring

    private int[] seqStack;
    private int seqStackHead;
    private final Logger log = LoggerFactory.getLogger(RingWalker.class);

    public final FieldReferenceOffsetManager from;
    
    //TODO: AA, need to add error checking to caputre the case when something on the stack has fallen off the ring
    //      This can be a simple assert when we move the tail that it does not go past the value in stack[0];
	final long[] activeReadFragmentStack;
	final long[] activeWriteFragmentStack; 

		
	int nextCursor = -1;
    
	
	RingWalker(int mask, FieldReferenceOffsetManager from) {
		this(false, false, -1, -1, -1, 0, new int[from.maximumFragmentStackDepth], -1, from, mask);
	}
	
	
    private RingWalker(boolean isNewMessage, boolean waiting, long waitingNextStop,
                                    long bnmHeadPosCache, int cursor, int activeFragmentDataSize, int[] seqStack, int seqStackHead,
                                    FieldReferenceOffsetManager from, int rbMask) {
    	if (null==from) {
    		throw new UnsupportedOperationException();
    	}
        this.msgIdx = -1;
        this.isNewMessage = isNewMessage;
        this.waiting = waiting;
        this.cursor = cursor;
        this.seqStack = seqStack;
        this.seqStackHead = seqStackHead;
        this.from = from;
        this.activeWriteFragmentStack = new long[from.maximumFragmentStackDepth];
        this.activeReadFragmentStack = new long[from.maximumFragmentStackDepth];
        
    }

    static void setMsgIdx(RingWalker rw, int idx, long llwHeadPosCache) {
		assert(idx<rw.from.fragDataSize.length) : "Corrupt stream, expected message idx < "+rw.from.fragDataSize.length+" however found "+idx;
		assert(idx>-3);
		rw.msgIdxPrev = rw.msgIdx;
		//rw.log.trace("set message id {}", idx);
		rw.msgIdx = idx;
		
		//This validation is very important, because all down stream consumers will assume it to be true.
		assert(-1 ==idx || (rw.from.hasSimpleMessagesOnly && 0==rw.msgIdx && rw.from.messageStarts.length==1)  ||
				TypeMask.Group == TokenBuilder.extractType(rw.from.tokens[rw.msgIdx])) :
					"Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(rw.from.tokens[rw.msgIdx])+ 
					" readBase "+rw.activeReadFragmentStack[0] + " nextWorkingTail:"+rw.nextWorkingTail+" headPosCache:"+llwHeadPosCache;
		assert(-1 ==idx || (rw.from.hasSimpleMessagesOnly && 0==rw.msgIdx && rw.from.messageStarts.length==1)  ||
				(OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(rw.from.tokens[rw.msgIdx])) == 0) :
					"Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(rw.from.tokens[rw.msgIdx])+
					" readBase "+rw.activeReadFragmentStack[0] + " nextWorkingTail:"+rw.nextWorkingTail+" headPosCache:"+llwHeadPosCache;
			
		
	}


    //TODO: may want to move preamble to after the ID, it may be easier to reason about.
    
    static boolean prepReadFragment(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer) {
		//this fragment does not start a message
		ringBufferConsumer.isNewMessage = false;
		
		///
		//check the ring buffer looking for full next fragment
		//return false if we don't have enough data 
		ringBufferConsumer.cursor = ringBufferConsumer.nextCursor;
		final long target = ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor] + ringBufferConsumer.nextWorkingTail; //One for the template ID NOTE: Caution, this simple implementation does NOT support preamble
		if (ringBuffer.llwHeadPosCache >= target) {
			prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBuffer.llwHeadPosCache = RingBuffer.headPosition(ringBuffer)) >= target) {
				prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
			} else {
				ringBufferConsumer.isNewMessage = false; 
								
				assert (ringBuffer.llwHeadPosCache<=ringBufferConsumer.nextWorkingTail) : 
					  "Partial fragment published!  expected "+(target-ringBufferConsumer.nextWorkingTail)+" but found "+(ringBuffer.llwHeadPosCache-ringBufferConsumer.nextWorkingTail);

				return false;
			}
		}
		return true;
	}


	static boolean prepReadMessage(RingBuffer ringBuffer, RingWalker ringBufferConsumer) {
	
		///
		//check the ring buffer looking for new message	
		//return false if we don't have enough data to read the first id and therefore the message
		if (ringBuffer.llwHeadPosCache > 1+ringBufferConsumer.nextWorkingTail) { 
			prepReadMessage(ringBuffer, ringBufferConsumer, ringBufferConsumer.nextWorkingTail);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBuffer.llwHeadPosCache = RingBuffer.headPosition(ringBuffer)) > 1+ringBufferConsumer.nextWorkingTail) {
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

		//always increment this tail position by the count of bytes used by this fragment
		ringBuffer.byteWorkingTailPos.value += ringBuffer.buffer[ringBuffer.mask & (int)(tmpNextWokingTail-1)];			


		//from the last known fragment move up the working tail position to this new fragment location
		ringBuffer.workingTailPos.value = tmpNextWokingTail;
		//save the index into these fragments so the reader will be able to find them.
		ringBufferConsumer.activeReadFragmentStack[ringBufferConsumer.from.fragDepth[ringBufferConsumer.cursor]] =tmpNextWokingTail;
		
		assert(ringBuffer.byteWorkingTailPos.value <= ringBuffer.bytesHeadPos.get()) : "expected to have data up to "+ringBuffer.byteWorkingTailPos.value+" but we only have "+ringBuffer.bytesHeadPos.get();
		
		if ((--ringBuffer.batchReleaseCountDown<=0)) {	
			
			releaseBlockBeforeReadMessage(ringBuffer);
		}

		int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize) -1;
		prepReadFragment2(ringBuffer, ringBufferConsumer, tmpNextWokingTail, target, lastScriptPos, ringBufferConsumer.from.tokens[lastScriptPos]);	
	

	}


	private static void prepReadFragment2(RingBuffer ringBuffer,
			final RingWalker ringBufferConsumer, long tmpNextWokingTail,
			final long target, int lastScriptPos, int lastTokenOfFragment) {
		//                                   11011    must not equal               10000
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
		
		//always increment this tail position by the count of bytes used by this fragment
		if (tmpNextWokingTail>0) { //first iteration it will not have a valid position
			ringBuffer.byteWorkingTailPos.value += ringBuffer.buffer[ringBuffer.mask & (int)(tmpNextWokingTail-1)];	
		}	

		//the byteWorkingTail now holds the new base
		RingBuffer.markBytesReadBase(ringBuffer);

		
		//
		//batched release of the old positions back to the producer
		//could be done every time but batching reduces contention
		//this batching is only done per-message so the fragments can remain and be read
		if ((--ringBuffer.batchReleaseCountDown>0)) {	
			//from the last known fragment move up the working tail position to this new fragment location
			ringBuffer.workingTailPos.value = ringBufferConsumer.nextWorkingTail;
		} else {			
			releaseBlockBeforeReadMessage(ringBuffer);
		}
		prepReadMessage2(ringBuffer, ringBufferConsumer, tmpNextWokingTail);

	}


	private static void releaseBlockBeforeReadMessage(RingBuffer ringBuffer) {
		ringBuffer.workingTailPos.value = ringBuffer.ringWalker.nextWorkingTail;
		ringBuffer.bytesTailPos.lazySet(ringBuffer.byteWorkingTailPos.value); 			
		ringBuffer.tailPos.lazySet(ringBuffer.workingTailPos.value);
					
		ringBuffer.batchReleaseCountDown = ringBuffer.batchReleaseCountDownInit;
	}


	private static void prepReadMessage2(RingBuffer ringBuffer, RingWalker ringBufferConsumer, final long tmpNextWokingTail) {
		//
		//Start new stack of fragments because this is a new message
		ringBufferConsumer.activeReadFragmentStack[0] = tmpNextWokingTail;				 
		setMsgIdx(ringBufferConsumer, readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail), ringBuffer.llwHeadPosCache);
		prepReadMessage2(ringBuffer, ringBufferConsumer, tmpNextWokingTail,	ringBufferConsumer.from.fragDataSize);
	}


	private static int readMsgIdx(RingBuffer ringBuffer, RingWalker ringBufferConsumer, final long tmpNextWokingTail) {
		return ringBuffer.buffer[ringBuffer.mask & (int)(tmpNextWokingTail + ringBufferConsumer.from.templateOffset)];
	}


	private static void prepReadMessage2(RingBuffer ringBuffer,	RingWalker ringBufferConsumer, final long tmpNextWokingTail, int[] fragDataSize) {
		if (ringBufferConsumer.msgIdx >= 0 && ringBufferConsumer.msgIdx < fragDataSize.length) {
			prepReadMessage2Normal(ringBuffer, ringBufferConsumer, tmpNextWokingTail, fragDataSize); 
		} else {
			prepReadMessage2EOF(ringBuffer, ringBufferConsumer, tmpNextWokingTail, fragDataSize);
		}
	}


	private static void prepReadMessage2Normal(RingBuffer ringBuffer,
			RingWalker ringBufferConsumer, final long tmpNextWokingTail,
			int[] fragDataSize) {

		ringBufferConsumer.nextWorkingTail = tmpNextWokingTail + fragDataSize[ringBufferConsumer.msgIdx];//save the size of this new fragment we are about to read  
				
		
		//This validation is very important, because all down stream consumers will assume it to be true.
		assert((ringBufferConsumer.from.hasSimpleMessagesOnly && 0==readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail) && ringBufferConsumer.from.messageStarts.length==1)  ||
				TypeMask.Group == TokenBuilder.extractType(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail)])) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail)]);
		assert((ringBufferConsumer.from.hasSimpleMessagesOnly && 0==readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail) && ringBufferConsumer.from.messageStarts.length==1)  ||
				(OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail)])) == 0) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWokingTail)]);
	
		ringBufferConsumer.cursor = ringBufferConsumer.msgIdx;  
		
		int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.msgIdx + ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.msgIdx]) -1;
		if (TypeMask.GroupLength == ((ringBufferConsumer.from.tokens[lastScriptPos] >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) {
			//Can not assume end of message any more.
			beginNewSequence(ringBufferConsumer, ringBuffer.buffer[(int)(ringBufferConsumer.from.fragDataSize[lastScriptPos] + tmpNextWokingTail)&ringBuffer.mask]);
		}
	}

	private static void prepReadMessage2EOF(RingBuffer ringBuffer,
			RingWalker ringBufferConsumer, final long tmpNextWokingTail,
			int[] fragDataSize) {
		//rare so we can afford some extra checking at this point 
		if (ringBufferConsumer.msgIdx > fragDataSize.length) {
			//this is very large so it is probably bad data, catch it now and send back a meaningful error
			int limit = (ringBuffer.mask & (int)(tmpNextWokingTail + ringBufferConsumer.from.templateOffset))+1;
			throw new UnsupportedOperationException("Bad msgId:"+ringBufferConsumer.msgIdx+
					" encountered at last absolute position:"+(tmpNextWokingTail + ringBufferConsumer.from.templateOffset)+
					" recent primary ring context:"+Arrays.toString( Arrays.copyOfRange(ringBuffer.buffer, Math.max(0, limit-10), limit )));
		}		
		
		//this is commonly used as the end of file marker    		
		ringBufferConsumer.nextWorkingTail = tmpNextWokingTail+RingBuffer.EOF_SIZE;

	}
    
 
	static void reset(RingWalker consumerData, int ringPos) {
        consumerData.waiting = (false);
        
        /////
        consumerData.cursor = (-1);
        consumerData.nextCursor = (-1);
        consumerData.seqStackHead = -1;
        consumerData.nextWorkingHead=ringPos;
        consumerData.nextWorkingTail=ringPos;        
        
        RingWalker.setMsgIdx(consumerData,-1,0);
        consumerData.isNewMessage = false;
        
    }

	
	static boolean tryWriteFragment1(RingBuffer ring, int cursorPosition, FieldReferenceOffsetManager from, int fragSize,
											long target, boolean hasRoom) {
		//try again and update the cache with the newest value
		if (hasRoom) {
			prepWriteFragment(ring, cursorPosition, from, fragSize);
		} else {
			//only if there is no room should we hit the CAS tailPos and then try again.
			hasRoom = (ring.llrTailPosCache = ring.tailPos.longValue()) >=  target;		
			if (hasRoom) {		
				prepWriteFragment(ring, cursorPosition, from, fragSize);
			}
		}
			
		return hasRoom;
	}
	
	static void prepWriteFragment(RingBuffer ring, int cursorPosition,	FieldReferenceOffsetManager from, int fragSize) {
		//NOTE: this is called by both blockWrite and tryWrite.  It must not call publish because we need to support
		//      nested long sequences where we don't know the length until after they are all written.
		
		prepWriteFragmentSpecificProcessing(ring, cursorPosition, from);
		ring.workingHeadPos.value += fragSize;
		ring.ringWalker.nextWorkingHead = ring.ringWalker.nextWorkingHead + fragSize;

		//when publish is called this new byte will be appended due to this request
		ring.writeTrailingCountOfBytesConsumed = (1==from.fragNeedsAppendedCountOfBytesConsumed[cursorPosition]);
				
	}


	private static void prepWriteFragmentSpecificProcessing(RingBuffer ring, int cursorPosition, FieldReferenceOffsetManager from) {
		//Must double check this here for nested sequences, TODO: B, confrim this assumption
		if (ring.writeTrailingCountOfBytesConsumed) {
			RingBuffer.writeTrailingCountOfBytesConsumed(ring, ring.ringWalker.nextWorkingHead -1 ); 
		}
		
		if (FieldReferenceOffsetManager.isTemplateStart(from, cursorPosition)) {
			prepWriteMessageStart(ring, cursorPosition, from);
		 } else {			
			//this fragment does not start a new message but its start position must be recorded for usage later
			ring.ringWalker.activeWriteFragmentStack[from.fragDepth[cursorPosition]]=ring.workingHeadPos.value;
		 }
	}


	private static void prepWriteMessageStart(RingBuffer ring,
			int cursorPosition, FieldReferenceOffsetManager from) {
		//each time some bytes were written in the previous fragment this value was incremented.		
		//now it becomes the base value for all byte writes
		RingBuffer.markBytesWriteBase(ring);
		
		//Start new stack of fragments because this is a new message
		ring.ringWalker.activeWriteFragmentStack[0] = ring.workingHeadPos.value;
		ring.buffer[ring.mask &(int)(ring.workingHeadPos.value + from.templateOffset)] = cursorPosition;
	}

	
	
	
	static boolean copyFragment0(RingBuffer inputRing, RingBuffer outputRing, long start, long end) {
		return copyFragment1(inputRing, outputRing, start, (int)(end-start), inputRing.buffer[inputRing.mask&(((int)end)-1)]);
	}


	private static boolean copyFragment1(RingBuffer inputRing, RingBuffer outputRing, long start, int spaceNeeded, int bytesToCopy) {
		
		if ((spaceNeeded >  outputRing.maxSize-(int)(outputRing.workingHeadPos.value - outputRing.tailPos.get()) )) {
			return false;
		}
		copyFragment2(inputRing, outputRing, (int)start, spaceNeeded, bytesToCopy);		
		return true;
	}


	private static void copyFragment2(RingBuffer inputRing,	RingBuffer outputRing, int start, int spaceNeeded, int bytesToCopy) {
		
		RingBuffer.copyIntsFromToRing(inputRing.buffer, start, inputRing.mask, 
				                      outputRing.buffer, (int)outputRing.workingHeadPos.value, outputRing.mask, 
				                      spaceNeeded);
		outputRing.workingHeadPos.value+=spaceNeeded;
		
		RingBuffer.copyBytesFromToRing(inputRing.byteBuffer, inputRing.byteWorkingTailPos.value, inputRing.byteMask, 
				                       outputRing.byteBuffer, outputRing.byteWorkingHeadPos.value, outputRing.byteMask, 
				                       bytesToCopy);
		outputRing.byteWorkingHeadPos.value =  RingBuffer.BYTES_WRAP_MASK&(bytesToCopy+outputRing.byteWorkingHeadPos.value);
		
		
		//release the input fragment, we are using working tail and next working tail so batch release should work on walker.
		//prepReadFragment and tryReadFragment do the batched release, so it is not done here.
		
		//NOTE: writes are using low-level calls and we must publish them.
		RingBuffer.publishHeadPositions(outputRing); //we use the working head pos so batching still works here.
	}
	
	
	

   
}