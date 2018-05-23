package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class StackStateWalker {
	private static final Logger log = LoggerFactory.getLogger(StackStateWalker.class);
	public final FieldReferenceOffsetManager from;

	int msgIdx=-1;
    int msgIdxPrev =-1; //for debug
    boolean isNewMessage;
    public int cursor;
            
    //These two fields are holding state for the high level API
    public long nextWorkingTail; //This is NOT the same as the low level tail cache, this is for reading side of the ring
    public long nextWorkingHead; //This is NOT the same as the low level head cache, this is for writing side of the ring

    public long holdingNextWorkingTail; 

    
    //TODO:M, Make code more foolproof for usage by developers. 
    //        need to add error checking to caputre the case when something on the stack has fallen off the ring
    //        This can be a simple assert when we move the tail that it does not go past the value in stack[0];
    
	final long[] activeReadFragmentStack;
	public final long[] activeWriteFragmentStack; 
	private final int[] seqStack;
	private final int[] seqCursors;
	private int seqStackHead; //TODO: convert to private
			
	int nextCursor = -1;
    
    StackStateWalker(FieldReferenceOffsetManager from, int slabSize) {
		
		    if (null==from) {
	            throw new UnsupportedOperationException();
	        }
	        this.msgIdx = -1;
	        this.isNewMessage = false;
	        this.cursor = -1;
	        this.seqStack = new int[from.maximumFragmentStackDepth];
	        this.seqCursors = new int[from.maximumFragmentStackDepth];
	        this.seqStackHead = -1;
	        this.from = from;
	        this.activeWriteFragmentStack = new long[from.maximumFragmentStackDepth];
	        this.activeReadFragmentStack = new long[from.maximumFragmentStackDepth];

	}

    static void setMsgIdx(StackStateWalker rw, int idx, long llwHeadPosCache) {
		rw.msgIdxPrev = rw.msgIdx;
	
		rw.msgIdx = idx;
		assert(idx < rw.from.fragDataSize.length) : "Bad msgIdx out of range";
		assert(idx>-3): "Bad msgIdx too small ";
		assert(isMsgIdxStartNewMessage(idx, rw)) : "Bad msgIdx is not a starting point. ";
		

		
		
		
		
		
		
		//This validation is very important, because all down stream consumers will assume it to be true.
		assert(-1 ==idx || (rw.from.hasSimpleMessagesOnly && 0==rw.msgIdx && rw.from.messageStarts.length==1)  ||
				TypeMask.Group == TokenBuilder.extractType(rw.from.tokens[rw.msgIdx])) :
					errorMessageForMessageStartValidation(rw, llwHeadPosCache);
		assert(-1 ==idx || (rw.from.hasSimpleMessagesOnly && 0==rw.msgIdx && rw.from.messageStarts.length==1)  ||
				(OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(rw.from.tokens[rw.msgIdx])) == 0) :
					errorMessageForMessageStartValidation(rw, llwHeadPosCache);			
		
	}


    private static String errorMessageForMessageStartValidation(StackStateWalker rw,
            long llwHeadPosCache) {
        return "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(rw.from.tokens[rw.msgIdx])+ 
        " readBase "+rw.activeReadFragmentStack[0] + " nextWorkingTail:"+rw.nextWorkingTail+" headPosCache:"+llwHeadPosCache;
    }

    public static boolean isSeqStackEmpty(StackStateWalker stackState) {
        return stackState.seqStackHead<0;
    }

    //TODO: may want to move preamble to after the ID, it may be easier to reason about.
    
    static boolean prepReadFragment(Pipe ringBuffer,
			final StackStateWalker ringBufferConsumer) {
		//this fragment does not start a message
		ringBufferConsumer.isNewMessage = false;
		
		///
		//check the ring buffer looking for full next fragment
		//return false if we don't have enough data 
		ringBufferConsumer.cursor = ringBufferConsumer.nextCursor; //TODO: for nested example nextCursor must be 35 NOT 26 when we reach the end of the sequence.
		
		assert(isValidFragmentStart(ringBuffer, ringBufferConsumer.nextWorkingTail)) : "last assigned fragment start is invalid, should have been detected far before this point.";
				
		final long target = ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor] + ringBufferConsumer.nextWorkingTail; //One for the template ID NOTE: Caution, this simple implementation does NOT support preamble
						
		if (ringBuffer.llWrite.llwHeadPosCache >= target) {
			prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBuffer.llWrite.llwHeadPosCache = Pipe.headPosition(ringBuffer)) >= target) {
				prepReadFragment(ringBuffer, ringBufferConsumer, ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.cursor], ringBufferConsumer.nextWorkingTail, target);
			} else {
				ringBufferConsumer.isNewMessage = false; 
								
				assert (ringBuffer.llWrite.llwHeadPosCache<=ringBufferConsumer.nextWorkingTail) : 
					  "Partial fragment published!  expected "+(target-ringBufferConsumer.nextWorkingTail)+" but found "+(ringBuffer.llWrite.llwHeadPosCache-ringBufferConsumer.nextWorkingTail);

				return false;
			}
		}
		
		return true;
	}



    private static String invalidFragmentStartMessage(
            final StackStateWalker ringBufferConsumer, final long target) {
        return "Bad target of "+target+" for new fragment start. cursor:"+ringBufferConsumer.cursor+" name:"+ringBufferConsumer.from.fieldNameScript[ringBufferConsumer.cursor] +"    X="+ringBufferConsumer.from.fragDataSize[ringBufferConsumer.cursor]+"+"+ ringBufferConsumer.nextWorkingTail;
    }


	static boolean prepReadMessage(Pipe ringBuffer, StackStateWalker ringBufferConsumer) {
	
		///
		//check the ring buffer looking for new message	
		//return false if we don't have enough data to read the first id and therefore the message
		if (ringBuffer.llWrite.llwHeadPosCache > 1+ringBufferConsumer.nextWorkingTail) { 
			prepReadMessage(ringBuffer, ringBufferConsumer, ringBufferConsumer.nextWorkingTail);
		} else {
			//only update the cache with this CAS call if we are still waiting for data
			if ((ringBuffer.llWrite.llwHeadPosCache = Pipe.headPosition(ringBuffer)) > 1+ringBufferConsumer.nextWorkingTail) {
				prepReadMessage(ringBuffer, ringBufferConsumer, ringBufferConsumer.nextWorkingTail);
			} else {
				//rare slow case where we don't find any data
				ringBufferConsumer.isNewMessage = false; 
				return false;					
			}
		}
		
        return true;//exit early because we do not have any nested closed groups to check for
	}


	private static void prepReadFragment(Pipe ringBuffer,
			final StackStateWalker ringBufferConsumer, final int scriptFragSize,
			long tmpNextWorkingTail, final long target) {

		//from the last known fragment move up the working tail position to this new fragment location
		Pipe.setWorkingTailPosition(ringBuffer, tmpNextWorkingTail);
		
		//save the index into these fragments so the reader will be able to find them.
		ringBufferConsumer.activeReadFragmentStack[ringBufferConsumer.from.fragDepth[ringBufferConsumer.cursor]] = tmpNextWorkingTail;
		
		assert(Pipe.getWorkingBlobRingTailPosition(ringBuffer) <= Pipe.getBlobRingHeadPosition(ringBuffer)) : "expected to have data up to "+Pipe.getWorkingBlobRingTailPosition(ringBuffer)+" but we only have "+Pipe.getBlobRingHeadPosition(ringBuffer);

		int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.cursor + scriptFragSize) -1;
		prepReadFragment2(ringBuffer, ringBufferConsumer, tmpNextWorkingTail, target, lastScriptPos, ringBufferConsumer.from.tokens[lastScriptPos]);
	
	}


	private static void prepReadFragment2(Pipe ringBuffer,
			final StackStateWalker ringBufferConsumer, long tmpNextWorkingTail,
			final long target, int lastScriptPos, int lastTokenOfFragment) {
	    
	    assert(isValidFragmentStart(ringBuffer, target)) : "Bad target of "+target+" for new fragment start";
	    
		//                                   11011    must not equal               10000
        if ( (lastTokenOfFragment &  ( 0x1B <<TokenBuilder.SHIFT_TYPE)) != ( 0x10<<TokenBuilder.SHIFT_TYPE ) ) {
        	 ringBufferConsumer.nextWorkingTail = target;//save the size of this new fragment we are about to read 
        } else {
        	 openOrCloseSequenceWhileInsideFragment(ringBuffer,	ringBufferConsumer, tmpNextWorkingTail, lastScriptPos, lastTokenOfFragment);
        	 ringBufferConsumer.nextWorkingTail = target;
        }
        
	}


	private static boolean isValidFragmentStart(Pipe ringBuffer, long newFragmentBegin) {
        
	    if (newFragmentBegin<=0) {
	        return true;
	    }
	    //this fragment must not be after the head write position
	    if (newFragmentBegin>Pipe.headPosition(ringBuffer)) {
	        FieldReferenceOffsetManager.debugFROM(Pipe.from(ringBuffer));
	        log.error("new fragment to read is after head write position "+newFragmentBegin+"  "+ringBuffer);	        
	        int start = Math.max(0, ringBuffer.slabMask&(int)newFragmentBegin-5);
	        int stop  = Math.min(Pipe.primaryBuffer(ringBuffer).length, ringBuffer.slabMask&(int)newFragmentBegin+5);
	        log.error("Buffer from {} is {}",start, Arrays.toString(Arrays.copyOfRange(Pipe.primaryBuffer(ringBuffer), start, stop)));
	        return false;
	    }
	    if (newFragmentBegin>0) {
	        int byteCount = Pipe.primaryBuffer(ringBuffer)[ringBuffer.slabMask & (int)(newFragmentBegin-1)];
	        if (byteCount<0) {
	            log.error("if this is a new fragment then previous fragment is negative byte count, more likely this is NOT a valid fragment start.");
	            int start = Math.max(0, ringBuffer.slabMask&(int)newFragmentBegin-5);
	            int stop  = Math.min(Pipe.primaryBuffer(ringBuffer).length, ringBuffer.slabMask&(int)newFragmentBegin+5);
	            log.error("Buffer from {} is {}",start, Arrays.toString(Arrays.copyOfRange(Pipe.primaryBuffer(ringBuffer), start, stop)));
	            return false;
	        }
            if (byteCount>ringBuffer.blobMask) {
                log.error("if this is a new fragment then previous fragment byte count is larger than byte buffer, more likely this is NOT a valid fragment start. "+byteCount);
                int start = Math.max(0, ringBuffer.slabMask&(int)newFragmentBegin-5);
                int stop  = Math.min(Pipe.primaryBuffer(ringBuffer).length, ringBuffer.slabMask&(int)newFragmentBegin+5);
                log.error("Buffer from {} is {}",start, Arrays.toString(Arrays.copyOfRange(Pipe.primaryBuffer(ringBuffer), start, stop)));
                return false;
            }	        
	    }
        return true;
    }


    private static void openOrCloseSequenceWhileInsideFragment(
			Pipe ringBuffer, final StackStateWalker ringBufferConsumer,
			long tmpNextWorkingTail, int lastScriptPos, int lastTokenOfFragment) {
		//this is a group or groupLength that has appeared while inside a fragment that does not start a message

		 //this single bit on indicates that this starts a sequence length  00100
		 if ( (lastTokenOfFragment &  ( 0x04 <<TokenBuilder.SHIFT_TYPE)) == 0 ) {
		     if (//if this is a closing sequence group.
		             (lastTokenOfFragment & ( (OperatorMask.Group_Bit_Seq|OperatorMask.Group_Bit_Close) <<TokenBuilder.SHIFT_OPER)) == ((OperatorMask.Group_Bit_Seq|OperatorMask.Group_Bit_Close)<<TokenBuilder.SHIFT_OPER)          
		             ) {    	         
		         continueSequence(ringBufferConsumer);
		     }
		 } else {
		     openSequenceWhileInsideFragment(ringBuffer, ringBufferConsumer, tmpNextWorkingTail, lastScriptPos);
		 }
		 
	}

    private static void openSequenceWhileInsideFragment(Pipe ringBuffer, final StackStateWalker ringBufferConsumer,
            long tmpNextWorkingTail, int lastScriptPos) {
        //this is a groupLength Sequence that starts inside of a fragment 
         int seqLength = Pipe.primaryBuffer(ringBuffer)[(int)(ringBufferConsumer.from.fragDataSize[lastScriptPos] + tmpNextWorkingTail)&ringBuffer.slabMask];
         //now start new sequence
         ringBufferConsumer.seqStack[++ringBufferConsumer.seqStackHead] = seqLength;
         ringBufferConsumer.seqCursors[ringBufferConsumer.seqStackHead] = ringBufferConsumer.nextCursor;
    }


    private static boolean isClosingSequence(int token) {
        return (TypeMask.Group==((token >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) && 
               (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) &&
               (0 != (token & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
    }

	//only called when a closing sequence group is hit.
	private static void continueSequence(final StackStateWalker ringBufferConsumer) {
		//check top of the stack	   
	    int stackHead = ringBufferConsumer.seqStackHead;
	    int seq;
		if ( (seq = ringBufferConsumer.seqStack[stackHead]-1) >0) {		            	
		    ringBufferConsumer.seqStack[stackHead] = seq;
			//stay on cursor location we are counting down.
		    ringBufferConsumer.nextCursor = ringBufferConsumer.seqCursors[stackHead];
		   // System.err.println("ENDING WITHsss :"+ringBufferConsumer.nextCursor);
		    
		} else {
		    ringBufferConsumer.seqStack[stackHead] = seq;
			closeSequence(ringBufferConsumer);
		}
	}

    private static void closeSequence(final StackStateWalker ringBufferConsumer) {
        if (--ringBufferConsumer.seqStackHead>=0) {
            if (isClosingSequence(ringBufferConsumer.from.tokens[ringBufferConsumer.nextCursor ])) {
                if (--ringBufferConsumer.seqStack[ringBufferConsumer.seqStackHead]>0) {  
                    ringBufferConsumer.nextCursor = ringBufferConsumer.seqCursors[ringBufferConsumer.seqStackHead];
                } else {
                    --ringBufferConsumer.seqStackHead;//this dec is the same as the one in the above conditional
                    //TODO:BB, Note this repeating pattern above, this supports 2 nested sequences, Rewrite as while loop to support any number of nested sequences.
                    ringBufferConsumer.nextCursor++;
                }
            }
        } else {
            assert(ringBufferConsumer.seqStackHead<0) : "Error the seqStack should be empty but found value at "+ringBufferConsumer.seqStackHead;
            ringBufferConsumer.nextCursor++;
        }
    }


	private static void prepReadMessage(Pipe pipe, StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail) {
	    //Would like to add an assert like this but the logic is not right yet.
	//    assert(pipe.ringWalker.nextWorkingTail == RingBuffer.getWorkingTailPosition(pipe) || 
	  //          pipe.lastReleasedTail != pipe.ringWalker.nextWorkingTail) : "Must call release before trying to read next message.";

	    //we now have enough room to read the id
		//for this simple case we always have a new message
		ringBufferConsumer.isNewMessage = true;	
		Pipe.setWorkingTailPosition(pipe, tmpNextWorkingTail);
		prepReadMessage2(pipe, ringBufferConsumer, tmpNextWorkingTail);
		
	}


	private static boolean isMsgIdxStartNewMessage(final int msgIdx, StackStateWalker ringBufferConsumer) {
        if (msgIdx < 0) {
            return true;
        }
	    
	    int[] starts = ringBufferConsumer.from.messageStarts;
	    int i = starts.length;
	    while (--i >= 0) {
	        if (starts[i] == msgIdx) {
	            return true;
	        }
	    }
	    
	    System.err.println("bad cursor "+msgIdx+" expected one of "+Arrays.toString(starts));	    
	    return false;
    }


    private static void prepReadMessage2(Pipe ringBuffer, StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail) {
		//
		//Start new stack of fragments because this is a new message
		ringBufferConsumer.activeReadFragmentStack[0] = tmpNextWorkingTail;
		setMsgIdx(ringBufferConsumer, readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail), ringBuffer.llWrite.llwHeadPosCache);
		prepReadMessage2(ringBuffer, ringBufferConsumer, tmpNextWorkingTail,	ringBufferConsumer.from.fragDataSize);
	}


	private static int readMsgIdx(Pipe ringBuffer, StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail) {
	    
		int i = ringBuffer.slabMask & (int)(tmpNextWorkingTail + ringBufferConsumer.from.templateOffset);
        int idx = Pipe.slab(ringBuffer)[i];
        
		
		assert(isMsgIdxStartNewMessage(idx, ringBufferConsumer)) : "Bad msgIdx is not a starting point.";
		return idx;
	}


	private static void prepReadMessage2(Pipe ringBuffer,	StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail, int[] fragDataSize) {
		if (ringBufferConsumer.msgIdx >= 0 && ringBufferConsumer.msgIdx < fragDataSize.length) {
			prepReadMessage2Normal(ringBuffer, ringBufferConsumer, tmpNextWorkingTail, fragDataSize);
		} else {
			prepReadMessage2EOF(ringBuffer, ringBufferConsumer, tmpNextWorkingTail, fragDataSize);
		}
	}


	private static void prepReadMessage2Normal(Pipe ringBuffer,
			StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail,
			int[] fragDataSize) {

		ringBufferConsumer.nextWorkingTail = tmpNextWorkingTail + fragDataSize[ringBufferConsumer.msgIdx];//save the size of this new fragment we are about to read
		
		
		//This validation is very important, because all down stream consumers will assume it to be true.
		assert((ringBufferConsumer.from.hasSimpleMessagesOnly && 0==readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail) && ringBufferConsumer.from.messageStarts.length==1)  ||
				TypeMask.Group == TokenBuilder.extractType(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail)])) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail)]);
	
		assert((ringBufferConsumer.from.hasSimpleMessagesOnly && 0==readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail) && ringBufferConsumer.from.messageStarts.length==1)  ||
				(OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail)])) == 0) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(ringBufferConsumer.from.tokens[readMsgIdx(ringBuffer, ringBufferConsumer, tmpNextWorkingTail)]);
	
		ringBufferConsumer.cursor = ringBufferConsumer.msgIdx;  
		
		int lastScriptPos = (ringBufferConsumer.nextCursor = ringBufferConsumer.msgIdx + ringBufferConsumer.from.fragScriptSize[ringBufferConsumer.msgIdx]) -1;
		if (TypeMask.GroupLength == ((ringBufferConsumer.from.tokens[lastScriptPos] >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE)) {
			//Can not assume end of message any more.
			int seqLength = Pipe.primaryBuffer(ringBuffer)[(int)(ringBufferConsumer.from.fragDataSize[lastScriptPos] + tmpNextWorkingTail)&ringBuffer.slabMask];
            final StackStateWalker ringBufferConsumer1 = ringBufferConsumer;
			//now start new sequence
            ringBufferConsumer1.seqStack[++ringBufferConsumer1.seqStackHead] = seqLength;
            ringBufferConsumer1.seqCursors[ringBufferConsumer1.seqStackHead] = ringBufferConsumer1.nextCursor;
		}
	}

	private static void prepReadMessage2EOF(Pipe ringBuffer,
			StackStateWalker ringBufferConsumer, final long tmpNextWorkingTail,
			int[] fragDataSize) {
		//rare so we can afford some extra checking at this point 
		if (ringBufferConsumer.msgIdx > fragDataSize.length) {
		    
		    
		    
			//this is very large so it is probably bad data, catch it now and send back a meaningful error
			int limit = (ringBuffer.slabMask & (int)(tmpNextWorkingTail + ringBufferConsumer.from.templateOffset))+1;
			throw new UnsupportedOperationException("Bad msgId:"+ringBufferConsumer.msgIdx+
					" encountered at last absolute position:"+(tmpNextWorkingTail + ringBufferConsumer.from.templateOffset)+
					" recent primary ring context:"+Arrays.toString( Arrays.copyOfRange(Pipe.slab(ringBuffer), Math.max(0, limit-10), limit ))+"\n"+ringBuffer);
		}		
		
		//this is commonly used as the end of file marker  
		ringBufferConsumer.nextWorkingTail = tmpNextWorkingTail+Pipe.EOF_SIZE;
	}
    
 
	static void reset(StackStateWalker ringWalker, int ringPos) {

        /////
        resetCursorState(ringWalker);
        
        ringWalker.nextWorkingHead=ringPos;// reading position
        ringWalker.nextWorkingTail=ringPos;// writing position        
                
    }



    static void resetCursorState(StackStateWalker ringWalker) {
        ringWalker.cursor = -1;
        ringWalker.nextCursor = -1;
        ringWalker.seqStackHead = -1;
        ringWalker.msgIdxPrev = -1;
        ringWalker.msgIdx = -1;
        
        ringWalker.isNewMessage = false;
    }

	

	
	static void prepWriteFragment(Pipe pipe, final int cursorPosition, FieldReferenceOffsetManager from, int fragSize) {
		//NOTE: this is called by both blockWrite and tryWrite.  It must not call publish because we need to support
		//      nested long sequences where we don't know the length until after they are all written.

		prepWriteFragmentSpecificProcessing(pipe, cursorPosition, from);
		Pipe.addAndGetWorkingHead(pipe, fragSize);
		pipe.ringWalker.nextWorkingHead = pipe.ringWalker.nextWorkingHead + fragSize;
	
	}


	private static void prepWriteFragmentSpecificProcessing(Pipe pipe, final int cursorPosition, FieldReferenceOffsetManager from) {
		
		if (FieldReferenceOffsetManager.isTemplateStart(from, cursorPosition)) {
			prepWriteMessageStart(pipe, cursorPosition, from);
		 } else {			
			//this fragment does not start a new message but its start position must be recorded for usage later
			pipe.ringWalker.activeWriteFragmentStack[from.fragDepth[cursorPosition]]=Pipe.workingHeadPosition(pipe);
		 }
	}


	private static void prepWriteMessageStart(Pipe pipe, final int cursorPosition, FieldReferenceOffsetManager from) {
		assert(isValidStart(from,cursorPosition)) : "cursorPosition must be a valid message start but it is not. Value is "+cursorPosition;
		//each time some bytes were written in the previous fragment this value was incremented.		
		//now it becomes the base value for all byte writes
		Pipe.markBytesWriteBase(pipe);
		
		//Start new stack of fragments because this is a new message
		pipe.ringWalker.activeWriteFragmentStack[0] = Pipe.workingHeadPosition(pipe);
		
		Pipe.slab(pipe)[pipe.slabMask &(int)(Pipe.workingHeadPosition(pipe) + from.templateOffset)] = cursorPosition;
	}

	private static boolean isValidStart(FieldReferenceOffsetManager from, int cursorPosition) {
		int i = from.messageStarts.length;
		while(--i>=0) {
			if (from.messageStarts[i]==cursorPosition) {
				return true;
			}
		}
		return false;
	}

	static boolean copyFragment0(Pipe inputRing, Pipe outputRing, long start, long end) {
		return copyFragment1(inputRing, outputRing, start, (int)(end-start), Pipe.slab(inputRing)[inputRing.slabMask&(((int)end)-1)]);
	}


	private static boolean copyFragment1(Pipe inputRing, Pipe outputRing, long start, int spaceNeeded, int bytesToCopy) {
		
		if ((spaceNeeded >  outputRing.sizeOfSlabRing-(int)(Pipe.workingHeadPosition(outputRing) - Pipe.tailPosition(outputRing)) )) {
			return false;
		}
		copyFragment2(inputRing, outputRing, (int)start, spaceNeeded, bytesToCopy);		
		return true;
	}


	private static void copyFragment2(Pipe inputRing,	Pipe outputRing, int start, int spaceNeeded, int bytesToCopy) {
		
		Pipe.copyIntsFromToRing(Pipe.slab(inputRing), start, inputRing.slabMask, 
		                        Pipe.slab(outputRing), (int)Pipe.workingHeadPosition(outputRing), outputRing.slabMask, 
				                spaceNeeded);
		Pipe.addAndGetWorkingHead(outputRing, spaceNeeded);
		
		Pipe.copyBytesFromToRing(Pipe.blob(inputRing), Pipe.getWorkingBlobRingTailPosition(inputRing), inputRing.blobMask, 
		                               Pipe.blob(outputRing), Pipe.getBlobWorkingHeadPosition(outputRing), outputRing.blobMask, 
				                       bytesToCopy);

        Pipe.addAndGetBytesWorkingHeadPosition(outputRing, bytesToCopy);		
		
		//release the input fragment, we are using working tail and next working tail so batch release should work on walker.
		//prepReadFragment and tryReadFragment do the batched release, so it is not done here.
		
		//NOTE: writes are using low-level calls and we must publish them.
		Pipe.publishHeadPositions(outputRing); //we use the working head pos so batching still works here.
	}
	
    static boolean hasContentToRead(Pipe pipe) {
        return (pipe.llWrite.llwHeadPosCache > 1+pipe.ringWalker.nextWorkingTail) || hasContentToReadSlow(pipe);
    }

	private static boolean hasContentToReadSlow(Pipe pipe) {
		return (pipe.llWrite.llwHeadPosCache =  Pipe.headPosition(pipe)) > 1+pipe.ringWalker.nextWorkingTail;
	}
	
    static boolean hasRoomForFragmentOfSizeX(Pipe pipe, long limit) {
        return (pipe.llRead.llrTailPosCache >= limit) || hasRoomForFragmentOfSizeXSlow(pipe, limit);
    }

	private static boolean hasRoomForFragmentOfSizeXSlow(Pipe pipe, long limit) {
		return (pipe.llRead.llrTailPosCache =  Pipe.tailPosition(pipe)) >= limit;
	}

    static boolean tryWriteFragment0(Pipe pipe, final int cursorPosition, int fragSize, long target) {
        assert(pipe.llRead.llrTailPosCache <= Pipe.tailPosition(pipe)) : "Tail cache corruption";
        return tryWriteFragment1(pipe, cursorPosition, Pipe.from(pipe), fragSize, target, pipe.llRead.llrTailPosCache >=  target);
    }
    
    static boolean tryWriteFragment1(Pipe pipe, final int cursorPosition, FieldReferenceOffsetManager from, int fragSize, long target, boolean hasRoom) {
                
        assert(Pipe.getPublishBatchSize(pipe)>0 || Pipe.headPosition(pipe)==Pipe.workingHeadPosition(pipe)) : 
        	        "Confirm that tryWrite is only called once per fragment written. OR setBatch publish to zero in startup.  head "+Pipe.headPosition(pipe)+" vs working head "+Pipe.workingHeadPosition(pipe);
        //try again and update the cache with the newest value
        if (hasRoom) {
            prepWriteFragment(pipe, cursorPosition, from, fragSize);
        } else {
            //only if there is no room should we hit the CAS tailPos and then try again.
            hasRoom = (pipe.llRead.llrTailPosCache = Pipe.tailPosition(pipe)) >=  target;       
            if (hasRoom) {      
                prepWriteFragment(pipe, cursorPosition, from, fragSize);
            }
        }
    
       return hasRoom;
    }

    static void blockWriteFragment0(Pipe pipe, int messageTemplateLOC, FieldReferenceOffsetManager from,
            StackStateWalker consumerData) {
        int fragSize = from.fragDataSize[messageTemplateLOC];
		long lastCheckedValue = pipe.llRead.llrTailPosCache;
		while (null==Pipe.slab(pipe) || lastCheckedValue < consumerData.nextWorkingHead - (pipe.sizeOfSlabRing - fragSize)) {
			Pipe.spinWork(pipe);
		    lastCheckedValue = Pipe.tailPosition(pipe);
		}
    	pipe.llRead.llrTailPosCache = lastCheckedValue;
    
    	prepWriteFragment(pipe, messageTemplateLOC, from, fragSize);
    }

    static void writeEOF(Pipe ring) {
        assert(Pipe.workingHeadPosition(ring)<=ring.ringWalker.nextWorkingHead) : "Unsupported use of high level API with low level methods.";
		long lastCheckedValue = ring.llRead.llrTailPosCache;
		while (null==Pipe.slab(ring) || lastCheckedValue < Pipe.workingHeadPosition(ring) - (ring.sizeOfSlabRing - Pipe.EOF_SIZE)) {
			Pipe.spinWork(ring);
		    lastCheckedValue = Pipe.tailPosition(ring);
		}
    	ring.llRead.llrTailPosCache = lastCheckedValue;
    	
    	assert(Pipe.tailPosition(ring)+ring.sizeOfSlabRing>=Pipe.headPosition(ring)+Pipe.EOF_SIZE) : "Must block first to ensure we have 2 spots for the EOF marker";
    	Pipe.setBytesHead(ring, Pipe.getBlobWorkingHeadPosition(ring));
    	Pipe.slab(ring)[ring.slabMask &((int)ring.ringWalker.nextWorkingHead +  Pipe.from(ring).templateOffset)]    = -1;	
    	Pipe.slab(ring)[ring.slabMask &((int)ring.ringWalker.nextWorkingHead +1 +  Pipe.from(ring).templateOffset)] = 0;
    }


	

   
}