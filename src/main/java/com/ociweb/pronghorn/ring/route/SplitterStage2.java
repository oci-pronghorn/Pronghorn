package com.ociweb.pronghorn.ring.route;

import com.ociweb.pronghorn.GraphManager;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stage.PronghornStage;

/**
 * Given n ring buffers with the same FROM/Schema
 * 
 * Does not require schema knowledge for copy but does ensure targets and source have the same FROM.
 * @author Nathan Tippy
 *
 */
public class SplitterStage2 extends PronghornStage {

	private RingBuffer source;
	private RingBuffer[] targets;
	private long[] targetHeadPos;
	
	public int moreToCopy=-2;;
	
	public SplitterStage2(GraphManager gm, RingBuffer source, RingBuffer ... targets) {
		super(gm,source,targets);
		
		this.source = source;
		this.targets = targets;
		
		FieldReferenceOffsetManager sourceFrom = RingBuffer.from(source);
		
		int i = targets.length;
		this.targetHeadPos = new long[i];
		
		while(--i>=0) {
			
			targetHeadPos[i] = targets[i].headPos.get(); 
			
			//targets can not batch returns so this must be set
			RingBuffer.setReleaseBatchSize(targets[i], 0);
			RingReader.setReleaseBatchSize(targets[i], 0);
					
			//confirm this target is large enough for the needed data.
			FieldReferenceOffsetManager targetFrom = RingBuffer.from(targets[i]);
			
			if (targetFrom != sourceFrom) {
				throw new UnsupportedOperationException("Both source and target schemas must be the same");
			}
			
			//NOTE: longest message that holds a sequence needs to fit within a ring if the use case is to set the sequence length last.
			//      therefore if that target is full and needs one more fragment we may have a problem if the batch it has grabbed is 
			//      nearly has large as the target ring.  To resolve this we only need to ensure that the target ring is 2x the source.
			
			int reqTargetSize = source.pBits+1; //target ring must be 2x bigger than source
			if (targets[i].pBits < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" primary bit size must be at least "+reqTargetSize+" but it was "+targets[i].pBits+
						           ". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			reqTargetSize = source.bBits+1;
			if (targets[i].bBits < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" byte bit size must be at least "+reqTargetSize+" but it was "+targets[i].bBits+
									". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			int minDif = source.bBits     -    source.pBits;
			int targDif = targets[i].bBits - targets[i].pBits;
			if (targDif<minDif) {
				throw new UnsupportedOperationException("The target ring "+i+" bit dif must be at least "+minDif+" but it was "+targDif);
			}
		}
	}

	@Override
	public void run() {		
		processAvailData(this);//TODO: C, Should enable use of true to return partial copy. this may cause hang as written.
	}

	private static boolean processAvailData(SplitterStage2 ss) {
		int byteHeadPos;
        long headPos;
		
        if (null==ss.source.buffer) {
        	ss.source.init();
        }
        //TODO: A, publush to a single atomic long and read it here.
        //get the new head position
        byteHeadPos = ss.source.bytesHeadPos.get();
		headPos = ss.source.headPos.get();		
		while(byteHeadPos != ss.source.bytesHeadPos.get() || headPos != ss.source.headPos.get()  ) {
			byteHeadPos = ss.source.bytesHeadPos.get();
			headPos = ss.source.headPos.get();
		}	
			
		
		//we have established the point that we can read up to, this value is changed by the writer on the other side
						
		//get the start and stop locations for the copy
		//now find the point to start reading from, this is moved forward with each new read.		
		int pMask = ss.source.mask;
		long tempTail = ss.source.tailPos.get();
		int primaryTailPos = pMask & (int)tempTail;				
		long totalPrimaryCopy = (headPos - tempTail);
		if (totalPrimaryCopy <= 0) {
			assert(totalPrimaryCopy==0);
			return false; //nothing to copy so come back later
		}
			
		int bMask = ss.source.byteMask;		
		int tempByteTail = ss.source.bytesTailPos.get();
		int byteTailPos = bMask & tempByteTail;
		int totalBytesCopy =      (bMask & byteHeadPos) - byteTailPos; 
		if (totalBytesCopy < 0) {
			totalBytesCopy += (bMask+1);
		}
				
		//now do the copies
		doingCopy(ss, byteTailPos, primaryTailPos, (int)totalPrimaryCopy, totalBytesCopy);
								
		//release tail so data can be written
		ss.source.bytesTailPos.lazySet(ss.source.byteWorkingTailPos.value = 0xEFFFFFFF&(tempByteTail + totalBytesCopy));		
		ss.source.tailPos.lazySet(ss.source.workingTailPos.value = tempTail + totalPrimaryCopy);
		
		return false; //finished all the copy  for now
	}

	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static void doingCopy(SplitterStage2 ss, 
			                   int byteTailPos, int primaryTailPos, 
			                   int totalPrimaryCopy, 
			                   int totalBytesCopy) {
		
		
		do {
			ss.moreToCopy = 0;
			int i = ss.targets.length;
			while (--i>=0) {			
				RingBuffer ringBuffer = ss.targets[i];					
								
				//check to see if we already pushed to this output ring.
				long headCache = ringBuffer.workingHeadPos.value;
				if ( (totalPrimaryCopy + ss.targetHeadPos[i]) > headCache) {		
					
					//the tail must be larger than this position for there to be room to write
					if ((ringBuffer.tailPos.get() >= totalPrimaryCopy + headCache - ringBuffer.maxSize) && 
						(totalBytesCopy <= (ringBuffer.maxByteSize- RingBuffer.bytesOfContent(ringBuffer)) ) ) {
						blockCopy(ss, byteTailPos, totalBytesCopy, primaryTailPos, totalPrimaryCopy, ringBuffer);
					} else {
						ss.moreToCopy++;
					}
					
				} // else this is already done.
				
			}
		} while(ss.moreToCopy>0);
		//reset for next time.
		int i = ss.targets.length;
		while (--i>=0) {
			//mark this one as done.
			ss.targetHeadPos[i] += totalPrimaryCopy;
		}
		ss.moreToCopy=-2;
	}

	public String toString() {
		return getClass().getSimpleName()+ (-2==moreToCopy ? " not running ": " moreToCopy:"+moreToCopy)+" source content "+RingBuffer.contentRemaining(source);
	}

	private static void blockCopy(SplitterStage2 ss, int byteTailPos,
								int totalBytesCopy, int primaryTailPos, int totalPrimaryCopy,
								RingBuffer ringBuffer) {
		
		//copy the bytes
		RingBuffer.copyBytesFromToRing(ss.source.byteBuffer,                   byteTailPos, ss.source.byteMask, 
									  ringBuffer.byteBuffer, ringBuffer.bytesHeadPos.get(), ringBuffer.byteMask, 
									  totalBytesCopy);
		ringBuffer.byteWorkingHeadPos.value = ringBuffer.bytesHeadPos.addAndGet(totalBytesCopy);
								
		//copy the primary data
		RingBuffer.copyIntsFromToRing(ss.source.buffer,                primaryTailPos, ss.source.mask, 
									 ringBuffer.buffer, (int)ringBuffer.headPos.get(), ringBuffer.mask, 
									 totalPrimaryCopy);
		ringBuffer.workingHeadPos.value = ringBuffer.headPos.addAndGet(totalPrimaryCopy);	
		
		//HackTEST
		ringBuffer.ringWalker.bnmHeadPosCache = ringBuffer.workingHeadPos.value;
		
	}
	
	
}
