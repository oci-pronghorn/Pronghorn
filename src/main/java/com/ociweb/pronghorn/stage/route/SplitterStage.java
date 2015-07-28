package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Given n ring buffers with the same FROM/Schema
 * 
 * Does not require schema knowledge for copy but does ensure targets and source have the same FROM.
 * @author Nathan Tippy
 *
 */
public class SplitterStage extends PronghornStage {

	private RingBuffer source;
	private RingBuffer[] targets;
	
	private int byteHeadPos;
    private long headPos;
    private long cachedTail;
	private long totalPrimaryCopy;
	private int[] working;
	private int   workingPos;
    	
	int tempByteTail; 
	int byteTailPos;
	int totalBytesCopy;
	
	public SplitterStage(GraphManager gm, RingBuffer source, RingBuffer ... targets) {
		super(gm,source,targets);
		
		this.source = source;
		this.targets = targets;
		
		this.cachedTail = RingBuffer.tailPosition(source);
	
		this.supportsBatchedPublish = false;
		this.supportsBatchedRelease = false;		
		
		FieldReferenceOffsetManager sourceFrom = RingBuffer.from(source);
		
		int i = targets.length;
		working = new int[i];
		while(--i>=0) {
			
			
					
			//confirm this target is large enough for the needed data.
			FieldReferenceOffsetManager targetFrom = RingBuffer.from(targets[i]);
			
			if (targetFrom != sourceFrom) {
				throw new UnsupportedOperationException("Both source and target schemas must be the same");
			}
			
			//NOTE: longest message that holds a sequence needs to fit within a ring if the use case is to set the sequence length last.
			//      therefore if that target is full and needs one more fragment we may have a problem if the batch it has grabbed is 
			//      nearly has large as the target ring.  To resolve this we only need to ensure that the target ring is 2x the source.
			
			int reqTargetSize = source.bitsOfStructuredLayoutRingBuffer+1; //target ring must be 2x bigger than source
			if (targets[i].bitsOfStructuredLayoutRingBuffer < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" primary bit size must be at least "+reqTargetSize+" but it was "+targets[i].bitsOfStructuredLayoutRingBuffer+
						           ". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			reqTargetSize = source.bitsOfUntructuredLayoutRingBuffer+1;
			if (targets[i].bitsOfUntructuredLayoutRingBuffer < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" byte bit size must be at least "+reqTargetSize+" but it was "+targets[i].bitsOfUntructuredLayoutRingBuffer+
									". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			int minDif = source.bitsOfUntructuredLayoutRingBuffer     -    source.bitsOfStructuredLayoutRingBuffer;
			int targDif = targets[i].bitsOfUntructuredLayoutRingBuffer - targets[i].bitsOfStructuredLayoutRingBuffer;
			if (targDif<minDif) {
				throw new UnsupportedOperationException("The target ring "+i+" bit dif must be at least "+minDif+" but it was "+targDif);
			}
		}
	}
	
	@Override
	public void run() {		
		processAvailData(this);
	}

	@Override
	public void shutdown() {
		//if we are in the middle of a partial copy push the data out, this is blocking
		while (0!=totalPrimaryCopy) {
			//if all the copies are done then record it as complete, does as much work as possible each time its called.
			if (doneCopy(this, byteTailPos, source.mask & (int)cachedTail, (int)totalPrimaryCopy, totalBytesCopy)) {
				recordCopyComplete(this, tempByteTail, totalBytesCopy);			
			}	
		}
	}
	
	private static void processAvailData(SplitterStage ss) {

		if (0==ss.totalPrimaryCopy) {
	        findStableCutPoint(ss);			
	        //we have established the point that we can read up to, this value is changed by the writer on the other side
										
			//get the start and stop locations for the copy
			//now find the point to start reading from, this is moved forward with each new read.
			if ((ss.totalPrimaryCopy = (ss.headPos - ss.cachedTail)) <= 0) {
				assert(ss.totalPrimaryCopy==0);
				return; //nothing to copy so come back later
			}
			//clear the flags for which targets have room
			int i = ss.working.length;
			ss.workingPos = i;
			while (--i>=0) {
				ss.working[i]=i;
			}
			//collect all the constant values needed for doing the copy
			ss.tempByteTail = RingBuffer.bytesTailPosition(ss.source);
			ss.byteTailPos = ss.source.byteMask & ss.tempByteTail;
			if ((ss.totalBytesCopy =      (ss.source.byteMask & ss.byteHeadPos) - ss.byteTailPos) < 0) {
				ss.totalBytesCopy += (ss.source.byteMask+1);
			}			
		}

		//if all the copies are done then record it as complete, does as much work as possible each time its called.
		if (doneCopy(ss, ss.byteTailPos, ss.source.mask & (int)ss.cachedTail, (int)ss.totalPrimaryCopy, ss.totalBytesCopy)) {
			recordCopyComplete(ss, ss.tempByteTail, ss.totalBytesCopy);			
		}					
		
		return; //finished all the copy  for now
	}

	private static void recordCopyComplete(SplitterStage ss, int tempByteTail, int totalBytesCopy) {
		//release tail so data can be written
		
		int i = RingBuffer.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
		RingBuffer.setBytesWorkingTail(ss.source, i);
        RingBuffer.setBytesTail(ss.source, i);   
		RingBuffer.publishWorkingTailPosition(ss.source,(ss.cachedTail+=ss.totalPrimaryCopy));
		ss.totalPrimaryCopy = 0; //clear so next time we find the next block
	}



	private static void findStableCutPoint(SplitterStage ss) {
		ss.byteHeadPos = RingBuffer.bytesHeadPosition(ss.source);
        ss.headPos = RingBuffer.headPosition(ss.source);		
		while(ss.byteHeadPos != RingBuffer.bytesHeadPosition(ss.source) || ss.headPos != RingBuffer.headPosition(ss.source) ) {
			ss.byteHeadPos = RingBuffer.bytesHeadPosition(ss.source);
			ss.headPos = RingBuffer.headPosition(ss.source);
		}
	}

	
	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static boolean doneCopy(SplitterStage ss, 
			                   int byteTailPos, int primaryTailPos, 
			                   int totalPrimaryCopy, 
			                   int totalBytesCopy) {

		int j = 0;
		int c = 0;
		int[] working = ss.working;
		int limit = ss.workingPos;
		while (j<limit) {
			
			if (!RingBuffer.roomToLowLevelWrite(ss.targets[working[j]], totalPrimaryCopy)) {
			 	working[c++] = working[j];
			} else {
				RingBuffer ringBuffer = ss.targets[working[j]];					
				copyData(ss, byteTailPos, totalBytesCopy, primaryTailPos, totalPrimaryCopy, ringBuffer);				
				RingBuffer.confirmLowLevelWrite(ringBuffer, totalPrimaryCopy);	
			}
			j++;
		}
		ss.workingPos = c;
		return 0==c; //returns false when there are still targets to write

	}


	public String toString() {
		return getClass().getSimpleName()+ " source content "+RingBuffer.contentRemaining(source);
	}

	private static void copyData(SplitterStage ss, int byteTailPos,
								int totalBytesCopy, int primaryTailPos, int totalPrimaryCopy,
								RingBuffer ringBuffer) {
		
		//copy the bytes
		RingBuffer.copyBytesFromToRing(RingBuffer.byteBuffer(ss.source),                   byteTailPos, ss.source.byteMask, 
		        RingBuffer.byteBuffer(ringBuffer), RingBuffer.bytesHeadPosition(ringBuffer), ringBuffer.byteMask, 
									  totalBytesCopy);
		
		RingBuffer.setBytesWorkingHead(ringBuffer, RingBuffer.addAndGetBytesHead(ringBuffer, totalBytesCopy));
								
		//copy the primary data
		int headPosition = (int)RingBuffer.headPosition(ringBuffer);
		RingBuffer.copyIntsFromToRing(RingBuffer.primaryBuffer(ss.source), primaryTailPos, ss.source.mask, 
		        RingBuffer.primaryBuffer(ringBuffer), headPosition, ringBuffer.mask, 
									 totalPrimaryCopy);
		
		RingBuffer.publishWorkingHeadPosition(ringBuffer, headPosition + totalPrimaryCopy);

	}
	
	
}
