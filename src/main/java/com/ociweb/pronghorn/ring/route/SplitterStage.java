package com.ociweb.pronghorn.ring.route;

import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.util.PaddedAtomicInteger;

/**
 * Given n ring buffers with the same FROM/Schema
 * 
 * Does not require schema knowledge for copy but does ensure targets and source have the same FROM.
 * @author Nathan Tippy
 *
 */
public class SplitterStage implements Runnable {

	private RingBuffer source;
	private RingBuffer[] targets;
	private long[] targetHeadPos;
	
	public SplitterStage(RingBuffer source, RingBuffer ... targets) {
		
		this.source = source;
		this.targets = targets;
		
		FieldReferenceOffsetManager sourceFrom = RingBuffer.from(source);
		
		int i = targets.length;
		this.targetHeadPos = new long[i];
		
		while(--i>=0) {
			
			targetHeadPos[i] = targets[i].headPos.get(); 
					
			//confirm this target is large enough for the needed data.
			FieldReferenceOffsetManager targetFrom = RingBuffer.from(targets[i]);
			
			if (targetFrom != sourceFrom) {
				throw new UnsupportedOperationException("Both source and target schemas must be the same");
			}
			
			if (targets[i].pBits < source.pBits) {
				throw new UnsupportedOperationException("The target ring "+i+" primary bit size must be at least "+source.pBits+" but it was "+targets[i].pBits);
			}
			
			if (targets[i].bBits < source.bBits) {
				throw new UnsupportedOperationException("The target ring "+i+" byte bit size must be at least "+source.bBits+" but it was "+targets[i].bBits);
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
		
		assert(Thread.currentThread().isDaemon()) : "This stage can only be run with daemon threads";
		if (!Thread.currentThread().isDaemon()) {
			throw new UnsupportedOperationException("This stage can only be run with daemon threads");
		}
		
		try{
			while (processAvailData(this)) {
					Thread.yield();
			}
		} catch (Throwable t) {
			RingBuffer.shutDown(source);
			int i = targets.length;
			while(--i>=0) {
				RingBuffer.shutDown(targets[i]);
			}
			
		}
	}

	private static boolean processAvailData(SplitterStage ss) {
		
		int byteHeadPos;
        long headPos;
		
        //TODO AAA, publush to a single atomic long and read it here.
        //get the new head position
        byteHeadPos = ss.source.bytesHeadPos.get();
		headPos = ss.source.headPos.get();		
		while(byteHeadPos != ss.source.bytesHeadPos.get() || headPos != ss.source.headPos.get()  ) {
			byteHeadPos = ss.source.bytesHeadPos.get();
			headPos = ss.source.headPos.get();
		}		
				
		//get the start and stop locations for the copy
		
		int pMask = ss.source.mask;
		long tempTail = ss.source.tailPos.get();
		int primaryTailPos = pMask & (int)tempTail;				
		int totalPrimaryCopy = (int)(((int)headPos)-((int)tempTail));
		if (totalPrimaryCopy<=0) {
			return true;
		}
		long absHeadPos = ss.source.workingTailPos.value+totalPrimaryCopy;
	
		
		int bMask = ss.source.byteMask;		
		int tempByteTail = ss.source.bytesTailPos.get();
		int byteTailPos = bMask & tempByteTail;
		int totalBytesCopy =  (int)(byteHeadPos - tempByteTail);
				
		//now do the copies
		doingCopy(ss, byteTailPos, primaryTailPos, totalPrimaryCopy, totalBytesCopy);
		RingBuffer.releaseReadLock(ss.source);
				
		//now move pointer forward
		ss.source.byteWorkingTailPos.value = byteHeadPos;
		ss.source.bytesTailPos.set(byteHeadPos);
		ss.source.workingTailPos.value =absHeadPos;
		ss.source.tailPos.set(absHeadPos);
		
		return true;
	}

	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static void doingCopy(SplitterStage ss, 
			                   int byteTailPos, int primaryTailPos, 
			                   int totalPrimaryCopy, 
			                   int totalBytesCopy) {
		
						
		
		boolean moreToCopy;
		do {
			moreToCopy = false;
			int i = ss.targets.length;
			while (--i>=0) {			
								
				if ((totalPrimaryCopy + ss.targetHeadPos[i]) > ss.targets[i].workingHeadPos.value) {
					RingBuffer ringBuffer = ss.targets[i];
										
					//the tail must be larger than this position for there to be room to rite
					long tail =  totalPrimaryCopy + ringBuffer.workingHeadPos.value - ringBuffer.maxSize;
					
					if (ringBuffer.tailPos.get()>=tail ) {
						//copy the bytes
						PaddedAtomicInteger bytesHeadPos = ringBuffer.bytesHeadPos;
						RingBuffer.copyBytesFromToRing(ss.source.byteBuffer, byteTailPos, ss.source.byteMask, 
								ringBuffer.byteBuffer, bytesHeadPos.get(), ringBuffer.byteMask, 
								totalBytesCopy);
						ringBuffer.byteWorkingHeadPos.value = bytesHeadPos.addAndGet(totalBytesCopy);
						
						//copy the primary data
						AtomicLong headPos = ringBuffer.headPos;
						RingBuffer.copyIntsFromToRing(ss.source.buffer, primaryTailPos, ss.source.mask, 
								ringBuffer.buffer, (int)headPos.get(), ringBuffer.mask, 
								totalPrimaryCopy);
						ringBuffer.workingHeadPos.value = headPos.addAndGet(totalPrimaryCopy);	

						RingBuffer.publishWrites(ringBuffer);
					} else {
						moreToCopy = true;
					}
				} // else this is already done.
				
			}
		} while(moreToCopy);
		
		//reset for next time.
		int i = ss.targets.length;
		while (--i>=0) {
			//mark this one as done.
			ss.targetHeadPos[i] += totalPrimaryCopy;
		}
		
	}
	
	
}
