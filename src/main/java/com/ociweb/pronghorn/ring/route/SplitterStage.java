package com.ociweb.pronghorn.ring.route;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

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
	
	public SplitterStage(RingBuffer source, RingBuffer ... targets) {
		
		this.source = source;
		this.targets = targets;
		
		
		FieldReferenceOffsetManager sourceFrom = RingBuffer.from(source);
		
		int i = targets.length;
		while(--i>=0) {
			//confirm this target is large enought for the needed data.
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
		int totalPrimaryCopy = (int)(headPos-tempTail);
		if (totalPrimaryCopy<=0) {
			return true;
		}
		
		
		int bMask = ss.source.byteMask;		
		int tempByteTail = ss.source.bytesTailPos.get();
		int byteTailPos = bMask & tempByteTail;
		int totalBytesCopy =  (int)(byteHeadPos - tempByteTail);
		
		//now do the copies
		while(doingCopy(ss, byteTailPos, primaryTailPos, headPos, totalPrimaryCopy, totalBytesCopy)) {
			Thread.yield();
		}
		
		//now move pointer forward
		ss.source.byteWorkingTailPos.value = byteHeadPos;
		ss.source.bytesTailPos.set(byteHeadPos);
		ss.source.workingTailPos.value = headPos;
		ss.source.tailPos.set(headPos);
		
		return true;
	}

	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static boolean doingCopy(SplitterStage ss, 
			                   int byteTailPos, int primaryTailPos, 
			                   long absHeadPos, int totalPrimaryCopy, 
			                   int totalBytesCopy) {
		
						
		boolean moreToCopy = false;
		int i = ss.targets.length;
		while (--i>=0) {			
			long targetHeadPos = ss.targets[i].headPos.get();
			if (targetHeadPos<absHeadPos) {
				//need to copy this one
				long targetTailPos = ss.targets[i].tailPos.get();
				if (totalPrimaryCopy <   ss.targets[i].maxSize - (targetHeadPos-targetTailPos)) {
					//we have room do the copy, confirmed by the primary ring				
					
					//copy the bytes
					RingBuffer.copyBytesFromToRing(ss.source.byteBuffer, byteTailPos, ss.source.byteMask, 
							                       ss.targets[i].byteBuffer, ss.targets[i].bytesHeadPos.get(), ss.targets[i].byteMask, 
							                       totalBytesCopy);
					ss.targets[i].byteWorkingHeadPos.value = ss.targets[i].bytesHeadPos.addAndGet(totalBytesCopy);
					 
					//copy the primary data
					RingBuffer.copyIntsFromToRing(ss.source.buffer, primaryTailPos, ss.source.mask, 
		                       					  ss.targets[i].buffer, (int)ss.targets[i].headPos.get(), ss.targets[i].mask, 
		                       					  totalPrimaryCopy);
					ss.targets[i].workingHeadPos.value = ss.targets[i].headPos.addAndGet(totalPrimaryCopy);	
					
					
					
				} else {
					//no room try again later
					moreToCopy = true;
				}
			}
			//else this one was already done.
		}
		
		return moreToCopy;
	}
	
	
}
