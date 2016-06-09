package com.ociweb.pronghorn.stage.route;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Given n ring buffers with the same FROM/Schema
 * 
 * Does not require schema knowledge for copy but does ensure targets and source have the same FROM.
 * @author Nathan Tippy
 *
 */
public class SplitterStage<T extends MessageSchema> extends PronghornStage {

	private Pipe<T> source;
	private Pipe<T>[] targets;
	
	private int byteHeadPos;
    private long headPos;
    private long cachedTail;
	private long totalPrimaryCopy;
	private int[] working;
	private int   workingPos;
    	
	int tempByteTail; 
	int byteTailPos;
	int totalBytesCopy;
	
	public SplitterStage(GraphManager gm, Pipe<T> source, Pipe<T> ... targets) {
		super(gm,source,targets);
		
		this.source = source;
		this.targets = targets;
		
		this.cachedTail = Pipe.tailPosition(source);
	
		this.supportsBatchedPublish = false;
		this.supportsBatchedRelease = false;		
		
		FieldReferenceOffsetManager sourceFrom = Pipe.from(source);
		
		int i = targets.length;
		working = new int[i];
		while(--i>=0) {
			
					
			//confirm this target is large enough for the needed data.
			FieldReferenceOffsetManager targetFrom = Pipe.from(targets[i]);
			
			if (targetFrom != sourceFrom) {
				throw new UnsupportedOperationException("Both source and target schemas must be the same");
			}
			
			//NOTE: longest message that holds a sequence needs to fit within a ring if the use case is to set the sequence length last.
			//      therefore if that target is full and needs one more fragment we may have a problem if the batch it has grabbed is 
			//      nearly has large as the target ring.  To resolve this we only need to ensure that the target ring is 2x the source.
			
			int reqTargetSize = source.bitsOfSlabRing+1; //target ring must be 2x bigger than source
			if (targets[i].bitsOfSlabRing < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" primary bit size must be at least "+reqTargetSize+" but it was "+targets[i].bitsOfSlabRing+
						           ". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			reqTargetSize = source.bitsOfBlogRing+1;
			if (targets[i].bitsOfBlogRing < reqTargetSize) {
				throw new UnsupportedOperationException("The target ring "+i+" byte bit size must be at least "+reqTargetSize+" but it was "+targets[i].bitsOfBlogRing+
									". To avoid blocking hang behavior the target rings must always be 2x larger than the source ring.");
			}
			
			int minDif = source.bitsOfBlogRing     -    source.bitsOfSlabRing;
			int targDif = targets[i].bitsOfBlogRing - targets[i].bitsOfSlabRing;
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
	
	private static <S extends MessageSchema> void processAvailData(SplitterStage<S> ss) {

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
            ss.tempByteTail = Pipe.getBlobRingTailPosition(ss.source);
            ss.totalBytesCopy =   ss.byteHeadPos -ss.tempByteTail;
            ss.byteTailPos = ss.source.byteMask & ss.tempByteTail;
		
		}

		//if all the copies are done then record it as complete, does as much work as possible each time its called.
		if (doneCopy(ss, ss.byteTailPos, ss.source.mask & (int)ss.cachedTail, (int)ss.totalPrimaryCopy, ss.totalBytesCopy)) {
			recordCopyComplete(ss, ss.tempByteTail, ss.totalBytesCopy);			
		}					
		
		return; //finished all the copy  for now
	}

	private static <S extends MessageSchema> void recordCopyComplete(SplitterStage<S> ss, int tempByteTail, int totalBytesCopy) {
		//release tail so data can be written
		
		int i = Pipe.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
		Pipe.setBytesWorkingTail(ss.source, i);
        Pipe.setBytesTail(ss.source, i);   
		Pipe.publishWorkingTailPosition(ss.source,(ss.cachedTail+=ss.totalPrimaryCopy));
		ss.totalPrimaryCopy = 0; //clear so next time we find the next block
	}



	private static <S extends MessageSchema> void findStableCutPoint(SplitterStage<S> ss) {
		ss.byteHeadPos = Pipe.getBlobRingHeadPosition(ss.source);
        ss.headPos = Pipe.headPosition(ss.source);		
		while(ss.byteHeadPos != Pipe.getBlobRingHeadPosition(ss.source) || ss.headPos != Pipe.headPosition(ss.source) ) {
			ss.byteHeadPos = Pipe.getBlobRingHeadPosition(ss.source);
			ss.headPos = Pipe.headPosition(ss.source);
		}
	}

	
	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static <S extends MessageSchema> boolean doneCopy(SplitterStage<S> ss, 
			                   int byteTailPos, int primaryTailPos, 
			                   int totalPrimaryCopy, 
			                   int totalBytesCopy) {

		int j = 0;
		int c = 0;
		int[] working = ss.working;
		int limit = ss.workingPos;
		while (j<limit) {
			
			if (!Pipe.hasRoomForWrite(ss.targets[working[j]], totalPrimaryCopy)) {
			 	working[c++] = working[j];
			} else {
			    Pipe.confirmLowLevelWrite(ss.targets[working[j]], totalPrimaryCopy);	
				copyData(ss, byteTailPos, totalBytesCopy, primaryTailPos, totalPrimaryCopy, ss.targets[working[j]]);				
			}
			j++;
		}
		ss.workingPos = c;
		return 0==c; //returns false when there are still targets to write

	}


	public String toString() {
		return getClass().getSimpleName()+ " source content "+Pipe.contentRemaining(source);
	}

	private static <S extends MessageSchema> void copyData(SplitterStage<S> ss, int byteTailPos,
								int totalBytesCopy, int primaryTailPos, int totalPrimaryCopy,
								Pipe<S> ringBuffer) {
		
		//copy the bytes
		Pipe.copyBytesFromToRing(Pipe.blob(ss.source),                   byteTailPos, ss.source.byteMask, 
		        Pipe.blob(ringBuffer), Pipe.getBlobRingHeadPosition(ringBuffer), ringBuffer.byteMask, 
									  totalBytesCopy);
		
		Pipe.setBytesWorkingHead(ringBuffer, Pipe.addAndGetBytesHead(ringBuffer, totalBytesCopy));
								
		//copy the primary data
		int headPosition = (int)Pipe.headPosition(ringBuffer);
		Pipe.copyIntsFromToRing(Pipe.slab(ss.source), primaryTailPos, ss.source.mask, 
		        Pipe.slab(ringBuffer), headPosition, ringBuffer.mask, 
									 totalPrimaryCopy);
		
		Pipe.publishWorkingHeadPosition(ringBuffer, headPosition + totalPrimaryCopy);

	}
	
	
}
