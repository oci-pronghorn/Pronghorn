package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.io.IOException;
import java.io.InputStream;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class RingInputStream extends InputStream {

	private final RingBuffer ring;
	private final int sourceByteMask;
	private final int recordSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
	private int remainingSourceLength = -1;
	private int remainingSourceMeta;
	private int remainingSourceOffset;
	private byte[] oneByte = new byte[1]; 
	
	public RingInputStream(RingBuffer ring) {
		this.ring = ring;
		this.sourceByteMask = ring.byteMask;
		if (RingBuffer.from(ring) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
		
	@Override
	public int read() {
		//this array does not escape the scope of this method so it will
		//probably be removed by the runtime compiler and directly use stack space
		
		if (remainingSourceLength <= 0) {
			return blockForNewContent(oneByte, 0, 1) <  0 ? -1 : 0xFF&oneByte[0];
		} else {		
			if (sendRemainingContent(oneByte, 0, 1)!=1) {
				throw new UnsupportedOperationException();
			}
			return 0xFF&oneByte[0];
		}
	}

	@Override
	public int read(byte[] b) {
		//favor true as the most frequent branch and keep the happy path first
		//this helps branch prediction and pre-fetch
		int result;
		if (remainingSourceLength <= 0) {
			result = blockForNewContent(b, 0, b.length);
		} else {		
			result = sendRemainingContent(b, 0, b.length);
		}
		if (0==result) {
			new Exception("BAD ZERO RETURN").printStackTrace();
		}
		return result;
	}

	@Override
	public int read(byte[] targetData, int targetOffset, int targetLength) {
		//favor true as the most frequent branch and keep the happy path first
		//this helps branch prediction and pre-fetch
		int result;
		if (remainingSourceLength <= 0) {
			result = blockForNewContent(targetData, targetOffset, targetLength);
		} else {		
			result = sendRemainingContent(targetData, targetOffset, targetLength);
		}
		if (0==result) {
			new Exception("BAD ZERO RETURN2").printStackTrace();
		}
		return result;
	}

	private int blockForNewContent(byte[] targetData, int targetOffset, int targetLength) {
		int returnLength = 0;
		//only need to look for 1 value then step forward by steps this lets us pick up the EOM message without hanging.
		long target = 1+tailPosition(ring);
		long headPosCache = headPosition(ring);
				
		do {
			//block until we have something to read
		    headPosCache = spinBlockOnHead(headPosCache, target, ring);
		    target+=recordSize;
		    returnLength = sendNewContent(targetData, targetOffset, targetLength);		    
		    
		} while (returnLength==0); //Must block until at least 1 byte was read or -1 EOF detected
		return returnLength;
	}
	
	private int sendNewContent(byte[] targetData, int targetOffset,	int targetLength) {
		
		int msgId = RingBuffer.takeMsgIdx(ring);
		
		if (msgId>=0) { //exit EOF logic
			int meta = takeRingByteMetaData(ring);//side effect, this moves the pointer and must happen before we call for length
			int sourceLength = takeRingByteLen(ring);
			return beginNewContent(targetData, targetOffset, targetLength, meta, sourceLength);
		} else {   
			int bytesCount = RingBuffer.takeValue(ring);
			assert(0==bytesCount);						
			releaseReadLock(ring);
			return -1;			
		}
	}

	private int beginNewContent(byte[] targetData, int targetOffset, int targetLength, int meta, int sourceLength) {
		byte[] sourceData = byteBackingArray(meta, ring);
		int sourceOffset = bytePosition(meta,ring,sourceLength);        					
								
		if (sourceLength<=targetLength) {
			//the entire block can be sent
			copyData(targetData, targetOffset, sourceLength, sourceData, sourceOffset);
			releaseReadLock(ring);
			return sourceLength;
		} else {
			//only part of the block can be sent so save some for later
			copyData(targetData, targetOffset, targetLength, sourceData, sourceOffset);
			
			//do not release read lock we are not done yet
			remainingSourceLength = sourceLength-targetLength;
			remainingSourceMeta = meta;
			remainingSourceOffset = sourceOffset+targetLength;
			
			return targetLength;
		}
	}

	private int sendRemainingContent(byte[] targetData, int targetOffset, int targetLength) {
		//send the rest of the data that we could not last time 
		//we assume that ending remaining content happens more frequently than the continuation
		if (remainingSourceLength<=targetLength) {
			return endRemainingContent(targetData, targetOffset);
		} else {
			return continueRemainingContent(targetData, targetOffset, targetLength);				
		}
	}

	private int continueRemainingContent(byte[] targetData, int targetOffset, int targetLength) {
		//only part of the block can be sent so save some for later
		
		copyData(targetData, targetOffset, targetLength, byteBackingArray(remainingSourceMeta, ring), remainingSourceOffset);
		
		//do not release read lock we are not done yet
		remainingSourceLength = remainingSourceLength-targetLength;
		remainingSourceOffset = remainingSourceOffset+targetLength;
		
		return targetLength;
	}

	private int endRemainingContent(byte[] targetData, int targetOffset) {
		//the entire remaining part of the block can be sent
		int len = remainingSourceLength;
		copyData(targetData, targetOffset, len, byteBackingArray(remainingSourceMeta, ring), remainingSourceOffset);
		releaseReadLock(ring);
		remainingSourceLength = -1; //clear because we are now done with the remaining content
		return len;
	}

	private void copyData(byte[] targetData, int targetOffset, int sourceLength, byte[] sourceData, int sourceOffset) {

		if ((sourceOffset&sourceByteMask) > ((sourceOffset+sourceLength-1) & sourceByteMask)) {
			//rolled over the end of the buffer
			 int len1 = 1+sourceByteMask-(sourceOffset&sourceByteMask);
			 System.arraycopy(sourceData, sourceOffset&sourceByteMask, targetData, targetOffset, len1);
			 System.arraycopy(sourceData, 0,                           targetData, targetOffset+len1, sourceLength-len1);
		} else {						
			 //simple add bytes
			 System.arraycopy(sourceData, sourceOffset&sourceByteMask, targetData, targetOffset, sourceLength); 
		}
	}
	
}
