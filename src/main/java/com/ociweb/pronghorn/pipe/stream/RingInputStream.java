package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.headPosition;
import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnHead;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.io.InputStream;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class RingInputStream extends InputStream implements AutoCloseable {

	private final Pipe ring;
	private final int sourceByteMask;
	private final int recordSize = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
	
	private int remainingSourceLength = -1;
	private int remainingSourceMeta;
	private int remainingSourceOffset;
	private byte[] oneByte = new byte[1]; 
	
	/**
	 * By definition an input stream is blocking so this adds a blocking API for the ring buffer.
	 * @param ring
	 */
	public RingInputStream(Pipe ring) {
		this.ring = ring;
		this.sourceByteMask = ring.byteMask;
		if (Pipe.from(ring) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
	}
	
	@Override
    public int available() throws IOException {
        return 0;
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

//	boolean closed = false;// TODO: C, clean this up to make simpler
	private int blockForNewContent(byte[] targetData, int targetOffset, int targetLength) {
//	    if (closed) {
//	        return -1;
//	    }
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
		
		int msgId = Pipe.takeMsgIdx(ring);
		
		if (msgId>=0) { //exit EOF logic
			int meta = takeRingByteMetaData(ring);//side effect, this moves the pointer and must happen before we call for length
			int sourceLength = takeRingByteLen(ring);
			return beginNewContent(targetData, targetOffset, targetLength, meta, sourceLength);
		} else { 
		    Pipe.confirmLowLevelRead(ring, recordSize);
			Pipe.releaseReads(ring); //TOOD: bad idea needs more elegant solution.
		//	closed = true;
			return -1;			
		}
	}

	private int beginNewContent(byte[] targetData, int targetOffset, int targetLength, int meta, int sourceLength) {
		byte[] sourceData = byteBackingArray(meta, ring);
		int sourceOffset = bytePosition(meta,ring,sourceLength);        					
								
		if (sourceLength<=targetLength) {
			//the entire block can be sent
			copyData(targetData, targetOffset, sourceLength, sourceData, sourceOffset);
			Pipe.confirmLowLevelRead(ring, recordSize);
			Pipe.releaseReads(ring);
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
		Pipe.confirmLowLevelRead(ring, recordSize);
		Pipe.releaseReads(ring);
		remainingSourceLength = -1; //clear because we are now done with the remaining content
		return len;
	}

	private void copyData(byte[] targetData, int targetOffset, int sourceLength, byte[] sourceData, int sourceOffset) {
	    if (0==sourceLength) {
	        return; //TODO: needs to be cleaned up but we can not use the logic below when length is zero.
	    }
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
