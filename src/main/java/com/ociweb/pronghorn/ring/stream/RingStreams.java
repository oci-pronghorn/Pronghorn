package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseMessageReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class RingStreams {
	
	/**
	 * Copies all bytes from the inputRing to the outputStream.  Will continue to do this until the inputRing
	 * provides a byteArray reference with negative length.
	 * 
	 * Upon exit the RingBuffer and OutputStream are NOT closed so this method can be called again if needed.
	 * 
	 * For example the same connection can be left open for sending multiple files in sequence.
	 * 
	 * 
	 * @param inputRing
	 * @param outputStream
	 * @throws IOException
	 */
	public static void writeToOutputStream(RingBuffer inputRing, OutputStream outputStream) throws IOException {
				
		long step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (RingBuffer.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//target is always 1 ahead of where we are then we step by step size, this lets us pick up the 
		//EOF message which is only length 1
		long target = 1+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
        long headPosCache = headPosition(inputRing);

        //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
        //      That change may however increase latency.
        
        int byteMask = inputRing.byteMask;
        int byteSize = byteMask+1;
        
        while (true) {
        	        	
        	//block until one more byteVector is ready.
        	
        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		           
        	
        	int msgId = RingBuffer.takeValue(inputRing);

        				
        	if (msgId<0) { //exit logic
            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer. TODO: AAAA need to remvoe.
            	int len = takeRingByteLen(inputRing);
            	
        		releaseMessageReadLock(inputRing);
          		return;
        	} else {          
            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
            	int len = takeRingByteLen(inputRing);
				byte[] data = byteBackingArray(meta, inputRing);
				
//            	if (FieldReferenceOffsetManager.USE_VAR_COUNT) {
//            		//using the low level seems like this is required
//            		if (len>=0) {
//            			inputRing.byteWorkingTailPos.value+=len;
//            		}
//            	}
				int off = bytePosition(meta,inputRing,len)&byteMask;        					
				
				int len1 = byteSize-off;
				if (len1>=len) {
					//simple add bytes
					outputStream.write(data, off, len); 
				} else {						
					//rolled over the end of the buffer
					outputStream.write(data, off, len1);
					outputStream.write(data, 0, len-len1);
				}
        		releaseMessageReadLock(inputRing);
        	}
        	
        	target += step;
            
        }   
		
	}
		
	/**
	 * Copies all bytes from the inputRing to each of the outputStreams.  Will continue to do this until the inputRing
	 * provides a byteArray reference with negative length.
	 * 
	 * Upon exit the RingBuffer and OutputStream are NOT closed so this method can be called again if needed.
	 * 
	 * For example the same connection can be left open for sending multiple files in sequence.
	 * 
	 * 
	 * @param inputRing
	 * @param outputStreams the streams we want to write the data to.
	 * @throws IOException
	 */
	public static void writeToOutputStreams(RingBuffer inputRing, OutputStream... outputStreams) throws IOException {
				
		new Exception("LOOK MA, this code is used").printStackTrace();
		
		long step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (RingBuffer.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//only need to look for 1 value then step forward by steps this lets us pick up the EOM message without hanging.
		long target = 1+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
        long headPosCache = headPosition(inputRing);

        //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
        //      That change may however increase latency.
        
        while (true) {
        	        	
        	//block until one more byteVector is ready.
        	
        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);
        	int msgId = RingBuffer.takeValue(inputRing);
        				
        	if (msgId<0) { //exit logic

            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer. //TODO: AAA, remove.
            	int len = takeRingByteLen(inputRing);
            	
            	releaseMessageReadLock(inputRing);
          		return;
        	} else {                    	
            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
            	int len = takeRingByteLen(inputRing);
            	
        		int byteMask = inputRing.byteMask;
				byte[] data = byteBackingArray(meta, inputRing);
				
//            	if (FieldReferenceOffsetManager.USE_VAR_COUNT) {
//            		//using the low level seems like this is required
//            		if (len>=0) {
//            			inputRing.byteWorkingTailPos.value+=len;
//            		}
//            	}
				int offset = bytePosition(meta,inputRing,len);        					
	
				int adjustedOffset = offset & byteMask;
				int adjustedEnd = (offset + len) & byteMask;
				int adjustedLength = 1 + byteMask - adjustedOffset;

				for(OutputStream os : outputStreams) {
					if ( adjustedOffset > adjustedEnd) {
						//rolled over the end of the buffer
					 	os.write(data, adjustedOffset, adjustedLength);
						os.write(data, 0, len - adjustedLength);
					} else {						
					 	//simple add bytes
						 os.write(data, adjustedOffset, len); 
					}
				}
				
        		releaseMessageReadLock(inputRing);
        	}
        	
        	target += step;
            
        }   
		
	}
	
	/**
	 * Copies all bytes from the inputStream to the outputRing.
	 * 
	 * Blocks as needed for the outputRing.
	 * Writes until the inputStream reaches EOF, this is signaled by a negative length from the call to read.
	 * 
	 * @param inputStream
	 * @param outputRing
	 * @throws IOException
	 */
	public static void readFromInputStream(InputStream inputStream, RingBuffer outputRing) throws IOException {
		assert (RingBuffer.from(outputRing) == FieldReferenceOffsetManager.RAW_BYTES);
		int fill =  1 + outputRing.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		int maxBlockSize = outputRing.maxAvgVarLen;
		
		long tailPosCache = spinBlockOnTail(tailPosition(outputRing), headPosition(outputRing)-fill, outputRing);    
		
		byte[] buffer = outputRing.byteBuffer;
		int byteMask = outputRing.byteMask;
		
		int position = outputRing.byteWorkingHeadPos.value;

		int size = 0;	
		try{
			while ( (size=inputStream.read(buffer,position&byteMask,((position&byteMask) > ((position+maxBlockSize-1) & byteMask)) ? 1+byteMask-(position&byteMask) : maxBlockSize))>=0 ) {	
				if (size>0) {
					//block until there is a slot to write into
					tailPosCache = spinBlockOnTail(tailPosCache, headPosition(outputRing)-fill, outputRing);
					RingBuffer.addMsgIdx(outputRing, 0);
					RingBuffer.validateVarLength(outputRing, size);
					RingBuffer.addBytePosAndLen(outputRing.buffer, outputRing.mask, outputRing.workingHeadPos, outputRing.bytesHeadPos.get(), position, size);
					outputRing.byteWorkingHeadPos.value = position + size;
					RingBuffer.publishWrite(outputRing);
					position += size;
				} else {
					Thread.yield();
				}
			}
		} catch (IOException ioex) {
			System.err.println("FAILURE detected at position: "+position+" last known sizes: "+size+" byteMask: "+outputRing.byteMask+
					" rolloever "+((position&byteMask) >= ((position+maxBlockSize-1) & byteMask))+"  "+(position&byteMask)+" > "+((position+maxBlockSize-1) & byteMask));
			throw ioex;
		}
	}
	
	
	/**
	 * copied data array into ring buffer.  It blocks if needed and will split the array on ring buffer if needed.
	 * 
	 * @param data
	 * @param output
	 * @param blockSize
	 */
	public static void writeBytesToRing(byte[] data, int dataOffset, int dataLength,  RingBuffer output, int blockSize) {
		assert (RingBuffer.from(output) == FieldReferenceOffsetManager.RAW_BYTES);
		
	 	int fill = 1 + output.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		   
		long tailPosCache = tailPosition(output);    
		 
		int position = dataOffset; //position within the data array
		int stop = dataOffset+dataLength;
		while (position<stop) {
			 
			    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(output)-fill, output);

			    int fragmentLength = (int)Math.min(blockSize, stop-position);
		 
			    RingBuffer.addMsgIdx(output, 0);
			    
		    	RingBuffer.addByteArray(data, position, fragmentLength, output);
		    	RingBuffer.publishWrite(output);
		        
		    	position+=fragmentLength;
			 
		}
	}

	public static void writeEOF(RingBuffer ring) {
		//RingWalker.blockingFlush(ring);
		
		int fill = 1 + ring.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		spinBlockOnTail(tailPosition(ring), headPosition(ring)-fill, ring);
		RingBuffer.addMsgIdx(ring, -1); //end of file, message
		RingBuffer.addNullByteArray(ring); //TOOD: must remove
		RingBuffer.publishWrite(ring);		
	}

	public static void visitBytes(RingBuffer inputRing, ByteVisitor visitor) {
		
		long step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (RingBuffer.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//only start by adding 1 so we can get EOM message without hang.
		long target = 1+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
	    long headPosCache = headPosition(inputRing);
	
	    //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
	    //      That change may however increase latency.
	    
	    while (true) {
	    	        	
	    	//block until one more byteVector is ready.
	    	
	    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		           
	    	
	    	int msg = RingBuffer.takeValue(inputRing);

	    	int byteMask = inputRing.byteMask;
	    				
	    	if (msg<0) { //exit logic
		    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.  TODO: AAA, remove
		    	int len = takeRingByteLen(inputRing);
		    	
	    		releaseMessageReadLock(inputRing);
	    		visitor.close();
	      		return;
	    	} else {                    	
		    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
		    	int len = takeRingByteLen(inputRing);
		    	
	    		byte[] data = byteBackingArray(meta, inputRing);
				
//            	if (FieldReferenceOffsetManager.USE_VAR_COUNT) {
//            		//using the low level seems like this is required
//            		if (len>=0) {
//            			inputRing.byteWorkingTailPos.value+=len;
//            		}
//            	}
				int offset = bytePosition(meta,inputRing,len);        					
				
				if ((offset&byteMask) > ((offset+len-1) & byteMask)) {
					//rolled over the end of the buffer
					 int len1 = 1+byteMask-(offset&byteMask);
					 visitor.visit(data, offset&byteMask, len1, 0, len-len1);
				} else {						
					 //simple add bytes
					 visitor.visit(data, offset&byteMask, len); 
				}
	    		releaseMessageReadLock(inputRing);
	    	}
	    	
	    	target += step;
	        
	    }   
		
	}

	
	
}
