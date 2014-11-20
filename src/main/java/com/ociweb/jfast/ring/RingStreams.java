package com.ociweb.jfast.ring;

import static com.ociweb.jfast.ring.RingBuffer.byteBackingArray;
import static com.ociweb.jfast.ring.RingBuffer.bytePosition;
import static com.ociweb.jfast.ring.RingBuffer.headPosition;
import static com.ociweb.jfast.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.jfast.ring.RingBuffer.tailPosition;
import static com.ociweb.jfast.ring.RingBuffer.releaseReadLock;
import static com.ociweb.jfast.ring.RingBuffer.spinBlockOnHead;
import static com.ociweb.jfast.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.jfast.ring.RingBuffer.takeRingByteMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
		if (inputRing.consumerData.from != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		long target = step+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
        long headPosCache = headPosition(inputRing);

        //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
        //      That change may however increase latency.
        
        while (true) {
        	        	
        	//block until one more byteVector is ready.
        	
        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		           
        	
        	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
        	int len = takeRingByteLen(inputRing);
        				
        	if (len<0) { //exit logic
        		releaseReadLock(inputRing);
          		return;
        	} else {                    	
        		int byteMask = inputRing.byteMask;
				byte[] data = byteBackingArray(meta, inputRing);
				int offset = bytePosition(meta,inputRing,len);        					
				
				if ((offset&byteMask) > ((offset+len) & byteMask)) {
					//rolled over the end of the buffer
					 int len1 = 1+byteMask-(offset&byteMask);
					 outputStream.write(data, offset&byteMask, len1);
					 outputStream.write(data, 0, len-len1);
				} else {						
					 //simple add bytes
					 outputStream.write(data, offset&byteMask, len); 
				}
        		releaseReadLock(inputRing);
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
		assert (outputRing.consumerData.from == FieldReferenceOffsetManager.RAW_BYTES);
		int fill =  1 + outputRing.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		int maxBlockSize = outputRing.byteMask / (outputRing.mask>>1);
		
		long tailPosCache = spinBlockOnTail(tailPosition(outputRing), headPosition(outputRing)-fill, outputRing);    
		
		byte[] buffer = outputRing.byteBuffer;
		int byteMask = outputRing.byteMask;
		
		int position = outputRing.byteWorkingHeadPos.value;
		int size;		
		while ( (size=inputStream.read(buffer,position&byteMask,((position&byteMask) > ((position+maxBlockSize) & byteMask)) ? 1+byteMask-(position&byteMask) : maxBlockSize))>=0 ) {	
			
			tailPosCache = spinBlockOnTail(tailPosCache, headPosition(outputRing)-fill, outputRing);
			
			RingWriter.finishWriteBytes(outputRing, position, size);
			RingBuffer.publishWrites(outputRing);
			position += size;
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
		assert (output.consumerData.from == FieldReferenceOffsetManager.RAW_BYTES);
		
	 	int fill = 1 + output.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		   
		long tailPosCache = tailPosition(output);    
		 
		int position = dataOffset; //position within the data array
		int stop = dataOffset+dataLength;
		while (position<stop) {
			 
			    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(output)-fill, output);

			    int fragmentLength = (int)Math.min(blockSize, stop-position);
		 
		    	RingBuffer.addByteArray(data, position, fragmentLength, output);
		    	RingBuffer.publishWrites(output);
		        
		    	position+=fragmentLength;
			 
		}
	}

	private static final byte[] EMPTY = new byte[0];

	public static void writeEOF(RingBuffer ring) {
		int fill = 1 + ring.mask - FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		spinBlockOnTail(tailPosition(ring), headPosition(ring)-fill, ring);
		RingBuffer.addByteArray(EMPTY, 0 , -1, ring);
		RingBuffer.publishWrites(ring);		
	}

	
	
}
