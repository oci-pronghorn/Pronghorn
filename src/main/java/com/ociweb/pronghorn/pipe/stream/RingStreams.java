package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.headPosition;
import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnHead;
import static com.ociweb.pronghorn.pipe.Pipe.spinBlockOnTail;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

@Deprecated //Consider using the newer DataInputBlobReader or DataOutputBlobWriter or the fieldWrite methods in PipeReader Pipe or PipeWriter
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
	public static void writeToOutputStream(Pipe inputRing, OutputStream outputStream) throws IOException {
				
		long step =  RawDataSchema.FROM.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (Pipe.from(inputRing) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//target is always 1 ahead of where we are then we step by step size, this lets us pick up the 
		//EOF message which is only length 2
		long target = 2+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
        long headPosCache = headPosition(inputRing);

        //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
        //      That change may however increase latency.
        
        int byteMask = inputRing.byteMask;
        int byteSize = byteMask+1;
        
        while (true) {
        	        	
        	//block until one more byteVector is ready.
        	
        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);	                        	    	                        		           
        	
        	int msgId = Pipe.takeMsgIdx(inputRing);

        				
        	if (msgId<0) { //exit logic
        		Pipe.releaseReads(inputRing);
          		break;
        	} else {          
            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
            	int len = takeRingByteLen(inputRing);
            	if (len>0) {            	
					byte[] data = byteBackingArray(meta, inputRing);
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
					outputStream.flush();
            	}
            	Pipe.releaseReads(inputRing);
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
	public static void writeToOutputStreams(Pipe inputRing, OutputStream... outputStreams) throws IOException {
						
		long step =  RawDataSchema.FROM.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (Pipe.from(inputRing) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//only need to look for 2 value then step forward by steps this lets us pick up the EOM message without hanging.
		long target = 2+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
        long headPosCache = headPosition(inputRing);

        //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
        //      That change may however increase latency.
        
        while (true) {
        	        	
        	//block until one more byteVector is ready.
        	
        	headPosCache = spinBlockOnHead(headPosCache, target, inputRing);
        	int msgId = Pipe.takeMsgIdx(inputRing);
        				
        	if (msgId<0) { //exit logic
        		int bytesCount = Pipe.takeValue(inputRing);
        		assert(0==bytesCount);
            	
        		Pipe.releaseReads(inputRing);
          		return;
        	} else {                    	
            	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
            	int len = takeRingByteLen(inputRing);
            	
        		int byteMask = inputRing.byteMask;
				byte[] data = byteBackingArray(meta, inputRing);
				
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
					os.flush();
				}
				
				Pipe.releaseReads(inputRing);
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
	@Deprecated
	public static void readFromInputStream(InputStream inputStream, Pipe outputRing) throws IOException {
		assert (Pipe.from(outputRing) == RawDataSchema.FROM);
		int step = RawDataSchema.FROM.fragDataSize[0];
		int fill =  1 + outputRing.mask - step;
		int maxBlockSize = outputRing.maxAvgVarLen;
		
		long targetTailValue = headPosition(outputRing)-fill;
		long tailPosCache = tailPosition(outputRing);
		
		byte[] buffer = Pipe.byteBuffer(outputRing);
		int byteMask = outputRing.byteMask;
		
		int position = Pipe.bytesWorkingHeadPosition(outputRing);

		int size = 0;	
		try{
		    new Exception("this does not support wrapping of blob data and any usages should be changed over to the new stream apis int PipeReader, Pipe and PipeWriter").printStackTrace();
			while ( (size=inputStream.read(buffer,position&byteMask,((position&byteMask) > ((position+maxBlockSize-1) & byteMask)) ? 1+byteMask-(position&byteMask) : maxBlockSize))>=0 ) {	
				if (size>0) {
					//block until there is a slot to write into
					tailPosCache = spinBlockOnTail(tailPosCache, targetTailValue, outputRing);///TODO:M Rewrite using RingBuffer.roomToLowLevelWrite(output, size)
					targetTailValue += step;
					
					Pipe.addMsgIdx(outputRing, 0);
					Pipe.validateVarLength(outputRing, size);
					Pipe.addBytePosAndLen(outputRing, position, size);
			        Pipe.addAndGetBytesWorkingHeadPosition(outputRing, size);
			        
			        Pipe.confirmLowLevelWrite(outputRing, RawDataSchema.FROM.fragDataSize[0]);
					Pipe.publishWrites(outputRing);
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
	public static void writeBytesToRing(byte[] data, int dataOffset, int dataLength,  Pipe output, int blockSize) {
		assert (Pipe.from(output) == RawDataSchema.FROM);
		
	 	int fill = 1 + output.mask - RawDataSchema.FROM.fragDataSize[0];
		   
		long tailPosCache = tailPosition(output);    
		 
		int position = dataOffset; //position within the data array
		int stop = dataOffset+dataLength;
		while (position<stop) {
			 
			    tailPosCache = spinBlockOnTail(tailPosCache, headPosition(output)-fill, output); ///TODO:M Rewrite using RingBuffer.roomToLowLevelWrite(output, size)

			    int fragmentLength = (int)Math.min(blockSize, stop-position);
		 
			    Pipe.addMsgIdx(output, 0);
			    
		    	Pipe.addByteArray(data, position, fragmentLength, output);
		    	Pipe.confirmLowLevelWrite(output, RawDataSchema.FROM.fragDataSize[0]);
		    	Pipe.publishWrites(output);
		        
		    	position+=fragmentLength;
			 
		}
	}

	@Deprecated
	public static void writeEOF(Pipe ring) {//TODO:M propose a way to remove the need for this poison pill and the blocking use of this call on close()
		spinBlockOnTail(tailPosition(ring), headPosition(ring)-(1 + ring.mask - Pipe.EOF_SIZE), ring);
		Pipe.publishEOF(ring);	
	}

	public static void visitBytes(Pipe inputRing, ByteVisitor visitor) {
		
		long step =  RawDataSchema.FROM.fragDataSize[0];
		
		 //this blind byte copy only works for this simple message type, it is not appropriate for other complex types
		if (Pipe.from(inputRing) != RawDataSchema.FROM) {
			throw new UnsupportedOperationException("This method can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		//only start by adding 2 so we can get EOM message without hang.
		long target = 2+tailPosition(inputRing);
				
		//write to outputStream only when we have data on inputRing.
	    long headPosCache = headPosition(inputRing);
	
	    //NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
	    //      That change may however increase latency.
	    
	    while (true) {
	    	        	
	    	//block until one more byteVector is ready.
	    	
	    	headPosCache = spinBlockOnHead(headPosCache, target, inputRing); //TODO:M,  make this non blocking- will require method signature change.	                        	    	                        		           
	    	
	    	int msg = Pipe.takeMsgIdx(inputRing);

	    	int byteMask = inputRing.byteMask;
	    				
	    	if (msg<0) { //exit logic
	    		int bytesCount = Pipe.takeValue(inputRing);
		    	assert(0==bytesCount);
		    	
		    	Pipe.confirmLowLevelRead(inputRing, RawDataSchema.FROM.fragDataSize[0]);
		    	Pipe.releaseReads(inputRing);
	    		visitor.close();
	      		return;
	    	} else {                    	
		    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
		    	int len = takeRingByteLen(inputRing);
		    	
	    		byte[] data = byteBackingArray(meta, inputRing);

				int offset = bytePosition(meta,inputRing,len);        					
				
				if ((offset&byteMask) > ((offset+len-1) & byteMask)) {
					//rolled over the end of the buffer
					 int len1 = 1+byteMask-(offset&byteMask);
					 visitor.visit(data, offset&byteMask, len1, 0, len-len1);
				} else {						
					 //simple add bytes
					 visitor.visit(data, offset&byteMask, len); 
				}
				Pipe.confirmLowLevelRead(inputRing, RawDataSchema.FROM.fragDataSize[0]);
				Pipe.releaseReads(inputRing);
	    	}
	    	
	    	target += step;
	        
	    }   
		
	}

	
	
}
