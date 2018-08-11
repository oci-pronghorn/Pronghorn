package com.ociweb.pronghorn.stage.stream;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.ZeroCopyByteArrayOutputStream;

/**
 * Takes the RawDataSchema on the input pipe and writes it to an output stream.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ToOutputStreamStage extends PronghornStage {

	private final Pipe<RawDataSchema> inputRing;
	private final OutputStream outputStream;
	private final int step;
	private final boolean eol;

	/**
	 *
	 * @param gm
	 * @param inputRing _in_ The RawDataSchema pipe that will be written to the output stream.
	 * @param outputStream
	 * @param eol
	 */
	public ToOutputStreamStage(GraphManager gm, Pipe<RawDataSchema> inputRing, OutputStream outputStream, boolean eol) {
		super(gm,inputRing,NONE);
		this.inputRing = inputRing;
				
		this.outputStream = outputStream;
		this.step =  RawDataSchema.FROM.fragDataSize[0];
		this.eol = eol;
		
	}

	    @Override
		public void run() {
			try {				
					
				//NOTE: This can be made faster by looping and summing all the lengths to do one single copy to the output stream
				//      That change may however increase latency.
				
				while (Pipe.hasContentToRead(inputRing)) {
						
					int msgId = Pipe.takeMsgIdx(inputRing);
					if (msgId<0) {
					    Pipe.releaseReadLock(inputRing);
					    Pipe.confirmLowLevelRead(inputRing, Pipe.EOF_SIZE);
					    Pipe.releaseAllBatchedReads(inputRing);
					    requestShutdown();
					    return;
					}
  
			    	int meta = Pipe.takeByteArrayMetaData(inputRing);//side effect, this moves the pointer.
	    					    			
			    	int len = Pipe.takeByteArrayLength(inputRing);
			    	int off = bytePosition(meta,inputRing,len)&inputRing.blobMask; 			
			    	
			    	if (len>=0) { 
			    	    
						byte[] data = byteBackingArray(meta, inputRing);
						int len1 = inputRing.sizeOfBlobRing - off;
						if (len1 >= len) {
							//simple add bytes
							outputStream.write(data, off, len); 
						} else {		
							//rolled over the end of the buffer
							outputStream.write(data, off, len1);
							outputStream.write(data, 0, len-len1);
						}
						if (eol) {
							outputStream.write('\n');
						}
						outputStream.flush();
			    	} else if (len<0) {
			    	    Pipe.releaseReadLock(inputRing);
	                    Pipe.confirmLowLevelRead(inputRing, step);
	                    Pipe.releaseAllBatchedReads(inputRing);
			    	    requestShutdown();
			    	    return;
			    	}
			        Pipe.releaseReadLock(inputRing);
			    	Pipe.confirmLowLevelRead(inputRing, step);		    	
 
				}			
				Pipe.releaseAllBatchedReads(inputRing);
			} catch (IOException e) {
				throw new RuntimeException(e);			
			} 	
								
		}
		
		@Override
		public void shutdown() {
			try {
				outputStream.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	
}
